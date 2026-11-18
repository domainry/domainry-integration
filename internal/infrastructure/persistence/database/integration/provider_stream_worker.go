package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-orm/query"
)

const (
	providerStreamMaxSessions       = 64
	providerStreamLeaseDuration     = 30 * time.Second
	providerStreamRenewInterval     = 10 * time.Second
	providerStreamReconcileInterval = time.Second
)

var errProviderStreamLeaseLost = errors.New("Integration Provider stream lost its lease")

type providerStreamSession struct {
	workspaceID, connectionKey string
	cancel                     context.CancelFunc
	done                       <-chan struct{}
}

type providerStreamCandidate struct {
	id                                                               int64
	workspaceID, connectorKey, providerKey, connectionKey, streamKey string
	stateVersion, attemptCount                                       int
	payload, progress, status                                        string
	dueAt, leaseExpiresAt, fencingToken                              int64
}

// NotifyProviderStreamChange immediately cancels a locally owned generation.
// The database fence written by Management remains the cross-process source of
// truth; this signal only removes the local lease-renew interval delay.
func (s *WorkerStore) NotifyProviderStreamChange(workspaceID, connectionKey string) {
	if s == nil {
		return
	}
	s.streamMu.Lock()
	for _, session := range s.streamSessions {
		if session.workspaceID == workspaceID && session.connectionKey == connectionKey {
			session.cancel()
		}
	}
	s.streamMu.Unlock()
	s.signalProviderStreams()
}

func (s *WorkerStore) signalProviderStreams() {
	select {
	case s.streamWake <- struct{}{}:
	default:
	}
}

// RunProviderStreams supervises process-owned stream sessions until ctx is
// cancelled. A durable lease and fencing token guarantee one accepted owner
// across Runtime replicas; the local registry guarantees bounded goroutines
// and deterministic shutdown within this process.
func (s *WorkerStore) RunProviderStreams(ctx context.Context) error {
	if s == nil || s.database == nil || s.delivery == nil {
		return fmt.Errorf("Integration Provider stream worker is unavailable")
	}
	ticker := time.NewTicker(providerStreamReconcileInterval)
	defer ticker.Stop()
	for {
		if err := s.reconcileProviderStreams(ctx); err != nil && ctx.Err() == nil {
			// Database/provider availability is expected to recover. Keep the
			// supervisor alive; claimed sessions retain their own lease checks.
		}
		select {
		case <-ctx.Done():
			return s.stopProviderStreams()
		case <-ticker.C:
		case <-s.streamWake:
		}
	}
}

func (s *WorkerStore) stopProviderStreams() error {
	s.streamMu.Lock()
	sessions := make([]providerStreamSession, 0, len(s.streamSessions))
	for _, session := range s.streamSessions {
		sessions = append(sessions, session)
		session.cancel()
	}
	s.streamMu.Unlock()
	for _, session := range sessions {
		<-session.done
	}
	return nil
}

func (s *WorkerStore) reconcileProviderStreams(ctx context.Context) error {
	s.streamMu.Lock()
	available := providerStreamMaxSessions - len(s.streamSessions)
	s.streamMu.Unlock()
	if available <= 0 {
		return nil
	}
	candidates, err := s.listProviderStreamCandidates(ctx, s.now(), available)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		s.streamMu.Lock()
		_, active := s.streamSessions[candidate.id]
		s.streamMu.Unlock()
		if active {
			continue
		}
		claimed, err := s.claimProviderStream(ctx, &candidate, s.now())
		if err != nil {
			return err
		}
		if claimed {
			s.startProviderStream(ctx, candidate)
		}
	}
	return nil
}

func (s *WorkerStore) listProviderStreamCandidates(ctx context.Context, now time.Time, limit int) ([]providerStreamCandidate, error) {
	nowMillis := now.UTC().UnixMilli()
	predicate := query.And(query.Equal("run_kind", providerRunKindStream), query.Or(
		query.And(query.In("status", "ready", "failed"), query.LessThanOrEqual("due_at", nowMillis), query.Or(query.Equal("lease_expires_at", int64(0)), query.LessThanOrEqual("lease_expires_at", nowMillis))),
		query.And(query.Equal("status", "running"), query.LessThanOrEqual("lease_expires_at", nowMillis)),
	))
	statement, args, err := query.NewSelectBuilder(s.dialect, providerRunTable).
		Columns("id", "workspace_id", "connector_key", "provider_key", "connection_key", "task_key", "state_version", "payload_json", "progress_json", "status", "attempt_count", "fencing_token", "due_at", "lease_expires_at").
		Where(predicate).OrderBy(query.Ascending("due_at"), query.Ascending("workspace_id"), query.Ascending("connection_key"), query.Ascending("task_key")).Limit(limit).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]providerStreamCandidate, 0, limit)
	for rows.Next() {
		var value providerStreamCandidate
		var progress sql.NullString
		if err := rows.Scan(&value.id, &value.workspaceID, &value.connectorKey, &value.providerKey, &value.connectionKey, &value.streamKey, &value.stateVersion, &value.payload, &progress, &value.status, &value.attemptCount, &value.fencingToken, &value.dueAt, &value.leaseExpiresAt); err != nil {
			return nil, err
		}
		if progress.Valid {
			value.progress = progress.String
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *WorkerStore) claimProviderStream(ctx context.Context, candidate *providerStreamCandidate, now time.Time) (bool, error) {
	owner := s.workerID + ":stream"
	statement, args, err := query.NewUpdateBuilder(s.dialect, providerRunTable).
		Set("status", "running").Set("lease_owner", owner).Set("lease_expires_at", now.Add(providerStreamLeaseDuration).UnixMilli()).Set("fencing_token", candidate.fencingToken+1).Set("updated_at", now.UnixMilli()).
		Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, candidate.workspaceID, providerRunTable, strconv.FormatInt(candidate.id, 10)), query.Equal("run_kind", providerRunKindStream), query.Equal("id", candidate.id), query.Equal("status", candidate.status), query.Equal("fencing_token", candidate.fencingToken), query.Equal("due_at", candidate.dueAt), query.Equal("lease_expires_at", candidate.leaseExpiresAt))).Build()
	if err != nil {
		return false, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return false, nil
	}
	candidate.fencingToken++
	candidate.status = "running"
	candidate.leaseExpiresAt = now.Add(providerStreamLeaseDuration).UnixMilli()
	return true, nil
}

func (s *WorkerStore) startProviderStream(parent context.Context, candidate providerStreamCandidate) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	s.streamMu.Lock()
	if _, exists := s.streamSessions[candidate.id]; exists {
		s.streamMu.Unlock()
		cancel()
		close(done)
		return
	}
	s.streamSessions[candidate.id] = providerStreamSession{workspaceID: candidate.workspaceID, connectionKey: candidate.connectionKey, cancel: cancel, done: done}
	s.streamMu.Unlock()
	go func() {
		defer close(done)
		defer func() {
			s.streamMu.Lock()
			delete(s.streamSessions, candidate.id)
			s.streamMu.Unlock()
			s.signalProviderStreams()
		}()
		s.executeProviderStream(ctx, candidate)
	}()
}

func (s *WorkerStore) executeProviderStream(ctx context.Context, candidate providerStreamCandidate) {
	connection, err := s.delivery.connection(ctx, deliveryRequest(candidate.workspaceID, candidate.connectorKey, candidate.connectionKey))
	if err != nil || connection.Status != "active" {
		if err == nil {
			err = fmt.Errorf("Integration Provider stream connection is inactive")
		}
		s.finishProviderStream(candidate, err, ctx.Err() != nil)
		return
	}
	provider, ok := s.delivery.providers.Provider(connection.ConnectorKey, connection.ProviderKey)
	if !ok {
		s.finishProviderStream(candidate, connector.PermanentError("integration.provider_unavailable", fmt.Errorf("provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)), false)
		return
	}
	processor, ok := providerStreamProcessor(provider)
	if !ok {
		s.finishProviderStream(candidate, connector.PermanentError("integration.provider_stream_unavailable", fmt.Errorf("provider %s/%s has no stream processor", connection.ConnectorKey, connection.ProviderKey)), false)
		return
	}
	resolvedSecrets, err := s.delivery.resolveProviderSecrets(ctx, candidate.workspaceID, connection.SecretRefs)
	if err != nil {
		s.finishProviderStream(candidate, err, ctx.Err() != nil)
		return
	}
	sink := &providerStreamSink{store: s, candidate: candidate, progress: candidate.progress}
	runContext, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() {
		runDone <- processor.RunStream(runContext, connector.StreamRequest{
			StreamKey: candidate.streamKey, StateVersion: candidate.stateVersion,
			Connection: connector.Connection{Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs},
			State:      []byte(candidate.payload), Secrets: resolvedSecrets.Values, Now: s.now(),
			Principal: connector.Principal{WorkspaceID: candidate.workspaceID, RequestID: strconv.FormatInt(candidate.id, 10), IsAuthenticated: true},
		}, sink)
	}()
	renew := time.NewTicker(providerStreamRenewInterval)
	defer renew.Stop()
	for {
		select {
		case runErr := <-runDone:
			if runErr == nil {
				runErr = connector.RetryableError("integration.provider_stream_ended", fmt.Errorf("provider stream returned without cancellation"))
			}
			s.finishProviderStream(candidate, runErr, ctx.Err() != nil)
			return
		case <-renew.C:
			retained, renewErr := s.renewProviderStreamLease(ctx, candidate, s.now())
			if renewErr != nil || !retained {
				if renewErr == nil {
					renewErr = errProviderStreamLeaseLost
				}
				// Cancellation interrupts a blocking WebSocket receive. Wait for
				// the provider to close its session before this owner exits.
				cancelRun()
				<-runDone
				s.finishProviderStream(candidate, renewErr, false)
				return
			}
		case <-ctx.Done():
			runErr := <-runDone
			s.finishProviderStream(candidate, runErr, true)
			return
		}
	}
}

func (s *WorkerStore) renewProviderStreamLease(ctx context.Context, candidate providerStreamCandidate, now time.Time) (bool, error) {
	statement, args, err := query.NewUpdateBuilder(s.dialect, providerRunTable).
		Set("lease_expires_at", now.Add(providerStreamLeaseDuration).UnixMilli()).Set("updated_at", now.UnixMilli()).
		Where(query.And(query.Equal("run_kind", providerRunKindStream), query.Equal("id", candidate.id), query.Equal("status", "running"), query.Equal("lease_owner", s.workerID+":stream"), query.Equal("fencing_token", candidate.fencingToken))).Build()
	if err != nil {
		return false, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func (s *WorkerStore) finishProviderStream(candidate providerStreamCandidate, runErr error, shuttingDown bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	now := s.now()
	status, dueAt, attempt, errorCode := "ready", now.UnixMilli(), candidate.attemptCount, ""
	if !shuttingDown {
		attempt++
		status, dueAt, errorCode = "failed", now.Add(providerStreamRetryDelay(candidate.id, attempt)).UnixMilli(), "integration.provider_stream_failed"
		if code, ok := connector.ProviderErrorCodeOf(runErr); ok {
			errorCode = code
		}
		if classification, ok := connector.ErrorClassificationOf(runErr); ok && classification == connector.ErrorPermanent {
			status, dueAt = "dead_letter", int64(0)
		}
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, providerRunTable).
		Set("status", status).Set("due_at", dueAt).Set("attempt_count", attempt).Set("last_error_code", errorCode).
		Set("lease_owner", "").Set("lease_expires_at", int64(0)).Set("updated_at", now.UnixMilli()).
		Where(query.And(query.Equal("run_kind", providerRunKindStream), query.Equal("id", candidate.id), query.Equal("status", "running"), query.Equal("lease_owner", s.workerID+":stream"), query.Equal("fencing_token", candidate.fencingToken))).Build()
	if err == nil {
		_, _ = s.database.ExecContext(ctx, statement, args...)
	}
}

func providerStreamRetryDelay(id int64, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exponent := min(attempt-1, 6)
	base := time.Second * time.Duration(1<<exponent)
	if base > time.Minute {
		base = time.Minute
	}
	// Stable positive jitter disperses reconnects without package-global RNG
	// contention and remains deterministic in tests.
	jitterMillis := (id*1103515245 + int64(attempt*12345)) % max(int64(1), base.Milliseconds()/2)
	if jitterMillis < 0 {
		jitterMillis = -jitterMillis
	}
	return base + time.Duration(jitterMillis)*time.Millisecond
}

type providerStreamSink struct {
	store     *WorkerStore
	candidate providerStreamCandidate
	mu        sync.Mutex
	progress  string
}

func (s *providerStreamSink) Commit(ctx context.Context, checkpoint connector.StreamCheckpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkpoint.Validate(); err != nil {
		return err
	}
	now := s.store.now()
	progress, err := providerTaskProgressJSON(s.progress, checkpoint.Progress)
	if err != nil {
		return err
	}
	tx, err := s.store.database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	statement, args, err := query.NewUpdateBuilder(s.store.dialect, providerRunTable).
		Set("payload_json", string(checkpoint.State)).Set("progress_json", progress).Set("last_success_at", now.UnixMilli()).Set("attempt_count", 0).Set("last_error_code", "").
		Set("lease_expires_at", now.Add(providerStreamLeaseDuration).UnixMilli()).Set("updated_at", now.UnixMilli()).
		Where(query.And(subjectRowsWriteAllowed(s.store.subjectLifecycle, s.store.dialect, s.candidate.workspaceID, providerRunTable, strconv.FormatInt(s.candidate.id, 10)), query.Equal("run_kind", providerRunKindStream), query.Equal("id", s.candidate.id), query.Equal("status", "running"), query.Equal("lease_owner", s.store.workerID+":stream"), query.Equal("fencing_token", s.candidate.fencingToken))).Build()
	if err != nil {
		return err
	}
	updated, err := tx.ExecContext(ctx, statement, args...)
	if err != nil {
		return err
	}
	if affected, _ := updated.RowsAffected(); affected != 1 {
		return errProviderStreamLeaseLost
	}
	ownerUserID, err := connectionOwnerUserID(ctx, tx, s.store.dialect, s.candidate.workspaceID, s.candidate.connectionKey)
	if err != nil {
		return err
	}
	inserted := 0
	for _, event := range checkpoint.Events {
		accepted, err := insertProviderEvent(ctx, tx, s.store.dialect, s.store.subjectLifecycle, s.candidate.workspaceID, s.candidate.connectorKey, s.candidate.providerKey, s.candidate.connectionKey, ownerUserID, event, false, now)
		if err != nil {
			return err
		}
		if accepted {
			inserted++
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.progress = progress
	if inserted > 0 && s.store.eventWorkNotifier != nil {
		s.store.eventWorkNotifier.EventsCommitted()
	}
	return nil
}

var _ connector.StreamSink = (*providerStreamSink)(nil)
