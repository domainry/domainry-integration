package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

const (
	providerRunTable      = "_integration_provider_runs"
	providerRunKindState  = "state"
	providerRunKindCommit = "commit"

	// Provider I/O is independent across connections. Keep the execution pool
	// bounded so one process can overlap vendor latency without creating an
	// unbounded goroutine or database-write burst. The connection lease below
	// continues to serialize tasks that belong to the same connection.
	providerTaskExecutionConcurrency = 32
)

type WorkerStore struct {
	database         modulehost.Database
	dialect          modulehost.Dialect
	delivery         *DeliveryStore
	operations       *OperationsStore
	workerID         string
	clock            func() time.Time
	subjectLifecycle *SubjectLifecyclePersistence
	schedulerMu      sync.Mutex
	schedulerCursor  string
	metricCleanupMu  sync.Mutex
	metricCleanupAt  time.Time
}

func NewWorkerStore(database modulehost.Database, dialect modulehost.Dialect, delivery *DeliveryStore, operations *OperationsStore, workerID string, subjectLifecycle ...*SubjectLifecyclePersistence) *WorkerStore {
	return &WorkerStore{database: database, dialect: dialect, delivery: delivery, operations: operations, workerID: strings.TrimSpace(workerID) + ":integration", subjectLifecycle: subjectLifecyclePersistence(subjectLifecycle)}
}

func (s *WorkerStore) ProcessDueEvents(ctx context.Context, limit int) (int, error) {
	return s.operations.ProcessDueEvents(ctx, s.workerID, limit)
}

func providerBackgroundProcessor(provider connector.Adapter) (connector.BackgroundProcessor, bool) {
	if capability, ok := provider.(connector.BackgroundCapabilityProvider); ok {
		return capability.BackgroundProcessor()
	}
	processor, ok := provider.(connector.BackgroundProcessor)
	return processor, ok
}

func (s *WorkerStore) ProcessDueProviderTasks(ctx context.Context, limit int) (int, error) {
	processed, taskErr := s.processProviderTasks(ctx, limit)
	committed, commitErr := s.processProviderCommits(ctx, limit)
	cleanupErr := s.cleanupExpiredProviderMetricWindows(ctx, s.now())
	if taskErr != nil {
		return processed + committed, taskErr
	}
	if commitErr != nil {
		return processed + committed, commitErr
	}
	return processed + committed, cleanupErr
}

func (s *WorkerStore) cleanupExpiredProviderMetricWindows(ctx context.Context, now time.Time) error {
	s.metricCleanupMu.Lock()
	defer s.metricCleanupMu.Unlock()
	if !s.metricCleanupAt.IsZero() && now.Before(s.metricCleanupAt.Add(time.Hour)) {
		return nil
	}
	if err := cleanupProviderMetricWindows(ctx, s.database, s.dialect, now); err != nil {
		return err
	}
	if err := cleanupProviderWakeReceipts(ctx, s.database, s.dialect, now); err != nil {
		return err
	}
	s.metricCleanupAt = now
	return nil
}

type providerTaskCandidate struct {
	id, workspaceID, connectorKey, providerKey, connectionKey, taskKey string
	dueAt, leaseExpiresAt                                              int64
	stateVersion, attemptCount                                         int
	payload, progress, status                                          string
	fencingToken, wakeSequence                                         int64
}

func (s *WorkerStore) processProviderTasks(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 500 {
		limit = 25
	}
	now := s.now()
	candidates, err := s.listFairProviderTaskCandidates(ctx, now, limit)
	if err != nil {
		return 0, err
	}
	type claimedProviderTask struct {
		candidate  providerTaskCandidate
		leaseOwner string
	}
	var firstErr error
	processed := 0
	pending := candidates
	for len(pending) > 0 {
		// A connection may own multiple task rows. Put duplicates in a later
		// wave so they retain the previous in-batch behavior while unrelated
		// connections overlap their Provider I/O.
		wave := make([]providerTaskCandidate, 0, len(pending))
		deferred := make([]providerTaskCandidate, 0)
		connections := make(map[string]struct{}, len(pending))
		for _, candidate := range pending {
			connectionID := candidate.workspaceID + "\x00" + candidate.connectionKey
			if _, exists := connections[connectionID]; exists {
				deferred = append(deferred, candidate)
				continue
			}
			connections[connectionID] = struct{}{}
			wave = append(wave, candidate)
		}
		claimedTasks := make([]claimedProviderTask, 0, len(wave))
		for _, candidate := range wave {
			leaseOwner, claimed, err := s.acquireProviderTask(ctx, candidate, now)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if !claimed {
				continue
			}
			claimedTasks = append(claimedTasks, claimedProviderTask{candidate: candidate, leaseOwner: leaseOwner})
		}
		if len(claimedTasks) > 0 {
			processed += len(claimedTasks)
			concurrency := min(providerTaskExecutionConcurrency, len(claimedTasks))
			jobs := make(chan claimedProviderTask)
			var workers sync.WaitGroup
			var errorMu sync.Mutex
			recordError := func(err error) {
				if err == nil {
					return
				}
				errorMu.Lock()
				defer errorMu.Unlock()
				if firstErr == nil {
					firstErr = err
				}
			}
			workers.Add(concurrency)
			for range concurrency {
				go func() {
					defer workers.Done()
					for task := range jobs {
						executionErr := s.executeProviderTask(ctx, task.candidate, now)
						releaseErr := s.releaseProviderConnectionLease(ctx, task.candidate, task.leaseOwner, now)
						recordError(executionErr)
						recordError(releaseErr)
					}
				}()
			}
			for _, task := range claimedTasks {
				jobs <- task
			}
			close(jobs)
			workers.Wait()
		}
		pending = deferred
	}
	return processed, firstErr
}

func dueProviderTaskPredicate(now time.Time) query.Predicate {
	nowMillis := providerDeadlineScanEnd(now)
	return query.And(
		query.Equal("run_kind", providerRunKindState), query.In("status", "ready", "failed", "processing"), query.LessThanOrEqual("due_at", nowMillis),
		query.Or(query.Equal("lease_expires_at", int64(0)), query.LessThanOrEqual("lease_expires_at", nowMillis)),
	)
}

func (s *WorkerStore) listFairProviderTaskCandidates(ctx context.Context, now time.Time, limit int) ([]providerTaskCandidate, error) {
	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()
	// An empty or entirely dead-lettered queue must not scan every workspace
	// row merely to discover that no runnable task exists.
	if available, err := s.hasDueProviderTask(ctx, now); err != nil {
		return nil, err
	} else if !available {
		return nil, nil
	}
	workspaces := make([]string, 0, limit)
	cursor := s.schedulerCursor
	for len(workspaces) < limit {
		workspaceID, found, err := s.nextDueProviderWorkspace(ctx, now, cursor)
		if err != nil {
			return nil, err
		}
		if !found {
			if cursor == "" {
				break
			}
			cursor = ""
			continue
		}
		if len(workspaces) > 0 && workspaceID == workspaces[0] {
			break
		}
		workspaces = append(workspaces, workspaceID)
		cursor = workspaceID
	}
	if len(workspaces) == 0 {
		return nil, nil
	}
	s.schedulerCursor = workspaces[len(workspaces)-1]
	perWorkspace := (limit + len(workspaces) - 1) / len(workspaces)
	groups := make([][]providerTaskCandidate, len(workspaces))
	for index, workspaceID := range workspaces {
		statement, values, buildErr := query.NewSelectBuilder(s.dialect, providerRunTable).
			Columns("id", "workspace_id", "connector_key", "provider_key", "connection_key", "task_key", "state_version", "payload_json", "progress_json", "status", "attempt_count", "fencing_token", "wake_sequence", "due_at", "lease_expires_at").
			Where(query.And(dueProviderTaskPredicate(now), query.Equal("workspace_id", workspaceID))).
			OrderBy(query.Ascending("due_at"), query.Ascending("connection_key"), query.Ascending("task_key")).Limit(perWorkspace).Build()
		if buildErr != nil {
			return nil, buildErr
		}
		workspaceRows, queryErr := s.database.QueryContext(ctx, statement, values...)
		if queryErr != nil {
			return nil, queryErr
		}
		for workspaceRows.Next() {
			var value providerTaskCandidate
			var progress sql.NullString
			if scanErr := workspaceRows.Scan(&value.id, &value.workspaceID, &value.connectorKey, &value.providerKey, &value.connectionKey, &value.taskKey, &value.stateVersion, &value.payload, &progress, &value.status, &value.attemptCount, &value.fencingToken, &value.wakeSequence, &value.dueAt, &value.leaseExpiresAt); scanErr != nil {
				_ = workspaceRows.Close()
				return nil, scanErr
			}
			if progress.Valid {
				value.progress = progress.String
			}
			groups[index] = append(groups[index], value)
		}
		if queryErr = workspaceRows.Err(); queryErr != nil {
			_ = workspaceRows.Close()
			return nil, queryErr
		}
		_ = workspaceRows.Close()
	}
	candidates := make([]providerTaskCandidate, 0, limit)
	for round := 0; len(candidates) < limit; round++ {
		added := false
		for _, group := range groups {
			if round >= len(group) {
				continue
			}
			candidates = append(candidates, group[round])
			added = true
			if len(candidates) == limit {
				break
			}
		}
		if !added {
			break
		}
	}
	return candidates, nil
}

func (s *WorkerStore) hasDueProviderTask(ctx context.Context, now time.Time) (bool, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, providerRunTable).
		Columns("id").Where(dueProviderTaskPredicate(now)).Limit(1).Build()
	if err != nil {
		return false, err
	}
	var id string
	err = s.database.QueryRowContext(ctx, statement, args...).Scan(&id)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// One indexed seek finds the next eligible workspace after the cursor. Unlike
// GROUP BY workspace_id, it does not revisit every due account on each batch.
// The cursor wraps across batches so a workspace with a large backlog cannot
// hide a later workspace forever.
func (s *WorkerStore) nextDueProviderWorkspace(ctx context.Context, now time.Time, after string) (string, bool, error) {
	predicate := dueProviderTaskPredicate(now)
	if after != "" {
		predicate = query.And(predicate, query.GreaterThan("workspace_id", after))
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, providerRunTable).
		Columns("workspace_id").Where(predicate).OrderBy(query.Ascending("workspace_id")).Limit(1).Build()
	if err != nil {
		return "", false, err
	}
	var workspaceID string
	err = s.database.QueryRowContext(ctx, statement, args...).Scan(&workspaceID)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	return workspaceID, err == nil, err
}

func (s *WorkerStore) claimProviderTaskWith(ctx context.Context, database sqlhost.DBTX, candidate providerTaskCandidate, now time.Time) (bool, error) {
	statement, args, err := query.NewUpdateBuilder(s.dialect, providerRunTable).Set("status", "processing").Set("lease_owner", s.workerID).Set("lease_expires_at", now.Add(providerTaskLeaseDuration).UnixMilli()).Set("fencing_token", candidate.fencingToken+1).Set("updated_at", now.UnixMilli()).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, candidate.workspaceID, providerRunTable, candidate.id), query.And(query.Equal("run_kind", providerRunKindState), query.Equal("id", candidate.id), query.Equal("status", candidate.status), query.Equal("fencing_token", candidate.fencingToken), query.Equal("due_at", candidate.dueAt), query.Equal("lease_expires_at", candidate.leaseExpiresAt)))).Build()
	if err != nil {
		return false, err
	}
	result, err := database.ExecContext(ctx, statement, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func (s *WorkerStore) executeProviderTask(ctx context.Context, candidate providerTaskCandidate, now time.Time) error {
	connection, err := s.delivery.connection(ctx, deliveryRequest(candidate.workspaceID, candidate.connectorKey, candidate.connectionKey))
	if err != nil {
		return s.failProviderTask(ctx, candidate, err, now)
	}
	provider, ok := s.delivery.providers.Provider(connection.ConnectorKey, connection.ProviderKey)
	if !ok {
		return s.failProviderTask(ctx, candidate, fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey), now)
	}
	processor, ok := providerBackgroundProcessor(provider)
	if !ok {
		return s.failProviderTask(ctx, candidate, fmt.Errorf("Integration provider %s/%s has no background processor", connection.ConnectorKey, connection.ProviderKey), now)
	}
	resolvedSecrets, err := s.delivery.resolveProviderSecrets(ctx, candidate.workspaceID, connection.SecretRefs)
	if err != nil {
		return s.failProviderTask(ctx, candidate, err, now)
	}
	result, err := processor.ProcessBackground(ctx, connector.BackgroundRequest{
		TaskKey: candidate.taskKey, StateVersion: candidate.stateVersion,
		Connection: connector.Connection{Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs},
		State:      json.RawMessage(candidate.payload), Secrets: resolvedSecrets.Values, Now: now,
		Principal: connector.Principal{WorkspaceID: candidate.workspaceID, RequestID: candidate.id, IsAuthenticated: true},
	})
	if err == nil {
		err = result.Validate()
	}
	err = s.delivery.persistProviderSecretUpdates(ctx, candidate.workspaceID, connection.SecretRefs, resolvedSecrets.Versions, result.SecretUpdates, err)
	if err != nil {
		return s.failProviderTask(ctx, candidate, err, now)
	}
	if err := s.commitProviderTaskResult(ctx, candidate, result, now); err != nil {
		return s.failProviderTask(ctx, candidate, err, now)
	}
	return nil
}

func deliveryRequest(workspaceID, connectorKey, connectionKey string) integrationmodel.DeliveryRequest {
	return integrationmodel.DeliveryRequest{WorkspaceID: workspaceID, ConnectorKey: connectorKey, ConnectionKey: connectionKey}
}

func (s *WorkerStore) commitProviderTaskResult(ctx context.Context, candidate providerTaskCandidate, result connector.BackgroundResult, now time.Time) error {
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	dueAt := result.NextDueAt.UTC()
	if dueAt.IsZero() {
		dueAt = now.Add(5 * time.Minute)
	}
	newWake := query.GreaterThan("wake_sequence", candidate.wakeSequence)
	progressJSON, err := providerTaskProgressJSON(candidate.progress, result.Progress)
	if err != nil {
		return err
	}
	update, args, err := query.NewUpdateBuilder(s.dialect, providerRunTable).Set("payload_json", string(result.State)).Set("progress_json", progressJSON).Set("status", "ready").
		SetExpression("due_at", query.CaseWhen(newWake, now.UnixMilli()).Else(dueAt.UTC().UnixMilli())).
		Set("last_error_code", "").Set("attempt_count", 0).Set("last_success_at", now.UnixMilli()).Set("lease_owner", "").Set("lease_expires_at", int64(0)).Set("updated_at", now.UnixMilli()).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, candidate.workspaceID, providerRunTable, candidate.id), query.And(query.Equal("run_kind", providerRunKindState), query.Equal("id", candidate.id), query.Equal("lease_owner", s.workerID), query.Equal("fencing_token", candidate.fencingToken+1)))).Build()
	if err != nil {
		return err
	}
	updated, err := tx.ExecContext(ctx, update, args...)
	if err != nil {
		return err
	}
	if affected, _ := updated.RowsAffected(); affected != 1 {
		return fmt.Errorf("Integration Provider task %q lost its lease", candidate.id)
	}
	if err := recordProviderHistoryObservations(ctx, tx, s.dialect, candidate, result.Observations, now); err != nil {
		return err
	}
	if err := completeProviderWakeReceipts(ctx, tx, s.dialect, candidate, now); err != nil {
		return err
	}
	ownerUserID, err := connectionOwnerUserID(ctx, tx, s.dialect, candidate.workspaceID, candidate.connectionKey)
	if err != nil {
		return err
	}
	for _, event := range result.Events {
		if _, err := insertProviderEvent(ctx, tx, s.dialect, s.subjectLifecycle, candidate.workspaceID, candidate.connectorKey, candidate.providerKey, candidate.connectionKey, ownerUserID, event, false, now); err != nil {
			return err
		}
	}
	for index, commit := range result.Commit {
		operation, ok := providerOperationForWorker(s.delivery.providers, candidate.connectorKey, candidate.providerKey, commit.OperationKey)
		if !ok || operation.ContractSHA256 != commit.ContractSHA256 {
			return fmt.Errorf("Integration Provider background commit %q does not match its operation contract", commit.OperationKey)
		}
		digest := sha256.Sum256([]byte(candidate.id + "\x00" + fmt.Sprint(candidate.fencingToken+1) + "\x00" + fmt.Sprint(index)))
		commitID := "provider_commit:" + hex.EncodeToString(digest[:])
		insert, values, err := query.NewInsertBuilder(s.dialect, providerRunTable).
			Columns("id", "workspace_id", "run_kind", "run_key", "connector_key", "provider_key", "connection_key", "task_key", "state_version", "operation_key", "contract_sha256", "payload_json", "status", "last_error_code", "attempt_count", "due_at", "lease_owner", "lease_expires_at", "fencing_token", "created_at", "updated_at").
			Values(commitID, candidate.workspaceID, providerRunKindCommit, commitID, candidate.connectorKey, candidate.providerKey, candidate.connectionKey, candidate.taskKey, 0, commit.OperationKey, commit.ContractSHA256, string(commit.Payload), "pending", "", 0, now.UnixMilli(), "", int64(0), 0, now.UnixMilli(), now.UnixMilli()).Build()
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, insert, values...); err != nil {
			return err
		}
	}
	for _, taskKey := range result.WakeTasks {
		if _, err := wakeProviderTasks(ctx, tx, s.dialect, s.subjectLifecycle, candidate.workspaceID, candidate.connectionKey, []string{taskKey}, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func providerTaskProgressJSON(previousJSON string, next *connector.BackgroundProgress) (string, error) {
	previousJSON = strings.TrimSpace(previousJSON)
	if previousJSON == "" {
		previousJSON = `{}`
	}
	if next == nil {
		return previousJSON, nil
	}
	if err := next.Validate(); err != nil {
		return "", err
	}
	if previousJSON != `{}` {
		var previous connector.BackgroundProgress
		if err := json.Unmarshal([]byte(previousJSON), &previous); err != nil || previous.Validate() != nil {
			return "", fmt.Errorf("Integration Provider task stored progress is invalid")
		}
		if previous.InitializationComplete && !next.InitializationComplete {
			return "", fmt.Errorf("Integration Provider task initialization progress cannot regress")
		}
		if next.CompletedUnits < previous.CompletedUnits {
			return "", fmt.Errorf("Integration Provider task completed units cannot decrease")
		}
	}
	encoded, err := json.Marshal(next)
	if err != nil {
		return "", fmt.Errorf("encode Integration Provider task progress: %w", err)
	}
	return string(encoded), nil
}

func providerOperationForWorker(registry modulehost.ProviderRegistry, connectorKey, providerKey, operationKey string) (connector.OperationDescriptor, bool) {
	provider, ok := registry.Provider(connectorKey, providerKey)
	if !ok {
		return connector.OperationDescriptor{}, false
	}
	return providerOperation(provider.Descriptor(), operationKey)
}

func (s *WorkerStore) failProviderTask(ctx context.Context, candidate providerTaskCandidate, processErr error, now time.Time) error {
	attempt := candidate.attemptCount + 1
	status, dueAt := "failed", now.Add(integrationEventRetryDelay(attempt)).UnixMilli()
	if attempt >= 5 {
		status, dueAt = "dead_letter", int64(0)
	}
	errorCode := "integration.provider_task_failed"
	if providerCode, ok := connector.ProviderErrorCodeOf(processErr); ok {
		errorCode = providerCode
	}
	tx, err := s.database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("start Integration Provider task failure transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	newWake := query.GreaterThan("wake_sequence", candidate.wakeSequence)
	statement, args, err := query.NewUpdateBuilder(s.dialect, providerRunTable).
		SetExpression("status", query.CaseWhen(newWake, "ready").Else(status)).
		SetExpression("due_at", query.CaseWhen(newWake, now.UnixMilli()).Else(dueAt)).
		Set("last_error_code", errorCode).
		SetExpression("attempt_count", query.CaseWhen(newWake, 0).Else(attempt)).
		Set("lease_owner", "").Set("lease_expires_at", int64(0)).Set("updated_at", now.UnixMilli()).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, candidate.workspaceID, providerRunTable, candidate.id), query.And(query.Equal("run_kind", providerRunKindState), query.Equal("id", candidate.id), query.Equal("lease_owner", s.workerID), query.Equal("fencing_token", candidate.fencingToken+1)))).Build()
	if err != nil {
		return err
	}
	updated, err := tx.ExecContext(ctx, statement, args...)
	if err != nil {
		return fmt.Errorf("persist Integration Provider task failure: %w", err)
	}
	affected, err := updated.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("Integration Provider task %q lost its lease before failure commit", candidate.id)
	}
	if errorCode == "google.http_429" {
		if err := recordGoogleHTTP429(ctx, tx, s.dialect, candidate.id, now); err != nil {
			return fmt.Errorf("record Integration Google HTTP 429 event: %w", err)
		}
	}
	if isGoogleGmailRateLimitCode(errorCode) {
		if err := recordGoogleGmailRateLimit(ctx, tx, s.dialect, errorCode, candidate.id, false, now); err != nil {
			return fmt.Errorf("record Integration Google Gmail rate-limit event: %w", err)
		}
	}
	if isFeishuRateLimitCode(errorCode) {
		if err := recordFeishuRateLimit(ctx, tx, s.dialect, candidate.id, false, now); err != nil {
			return fmt.Errorf("record Integration Feishu rate-limit event: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return processErr
}

type providerCommitCandidate struct {
	id, workspaceID, connectorKey, providerKey, connectionKey, operationKey, payload, status string
	dueAt, leaseExpiresAt                                                                    int64
	attemptCount                                                                             int
	fencingToken                                                                             int64
}

func (s *WorkerStore) processProviderCommits(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 500 {
		limit = 25
	}
	now := s.now()
	statement, args, err := query.NewSelectBuilder(s.dialect, providerRunTable).Columns("id", "workspace_id", "connector_key", "provider_key", "connection_key", "operation_key", "payload_json", "status", "attempt_count", "fencing_token", "due_at", "lease_expires_at").Where(query.And(query.Equal("run_kind", providerRunKindCommit), query.In("status", "pending", "failed"), query.LessThanOrEqual("due_at", providerDeadlineScanEnd(now)), query.Or(query.Equal("lease_expires_at", int64(0)), query.LessThanOrEqual("lease_expires_at", providerDeadlineScanEnd(now))))).OrderBy(query.Ascending("due_at")).Limit(limit).Build()
	if err != nil {
		return 0, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return 0, err
	}
	candidates := []providerCommitCandidate{}
	for rows.Next() {
		var value providerCommitCandidate
		if err := rows.Scan(&value.id, &value.workspaceID, &value.connectorKey, &value.providerKey, &value.connectionKey, &value.operationKey, &value.payload, &value.status, &value.attemptCount, &value.fencingToken, &value.dueAt, &value.leaseExpiresAt); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ready, err := providerDeadlineReady(value.dueAt, value.leaseExpiresAt, now)
		if err != nil {
			_ = rows.Close()
			return 0, err
		}
		if ready {
			candidates = append(candidates, value)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	_ = rows.Close()
	processed := 0
	var firstErr error
	for _, candidate := range candidates {
		claim, values, err := query.NewUpdateBuilder(s.dialect, providerRunTable).Set("status", "processing").Set("lease_owner", s.workerID).Set("lease_expires_at", now.Add(time.Minute).UnixMilli()).Set("fencing_token", candidate.fencingToken+1).Set("updated_at", now.UnixMilli()).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, candidate.workspaceID, providerRunTable, candidate.id), query.And(query.Equal("run_kind", providerRunKindCommit), query.Equal("id", candidate.id), query.Equal("status", candidate.status), query.Equal("fencing_token", candidate.fencingToken), query.Equal("due_at", candidate.dueAt), query.Equal("lease_expires_at", candidate.leaseExpiresAt)))).Build()
		if err != nil {
			return processed, err
		}
		claimed, err := s.database.ExecContext(ctx, claim, values...)
		if err != nil {
			return processed, err
		}
		if affected, _ := claimed.RowsAffected(); affected != 1 {
			continue
		}
		processed++
		_, deliveryErr := s.delivery.Accept(ctx, integrationmodel.DeliveryRequest{MessageID: candidate.id, DeduplicationKey: candidate.id, WorkspaceID: candidate.workspaceID, ConnectorKey: candidate.connectorKey, ConnectionKey: candidate.connectionKey, Operation: candidate.operationKey, Payload: json.RawMessage(candidate.payload)})
		status, dueAt, attempt := "succeeded", int64(0), candidate.attemptCount
		if deliveryErr != nil {
			status, attempt, dueAt = "failed", candidate.attemptCount+1, now.Add(integrationEventRetryDelay(candidate.attemptCount+1)).UnixMilli()
			if attempt >= 5 {
				status, dueAt = "dead_letter", int64(0)
			}
			if firstErr == nil {
				firstErr = deliveryErr
			}
		}
		update, values, err := query.NewUpdateBuilder(s.dialect, providerRunTable).Set("status", status).Set("attempt_count", attempt).Set("due_at", dueAt).Set("lease_owner", "").Set("lease_expires_at", int64(0)).Set("updated_at", now.UnixMilli()).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, candidate.workspaceID, providerRunTable, candidate.id), query.And(query.Equal("run_kind", providerRunKindCommit), query.Equal("id", candidate.id), query.Equal("lease_owner", s.workerID), query.Equal("fencing_token", candidate.fencingToken+1)))).Build()
		if err != nil {
			return processed, err
		}
		if _, err := s.database.ExecContext(ctx, update, values...); err != nil {
			return processed, err
		}
	}
	return processed, firstErr
}

func (s *WorkerStore) ProcessDueReconciliations(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 500 {
		limit = 25
	}
	now := s.now()
	invocations, err := s.failedInvocationsAcrossWorkspaces(ctx, limit, now)
	if err != nil {
		return 0, err
	}
	processed := 0
	var firstErr error
	for _, invocation := range invocations {
		provider, ok := s.delivery.providers.Provider(invocation.ConnectorKey, invocation.ProviderKey)
		if !ok {
			continue
		}
		operation, ok := providerOperation(provider.Descriptor(), invocation.Operation)
		if !ok || operation.Reliability.Reconciliation != connector.ReconciliationProviderLookup {
			continue
		}
		reconciler, ok := provider.(connector.Reconciler)
		if !ok {
			if firstErr == nil {
				firstErr = fmt.Errorf("Integration provider %s/%s requires reconciliation but exposes no Reconciler", invocation.ConnectorKey, invocation.ProviderKey)
			}
			continue
		}
		claim, claimArgs, err := query.NewUpdateBuilder(s.dialect, "_integration_invocations").Set("status", "reconciling").Set("updated_at", now.UnixMilli()).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, invocation.WorkspaceID, "_integration_invocations", invocation.ID), query.And(query.Equal("workspace_id", invocation.WorkspaceID), query.Equal("id", invocation.ID), query.Equal("status", invocation.Status), query.Equal("updated_at", timestampMillis(invocation.UpdatedAt))))).Build()
		if err != nil {
			return processed, err
		}
		claimed, err := s.database.ExecContext(ctx, claim, claimArgs...)
		if err != nil {
			return processed, err
		}
		if affected, _ := claimed.RowsAffected(); affected != 1 {
			continue
		}
		connection, err := s.delivery.connection(ctx, deliveryRequest(invocation.WorkspaceID, invocation.ConnectorKey, invocation.ConnectionKey))
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		resolvedSecrets, err := s.delivery.resolveProviderSecrets(ctx, invocation.WorkspaceID, connection.SecretRefs)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		payload, _ := json.Marshal(invocation.Metadata["request"])
		result, reconcileErr := reconciler.Reconcile(ctx, connector.ReconcileRequest{
			ConnectorKey: invocation.ConnectorKey, ProviderKey: invocation.ProviderKey, OperationKey: invocation.Operation,
			ContractSHA256: operation.ContractSHA256,
			Connection:     connector.Connection{Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs},
			RequestRef:     invocation.RequestRef, ResponseRef: invocation.ResponseRef, Payload: payload, Secrets: resolvedSecrets.Values,
			Principal: connector.Principal{WorkspaceID: invocation.WorkspaceID, RequestID: invocation.RequestRef, IsAuthenticated: true},
		})
		if reconcileErr == nil {
			reconcileErr = result.Validate()
		}
		if result.Result != nil {
			reconcileErr = s.delivery.persistProviderSecretUpdates(ctx, invocation.WorkspaceID, connection.SecretRefs, resolvedSecrets.Versions, result.Result.SecretUpdates, reconcileErr)
		}
		if err := s.persistReconciliation(ctx, invocation, result, reconcileErr, now); err != nil && firstErr == nil {
			firstErr = err
		}
		processed++
	}
	return processed, firstErr
}

func (s *WorkerStore) failedInvocationsAcrossWorkspaces(ctx context.Context, limit int, now time.Time) ([]integrationmodel.Invocation, error) {
	// Account writes have their own receipt lifecycle and never enter automatic
	// provider reconciliation, even if a future registry changes its policy.
	// Filter before LIMIT so failed account writes cannot starve legacy work.
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_invocations").Columns(invocationColumns()...).Where(query.And(query.NotLike("id", "account-write:%"), query.Or(
		query.Equal("status", "failed"), query.And(query.Equal("status", "reconciling"), query.LessThanOrEqual("updated_at", now.Add(-time.Minute).UnixMilli())),
	))).OrderBy(query.Ascending("updated_at")).Limit(limit * 4).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []integrationmodel.Invocation{}
	for rows.Next() {
		value, err := scanInvocation(rows)
		if err != nil {
			return nil, err
		}
		if terminal, _ := value.Metadata["reconciliation_terminal"].(bool); terminal {
			continue
		}
		if next, ok := persistedMillis(value.Metadata["reconciliation_next_at"]); ok && next > now.UnixMilli() {
			continue
		}
		values = append(values, value)
		if len(values) == limit {
			break
		}
	}
	return values, rows.Err()
}

func persistedMillis(value any) (int64, bool) {
	switch value := value.(type) {
	case int64:
		return value, true
	case int:
		return int64(value), true
	case float64:
		millis := int64(value)
		return millis, float64(millis) == value
	case json.Number:
		millis, err := value.Int64()
		return millis, err == nil
	default:
		return 0, false
	}
}

func (s *WorkerStore) persistReconciliation(ctx context.Context, invocation integrationmodel.Invocation, result connector.ReconcileResult, reconcileErr error, now time.Time) error {
	metadata := invocation.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	attempt, _ := metadata["reconciliation_attempt"].(float64)
	metadata["reconciliation_attempt"] = int(attempt) + 1
	metadata["reconciliation_outcome"] = string(result.Outcome)
	status, errorText, responseRef := "failed", invocation.Error, invocation.ResponseRef
	if reconcileErr != nil {
		metadata["reconciliation_next_at"] = now.Add(5 * time.Minute).UnixMilli()
		errorText = reconcileErr.Error()
	} else {
		switch result.Outcome {
		case connector.ReconciliationSucceeded:
			status, errorText, metadata["reconciliation_terminal"] = "succeeded", "", true
			delete(metadata, "reconciliation_next_at")
			if result.Result != nil {
				responseRef = result.Result.ResponseRef
				metadata["response"] = json.RawMessage(result.Result.Payload)
			}
		case connector.ReconciliationPending:
			metadata["reconciliation_next_at"] = now.Add(result.RetryAfter).UnixMilli()
		case connector.ReconciliationFailed, connector.ReconciliationNotFound, connector.ReconciliationUnknown:
			metadata["reconciliation_terminal"] = true
			delete(metadata, "reconciliation_next_at")
			errorText = result.FailureCode
		}
	}
	payload, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_invocations").Set("status", status).Set("response_ref", responseRef).Set("error", errorText).Set("metadata_json", string(payload)).Set("updated_at", now.UnixMilli()).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, invocation.WorkspaceID, "_integration_invocations", invocation.ID), query.And(query.Equal("workspace_id", invocation.WorkspaceID), query.Equal("id", invocation.ID)))).Build()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, statement, args...)
	return err
}

func (s *WorkerStore) ProcessDueCredentialExpirations(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 500 {
		limit = 25
	}
	now := s.now().UnixMilli()
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns("id", "workspace_id", "secret_key").Where(query.And(query.Equal("status", "active"), query.NotEqual("expires_at", int64(0)), query.LessThanOrEqual("expires_at", now))).OrderBy(query.Ascending("expires_at")).Limit(limit).Build()
	if err != nil {
		return 0, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return 0, err
	}
	type expired struct{ id, workspaceID, key string }
	values := []expired{}
	for rows.Next() {
		var value expired
		if err := rows.Scan(&value.id, &value.workspaceID, &value.key); err != nil {
			_ = rows.Close()
			return 0, err
		}
		values = append(values, value)
	}
	_ = rows.Close()
	processed := 0
	for _, value := range values {
		update, args, err := query.NewUpdateBuilder(s.dialect, "_integration_secrets").Set("status", "expired").Set("updated_at", now).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, value.workspaceID, "_integration_secrets", value.id), query.And(query.Equal("id", value.id), query.Equal("workspace_id", value.workspaceID), query.Equal("secret_key", value.key), query.Equal("status", "active")))).Build()
		if err != nil {
			return processed, err
		}
		result, err := s.database.ExecContext(ctx, update, args...)
		if err != nil {
			return processed, err
		}
		if affected, _ := result.RowsAffected(); affected == 1 {
			processed++
		}
	}
	return processed, nil
}

var _ integrationsdk.LocalWorkers = (*WorkerStore)(nil)
