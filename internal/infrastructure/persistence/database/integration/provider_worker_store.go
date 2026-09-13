package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

type WorkerStore struct {
	database   modulehost.Database
	dialect    modulehost.Dialect
	delivery   *DeliveryStore
	operations *OperationsStore
	workerID   string
	clock      func() time.Time
}

func NewWorkerStore(database modulehost.Database, dialect modulehost.Dialect, delivery *DeliveryStore, operations *OperationsStore, workerID string) *WorkerStore {
	return &WorkerStore{database: database, dialect: dialect, delivery: delivery, operations: operations, workerID: strings.TrimSpace(workerID) + ":integration"}
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
	if err := s.synchronizeProviderTasks(ctx); err != nil {
		return 0, err
	}
	processed, taskErr := s.processProviderTasks(ctx, limit)
	committed, commitErr := s.processProviderCommits(ctx, limit)
	if taskErr != nil {
		return processed + committed, taskErr
	}
	return processed + committed, commitErr
}

func (s *WorkerStore) synchronizeProviderTasks(ctx context.Context) error {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connections").Columns(
		"connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at",
	).Where(query.Equal("status", "active")).Build()
	if err != nil {
		return err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return fmt.Errorf("list Integration Provider task connections: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		connection, err := scanConnection(rows)
		if err != nil {
			return err
		}
		provider, ok := s.delivery.providers.Provider(connection.ConnectorKey, connection.ProviderKey)
		if !ok {
			return fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
		}
		processor, ok := providerBackgroundProcessor(provider)
		if !ok {
			continue
		}
		providerConnection := connector.Connection{Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Name: connection.Name, Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs}
		for _, task := range processor.BackgroundTasks(providerConnection) {
			if err := task.Validate(); err != nil {
				return fmt.Errorf("validate Integration Provider background task: %w", err)
			}
			if err := s.upsertProviderTask(ctx, providerConnection, task); err != nil {
				return err
			}
		}
	}
	return rows.Err()
}

func (s *WorkerStore) upsertProviderTask(ctx context.Context, connection connector.Connection, task connector.BackgroundTaskDescriptor) error {
	lookup, args, err := query.NewSelectBuilder(s.dialect, "_integration_connector_provider_states").Columns("id", "state_version").Where(query.And(query.Equal("workspace_id", connection.WorkspaceID), query.Equal("connection_key", connection.Key), query.Equal("task_key", task.Key))).Build()
	if err != nil {
		return err
	}
	var id string
	var stateVersion int
	err = s.database.QueryRowContext(ctx, lookup, args...).Scan(&id, &stateVersion)
	now := s.now().Format(time.RFC3339Nano)
	if err == sql.ErrNoRows {
		id = ownerID("provider_task_", connection.WorkspaceID, connection.Key+"\x00"+task.Key)
		statement, values, buildErr := query.NewInsertBuilder(s.dialect, "_integration_connector_provider_states").Columns("id", "workspace_id", "connector_key", "provider_key", "connection_key", "task_key", "state_version", "payload_json", "status", "due_at", "last_error_code", "attempt_count", "lease_owner", "lease_expires_at", "fencing_token", "updated_at").Values(id, connection.WorkspaceID, connection.ConnectorKey, connection.ProviderKey, connection.Key, task.Key, task.StateVersion, `{}`, "ready", now, "", 0, "", "", 0, now).Build()
		if buildErr != nil {
			return buildErr
		}
		_, err = s.database.ExecContext(ctx, statement, values...)
		return err
	}
	if err != nil {
		return err
	}
	if stateVersion == task.StateVersion {
		return nil
	}
	statement, values, err := query.NewUpdateBuilder(s.dialect, "_integration_connector_provider_states").Set("state_version", task.StateVersion).Set("payload_json", `{}`).Set("status", "ready").Set("due_at", now).Set("last_error_code", "").Set("attempt_count", 0).Set("lease_owner", "").Set("lease_expires_at", "").Set("updated_at", now).Where(query.And(subjectRowWriteAllowed("_integration_connector_provider_states"), query.Equal("id", id))).Build()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, statement, values...)
	return err
}

type providerTaskCandidate struct {
	id, workspaceID, connectorKey, providerKey, connectionKey, taskKey string
	dueAt, leaseExpiresAt                                              string
	stateVersion, attemptCount                                         int
	payload, status                                                    string
	fencingToken                                                       int64
}

func (s *WorkerStore) processProviderTasks(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 500 {
		limit = 25
	}
	now := s.now()
	nowText := providerDeadlineScanEnd(now)
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connector_provider_states").Columns("id", "workspace_id", "connector_key", "provider_key", "connection_key", "task_key", "state_version", "payload_json", "status", "attempt_count", "fencing_token", "due_at", "lease_expires_at").Where(query.And(
		query.In("status", "ready", "failed"), query.LessThan("due_at", nowText), query.Or(query.Equal("lease_expires_at", ""), query.LessThan("lease_expires_at", nowText)),
	)).OrderBy(query.Ascending("due_at")).Limit(limit).Build()
	if err != nil {
		return 0, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return 0, err
	}
	candidates := []providerTaskCandidate{}
	for rows.Next() {
		var value providerTaskCandidate
		if err := rows.Scan(&value.id, &value.workspaceID, &value.connectorKey, &value.providerKey, &value.connectionKey, &value.taskKey, &value.stateVersion, &value.payload, &value.status, &value.attemptCount, &value.fencingToken, &value.dueAt, &value.leaseExpiresAt); err != nil {
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
		claimed, err := s.claimProviderTask(ctx, candidate, now)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !claimed {
			continue
		}
		processed++
		if err := s.executeProviderTask(ctx, candidate, now); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return processed, firstErr
}

func (s *WorkerStore) claimProviderTask(ctx context.Context, candidate providerTaskCandidate, now time.Time) (bool, error) {
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_connector_provider_states").Set("status", "processing").Set("lease_owner", s.workerID).Set("lease_expires_at", now.Add(time.Minute).Format(time.RFC3339Nano)).Set("fencing_token", candidate.fencingToken+1).Set("updated_at", now.Format(time.RFC3339Nano)).Where(query.And(subjectRowWriteAllowed("_integration_connector_provider_states"), query.And(query.Equal("id", candidate.id), query.Equal("status", candidate.status), query.Equal("fencing_token", candidate.fencingToken), query.Equal("due_at", candidate.dueAt), query.Equal("lease_expires_at", candidate.leaseExpiresAt)))).Build()
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
	update, args, err := query.NewUpdateBuilder(s.dialect, "_integration_connector_provider_states").Set("payload_json", string(result.State)).Set("status", "ready").Set("due_at", dueAt.UTC().Format(time.RFC3339Nano)).Set("last_error_code", "").Set("attempt_count", 0).Set("lease_owner", "").Set("lease_expires_at", "").Set("updated_at", now.Format(time.RFC3339Nano)).Where(query.And(subjectRowWriteAllowed("_integration_connector_provider_states"), query.And(query.Equal("id", candidate.id), query.Equal("lease_owner", s.workerID), query.Equal("fencing_token", candidate.fencingToken+1)))).Build()
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
	for _, event := range result.Events {
		payload, err := integrationWebhookEventPayload(event.Payload, candidate.connectorKey, candidate.connectionKey)
		if err != nil {
			return err
		}
		digest := sha256.Sum256([]byte(candidate.workspaceID + "\x00" + candidate.providerKey + "\x00" + strings.TrimSpace(event.ExternalID)))
		eventID := "event:" + hex.EncodeToString(digest[:])
		lookup, values, err := query.NewSelectBuilder(s.dialect, "_integration_events").Columns("id").Where(query.And(query.Equal("workspace_id", candidate.workspaceID), query.Equal("provider", candidate.providerKey), query.Equal("external_id", event.ExternalID))).Build()
		if err != nil {
			return err
		}
		var existing string
		lookupErr := tx.QueryRowContext(ctx, lookup, values...).Scan(&existing)
		if lookupErr == nil {
			continue
		}
		if lookupErr != sql.ErrNoRows {
			return lookupErr
		}
		insert, values, err := query.NewInsertBuilder(s.dialect, "_integration_events").Columns("id", "workspace_id", "provider", "event_type", "external_id", "status", "payload_json", "error", "attempt_count", "next_retry_at", "last_attempt_at", "lease_owner", "lease_expires_at", "fencing_token", "received_at", "updated_at").Values(eventID, candidate.workspaceID, candidate.providerKey, event.EventType, event.ExternalID, "received", string(payload), nil, 0, "", "", "", "", 0, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)).Build()
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, insert, values...); err != nil {
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
		insert, values, err := query.NewInsertBuilder(s.dialect, "_integration_connector_provider_commits").Columns("id", "workspace_id", "connector_key", "provider_key", "connection_key", "task_key", "operation_key", "contract_sha256", "payload_json", "status", "attempt_count", "due_at", "lease_owner", "lease_expires_at", "fencing_token", "created_at", "updated_at").Values(commitID, candidate.workspaceID, candidate.connectorKey, candidate.providerKey, candidate.connectionKey, candidate.taskKey, commit.OperationKey, commit.ContractSHA256, string(commit.Payload), "pending", 0, now.Format(time.RFC3339Nano), "", "", 0, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)).Build()
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, insert, values...); err != nil {
			return err
		}
	}
	for _, taskKey := range result.WakeTasks {
		wake, values, err := query.NewUpdateBuilder(s.dialect, "_integration_connector_provider_states").Set("due_at", now.Format(time.RFC3339Nano)).Set("updated_at", now.Format(time.RFC3339Nano)).Where(query.And(subjectRowWriteAllowed("_integration_connector_provider_states"), query.And(query.Equal("workspace_id", candidate.workspaceID), query.Equal("connection_key", candidate.connectionKey), query.Equal("task_key", taskKey)))).Build()
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, wake, values...); err != nil {
			return err
		}
	}
	return tx.Commit()
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
	status, dueAt := "failed", now.Add(integrationEventRetryDelay(attempt)).Format(time.RFC3339Nano)
	if attempt >= 5 {
		status, dueAt = "dead_letter", ""
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_connector_provider_states").Set("status", status).Set("due_at", dueAt).Set("last_error_code", "integration.provider_task_failed").Set("attempt_count", attempt).Set("lease_owner", "").Set("lease_expires_at", "").Set("updated_at", now.Format(time.RFC3339Nano)).Where(query.And(subjectRowWriteAllowed("_integration_connector_provider_states"), query.And(query.Equal("id", candidate.id), query.Equal("lease_owner", s.workerID), query.Equal("fencing_token", candidate.fencingToken+1)))).Build()
	if err == nil {
		_, err = s.database.ExecContext(ctx, statement, args...)
	}
	if err != nil {
		return fmt.Errorf("persist Integration Provider task failure after %v: %w", processErr, err)
	}
	return processErr
}

type providerCommitCandidate struct {
	id, workspaceID, connectorKey, providerKey, connectionKey, operationKey, payload, status string
	dueAt, leaseExpiresAt                                                                    string
	attemptCount                                                                             int
	fencingToken                                                                             int64
}

func (s *WorkerStore) processProviderCommits(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 500 {
		limit = 25
	}
	now := s.now()
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connector_provider_commits").Columns("id", "workspace_id", "connector_key", "provider_key", "connection_key", "operation_key", "payload_json", "status", "attempt_count", "fencing_token", "due_at", "lease_expires_at").Where(query.And(query.In("status", "pending", "failed"), query.LessThan("due_at", providerDeadlineScanEnd(now)), query.Or(query.Equal("lease_expires_at", ""), query.LessThan("lease_expires_at", providerDeadlineScanEnd(now))))).OrderBy(query.Ascending("due_at")).Limit(limit).Build()
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
		claim, values, err := query.NewUpdateBuilder(s.dialect, "_integration_connector_provider_commits").Set("status", "processing").Set("lease_owner", s.workerID).Set("lease_expires_at", now.Add(time.Minute).Format(time.RFC3339Nano)).Set("fencing_token", candidate.fencingToken+1).Set("updated_at", now.Format(time.RFC3339Nano)).Where(query.And(subjectRowWriteAllowed("_integration_connector_provider_commits"), query.And(query.Equal("id", candidate.id), query.Equal("status", candidate.status), query.Equal("fencing_token", candidate.fencingToken), query.Equal("due_at", candidate.dueAt), query.Equal("lease_expires_at", candidate.leaseExpiresAt)))).Build()
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
		status, dueAt, attempt := "succeeded", "", candidate.attemptCount
		if deliveryErr != nil {
			status, attempt, dueAt = "failed", candidate.attemptCount+1, now.Add(integrationEventRetryDelay(candidate.attemptCount+1)).Format(time.RFC3339Nano)
			if attempt >= 5 {
				status, dueAt = "dead_letter", ""
			}
			if firstErr == nil {
				firstErr = deliveryErr
			}
		}
		update, values, err := query.NewUpdateBuilder(s.dialect, "_integration_connector_provider_commits").Set("status", status).Set("attempt_count", attempt).Set("due_at", dueAt).Set("lease_owner", "").Set("lease_expires_at", "").Set("updated_at", now.Format(time.RFC3339Nano)).Where(query.And(subjectRowWriteAllowed("_integration_connector_provider_commits"), query.And(query.Equal("id", candidate.id), query.Equal("lease_owner", s.workerID), query.Equal("fencing_token", candidate.fencingToken+1)))).Build()
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
		claim, claimArgs, err := query.NewUpdateBuilder(s.dialect, "_integration_invocations").Set("status", "reconciling").Set("updated_at", now.Format(time.RFC3339Nano)).Where(query.And(subjectRowWriteAllowed("_integration_invocations"), query.And(query.Equal("workspace_id", invocation.WorkspaceID), query.Equal("id", invocation.ID), query.Equal("status", invocation.Status), query.Equal("updated_at", invocation.UpdatedAt)))).Build()
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
		query.Equal("status", "failed"), query.And(query.Equal("status", "reconciling"), query.LessThanOrEqual("updated_at", now.Add(-time.Minute).Format(time.RFC3339Nano))),
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
		if next, _ := value.Metadata["reconciliation_next_at"].(string); strings.TrimSpace(next) != "" && next > now.Format(time.RFC3339Nano) {
			continue
		}
		values = append(values, value)
		if len(values) == limit {
			break
		}
	}
	return values, rows.Err()
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
		metadata["reconciliation_next_at"] = now.Add(5 * time.Minute).Format(time.RFC3339Nano)
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
			metadata["reconciliation_next_at"] = now.Add(result.RetryAfter).Format(time.RFC3339Nano)
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
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_invocations").Set("status", status).Set("response_ref", responseRef).Set("error", errorText).Set("metadata_json", string(payload)).Set("updated_at", now.Format(time.RFC3339Nano)).Where(query.And(subjectRowWriteAllowed("_integration_invocations"), query.And(query.Equal("workspace_id", invocation.WorkspaceID), query.Equal("id", invocation.ID)))).Build()
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
	now := s.now().Format(time.RFC3339Nano)
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns("workspace_id", "secret_key").Where(query.And(query.Equal("status", "active"), query.NotEqual("expires_at", ""), query.LessThanOrEqual("expires_at", now))).OrderBy(query.Ascending("expires_at")).Limit(limit).Build()
	if err != nil {
		return 0, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return 0, err
	}
	type expired struct{ workspaceID, key string }
	values := []expired{}
	for rows.Next() {
		var value expired
		if err := rows.Scan(&value.workspaceID, &value.key); err != nil {
			_ = rows.Close()
			return 0, err
		}
		values = append(values, value)
	}
	_ = rows.Close()
	processed := 0
	for _, value := range values {
		update, args, err := query.NewUpdateBuilder(s.dialect, "_integration_secrets").Set("status", "expired").Set("updated_at", now).Where(query.And(subjectRowWriteAllowed("_integration_secrets"), query.And(query.Equal("workspace_id", value.workspaceID), query.Equal("secret_key", value.key), query.Equal("status", "active")))).Build()
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
