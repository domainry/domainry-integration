package integration

import (
	"context"
	"fmt"
	"sort"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

type materializedProviderTask struct {
	id, connectorKey, providerKey, taskKey, status string
	stateVersion                                   int
}

// materializeProviderTasks is called in the same transaction as a Connection
// change. The hot worker path therefore claims indexed due rows directly and
// never scans every active Connection before each batch.
func materializeProviderTasks(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, providers modulehost.ProviderRegistry, subjectLifecycle *SubjectLifecyclePersistence, connection connector.Connection, now time.Time) error {
	if connection.Status != "active" {
		return cancelProviderRunsForConnection(ctx, database, dialect, subjectLifecycle, connection.WorkspaceID, connection.Key, now)
	}
	desired := map[string]connector.BackgroundTaskDescriptor{}
	provider, ok := providers.Provider(connection.ConnectorKey, connection.ProviderKey)
	if !ok {
		return fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
	}
	if processor, ok := providerBackgroundProcessor(provider); ok {
		for _, task := range processor.BackgroundTasks(connection) {
			if err := task.Validate(); err != nil {
				return fmt.Errorf("validate Integration Provider background task: %w", err)
			}
			if _, duplicate := desired[task.Key]; duplicate {
				return fmt.Errorf("Integration Provider background task %q is duplicated", task.Key)
			}
			desired[task.Key] = task
		}
	}

	existing, err := listMaterializedProviderTasks(ctx, database, dialect, connection.WorkspaceID, connection.Key)
	if err != nil {
		return err
	}
	for key, value := range existing {
		if _, keep := desired[key]; keep {
			continue
		}
		if err := cancelMaterializedProviderTask(ctx, database, dialect, subjectLifecycle, connection.WorkspaceID, connection.Key, value.id, now); err != nil {
			return err
		}
	}

	keys := make([]string, 0, len(desired))
	for key := range desired {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := upsertMaterializedProviderTask(ctx, database, dialect, subjectLifecycle, connection, desired[key], existing[key], now); err != nil {
			return err
		}
	}
	return nil
}

func listMaterializedProviderTasks(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, workspaceID, connectionKey string) (map[string]materializedProviderTask, error) {
	statement, args, err := query.NewSelectBuilder(dialect, providerRunTable).
		Columns("id", "connector_key", "provider_key", "task_key", "state_version", "status").
		Where(query.And(query.Equal("run_kind", providerRunKindState), query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey))).
		OrderBy(query.Ascending("task_key")).Build()
	if err != nil {
		return nil, err
	}
	rows, err := database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := map[string]materializedProviderTask{}
	for rows.Next() {
		var value materializedProviderTask
		if err := rows.Scan(&value.id, &value.connectorKey, &value.providerKey, &value.taskKey, &value.stateVersion, &value.status); err != nil {
			return nil, err
		}
		values[value.taskKey] = value
	}
	return values, rows.Err()
}

func upsertMaterializedProviderTask(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, subjectLifecycle *SubjectLifecyclePersistence, connection connector.Connection, task connector.BackgroundTaskDescriptor, current materializedProviderTask, now time.Time) error {
	nowMillis := now.UTC().UnixMilli()
	if current.id == "" {
		id := ownerID("provider_task_", connection.WorkspaceID, connection.Key+"\x00"+task.Key)
		statement, args, err := query.NewInsertBuilder(dialect, providerRunTable).
			Columns("id", "workspace_id", "run_kind", "run_key", "connector_key", "provider_key", "connection_key", "task_key", "state_version", "operation_key", "contract_sha256", "payload_json", "status", "last_error_code", "attempt_count", "due_at", "lease_owner", "lease_expires_at", "fencing_token", "created_at", "updated_at").
			Values(id, connection.WorkspaceID, providerRunKindState, task.Key, connection.ConnectorKey, connection.ProviderKey, connection.Key, task.Key, task.StateVersion, "", "", `{}`, "ready", "", 0, nowMillis, "", int64(0), 0, nowMillis, nowMillis).Build()
		if err != nil {
			return err
		}
		_, err = database.ExecContext(ctx, statement, args...)
		return err
	}
	if current.stateVersion == task.StateVersion && current.connectorKey == connection.ConnectorKey && current.providerKey == connection.ProviderKey && current.status != "cancelled" {
		return nil
	}
	statement, args, err := query.NewUpdateBuilder(dialect, providerRunTable).
		Set("connector_key", connection.ConnectorKey).Set("provider_key", connection.ProviderKey).
		Set("state_version", task.StateVersion).Set("payload_json", `{}`).Set("status", "ready").Set("due_at", nowMillis).
		Set("last_error_code", "").Set("attempt_count", 0).Set("last_success_at", int64(0)).Set("lease_owner", "").Set("lease_expires_at", int64(0)).
		SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).Set("updated_at", nowMillis).
		Where(query.And(subjectRowsWriteAllowed(subjectLifecycle, dialect, connection.WorkspaceID, providerRunTable, current.id), query.Equal("run_kind", providerRunKindState), query.Equal("id", current.id))).Build()
	if err != nil {
		return err
	}
	result, err := database.ExecContext(ctx, statement, args...)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return fmt.Errorf("Integration Provider task %q changed while materializing", current.id)
	}
	return nil
}

func cancelProviderTasksForConnection(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, subjectLifecycle *SubjectLifecyclePersistence, workspaceID, connectionKey string, now time.Time) error {
	return cancelProviderRunsForConnection(ctx, database, dialect, subjectLifecycle, workspaceID, connectionKey, now)
}

func cancelProviderRunsForConnection(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, subjectLifecycle *SubjectLifecyclePersistence, workspaceID, connectionKey string, now time.Time) error {
	statement, args, err := query.NewUpdateBuilder(dialect, providerRunTable).
		Set("status", "cancelled").Set("due_at", int64(0)).Set("lease_owner", "").Set("lease_expires_at", int64(0)).
		SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).Set("updated_at", now.UTC().UnixMilli()).
		Where(query.And(
			subjectRowsWriteAllowed(subjectLifecycle, dialect, workspaceID, providerRunTable, ""),
			query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey),
			query.In("run_kind", providerRunKindState, providerRunKindCommit), query.NotIn("status", "cancelled", "succeeded"),
		)).Build()
	if err != nil {
		return err
	}
	if _, err = database.ExecContext(ctx, statement, args...); err != nil {
		return err
	}
	removeLease, args, err := query.NewDeleteBuilder(dialect, providerConnectionLeaseTable).
		Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey))).Build()
	if err != nil {
		return err
	}
	_, err = database.ExecContext(ctx, removeLease, args...)
	return err
}

func cancelMaterializedProviderTask(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, subjectLifecycle *SubjectLifecyclePersistence, workspaceID, connectionKey, id string, now time.Time) error {
	statement, args, err := query.NewUpdateBuilder(dialect, providerRunTable).
		Set("status", "cancelled").Set("due_at", int64(0)).Set("lease_owner", "").Set("lease_expires_at", int64(0)).
		SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).Set("updated_at", now.UTC().UnixMilli()).
		Where(query.And(subjectRowsWriteAllowed(subjectLifecycle, dialect, workspaceID, providerRunTable, id), query.Equal("run_kind", providerRunKindState), query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey), query.Equal("id", id), query.NotEqual("status", "cancelled"))).Build()
	if err != nil {
		return err
	}
	_, err = database.ExecContext(ctx, statement, args...)
	return err
}
