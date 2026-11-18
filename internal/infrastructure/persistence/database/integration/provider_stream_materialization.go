package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

type materializedProviderStream struct {
	id                                           int64
	connectorKey, providerKey, streamKey, status string
	stateVersion                                 int
}

func providerStreamProcessor(provider connector.Adapter) (connector.StreamProcessor, bool) {
	if capability, ok := provider.(connector.StreamCapabilityProvider); ok {
		return capability.StreamProcessor()
	}
	processor, ok := provider.(connector.StreamProcessor)
	return processor, ok
}

func materializeProviderStreams(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, providers modulehost.ProviderRegistry, subjectLifecycle *SubjectLifecyclePersistence, connection connector.Connection, now time.Time) error {
	desired := map[string]connector.StreamDescriptor{}
	provider, ok := providers.Provider(connection.ConnectorKey, connection.ProviderKey)
	if !ok {
		return fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
	}
	if processor, ok := providerStreamProcessor(provider); ok {
		for _, stream := range processor.Streams(connection) {
			if err := stream.Validate(); err != nil {
				return fmt.Errorf("validate Integration Provider stream: %w", err)
			}
			if _, duplicate := desired[stream.Key]; duplicate {
				return fmt.Errorf("Integration Provider stream %q is duplicated", stream.Key)
			}
			desired[stream.Key] = stream
		}
	}
	existing, err := listMaterializedProviderStreams(ctx, database, dialect, connection.WorkspaceID, connection.Key)
	if err != nil {
		return err
	}
	for key, value := range existing {
		if _, keep := desired[key]; keep {
			continue
		}
		if err := cancelMaterializedProviderStream(ctx, database, dialect, subjectLifecycle, connection.WorkspaceID, connection.Key, value.id, now); err != nil {
			return err
		}
	}
	keys := make([]string, 0, len(desired))
	for key := range desired {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := upsertMaterializedProviderStream(ctx, database, dialect, subjectLifecycle, connection, desired[key], existing[key], now); err != nil {
			return err
		}
	}
	return nil
}

func listMaterializedProviderStreams(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, workspaceID, connectionKey string) (map[string]materializedProviderStream, error) {
	statement, args, err := query.NewSelectBuilder(dialect, providerRunTable).
		Columns("id", "connector_key", "provider_key", "task_key", "state_version", "status").
		Where(query.And(query.Equal("run_kind", providerRunKindStream), query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey))).
		OrderBy(query.Ascending("task_key")).Build()
	if err != nil {
		return nil, err
	}
	rows, err := database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := map[string]materializedProviderStream{}
	for rows.Next() {
		var value materializedProviderStream
		if err := rows.Scan(&value.id, &value.connectorKey, &value.providerKey, &value.streamKey, &value.stateVersion, &value.status); err != nil {
			return nil, err
		}
		values[value.streamKey] = value
	}
	return values, rows.Err()
}

func upsertMaterializedProviderStream(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, subjectLifecycle *SubjectLifecyclePersistence, connection connector.Connection, stream connector.StreamDescriptor, current materializedProviderStream, now time.Time) error {
	nowMillis := now.UTC().UnixMilli()
	if current.id == 0 {
		statement, args, err := query.NewInsertBuilder(dialect, providerRunTable).
			Columns("workspace_id", "run_kind", "run_key", "connector_key", "provider_key", "connection_key", "task_key", "state_version", "operation_key", "contract_sha256", "payload_json", "progress_json", "status", "last_error_code", "attempt_count", "due_at", "lease_owner", "lease_expires_at", "fencing_token", "created_at", "updated_at").
			Values(connection.WorkspaceID, providerRunKindStream, stream.Key, connection.ConnectorKey, connection.ProviderKey, connection.Key, stream.Key, stream.StateVersion, "", "", `{}`, `{}`, "ready", "", 0, nowMillis, "", int64(0), 0, nowMillis, nowMillis).Build()
		if err != nil {
			return err
		}
		_, err = database.ExecContext(ctx, statement, args...)
		return err
	}
	changedContract := current.stateVersion != stream.StateVersion || current.connectorKey != connection.ConnectorKey || current.providerKey != connection.ProviderKey
	builder := query.NewUpdateBuilder(dialect, providerRunTable).
		Set("connector_key", connection.ConnectorKey).Set("provider_key", connection.ProviderKey).Set("state_version", stream.StateVersion).
		Set("status", "ready").Set("due_at", nowMillis).Set("last_error_code", "").Set("attempt_count", 0).
		Set("lease_owner", "").Set("lease_expires_at", int64(0)).SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).Set("updated_at", nowMillis)
	if changedContract {
		builder.Set("payload_json", `{}`).Set("progress_json", `{}`).Set("last_success_at", int64(0))
	}
	statement, args, err := builder.Where(query.And(subjectRowsWriteAllowed(subjectLifecycle, dialect, connection.WorkspaceID, providerRunTable, strconv.FormatInt(current.id, 10)), query.Equal("run_kind", providerRunKindStream), query.Equal("id", current.id))).Build()
	if err != nil {
		return err
	}
	result, err := database.ExecContext(ctx, statement, args...)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return fmt.Errorf("Integration Provider stream %d changed while materializing", current.id)
	}
	return nil
}

func cancelMaterializedProviderStream(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, subjectLifecycle *SubjectLifecyclePersistence, workspaceID, connectionKey string, id int64, now time.Time) error {
	statement, args, err := query.NewUpdateBuilder(dialect, providerRunTable).
		Set("status", "cancelled").Set("due_at", int64(0)).Set("lease_owner", "").Set("lease_expires_at", int64(0)).
		SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).Set("updated_at", now.UTC().UnixMilli()).
		Where(query.And(subjectRowsWriteAllowed(subjectLifecycle, dialect, workspaceID, providerRunTable, strconv.FormatInt(id, 10)), query.Equal("run_kind", providerRunKindStream), query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey), query.Equal("id", id), query.NotEqual("status", "cancelled"))).Build()
	if err != nil {
		return err
	}
	_, err = database.ExecContext(ctx, statement, args...)
	return err
}

// restartProviderStreamsReferencingSecret fences sessions in the same owner
// transaction as secret mutation, so no checkpoint using stale material can
// commit after the credential change becomes visible.
func restartProviderStreamsReferencingSecret(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, subjectLifecycle *SubjectLifecyclePersistence, workspaceID, secretKey string, now time.Time) ([]string, error) {
	statement, args, err := query.NewSelectBuilder(dialect, "_integration_connections").Columns("connection_key", "secret_refs_json").Where(query.Equal("workspace_id", strings.TrimSpace(workspaceID))).Build()
	if err != nil {
		return nil, err
	}
	rows, err := database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	var keys []string
	for rows.Next() {
		var connectionKey, refsJSON string
		if err := rows.Scan(&connectionKey, &refsJSON); err != nil {
			_ = rows.Close()
			return nil, err
		}
		refs := map[string]string{}
		if json.Unmarshal([]byte(refsJSON), &refs) != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("decode Integration connection secret references")
		}
		for _, ref := range refs {
			if ref == "secret:"+strings.TrimSpace(secretKey) {
				keys = append(keys, connectionKey)
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()
	sort.Strings(keys)
	for _, connectionKey := range keys {
		update, values, err := query.NewUpdateBuilder(dialect, providerRunTable).
			Set("status", "ready").Set("due_at", now.UTC().UnixMilli()).Set("last_error_code", "").Set("attempt_count", 0).
			Set("lease_owner", "").Set("lease_expires_at", int64(0)).SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).Set("updated_at", now.UTC().UnixMilli()).
			Where(query.And(subjectRowsWriteAllowed(subjectLifecycle, dialect, workspaceID, providerRunTable, ""), query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey), query.Equal("run_kind", providerRunKindStream), query.NotEqual("status", "cancelled"))).Build()
		if err != nil {
			return nil, err
		}
		if _, err := database.ExecContext(ctx, update, values...); err != nil && err != sql.ErrNoRows {
			return nil, err
		}
	}
	return keys, nil
}
