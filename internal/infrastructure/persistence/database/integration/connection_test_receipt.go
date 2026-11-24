package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-foundation/requestcontext"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

const connectionTestOperation = "test_connection"

func (s *ManagementStore) recordConnectionTest(ctx context.Context, connection integrationsdk.Connection, receipt integrationsdk.DeliveryReceipt, started time.Time, probeErr error) error {
	completed := time.Now().UTC()
	// Keep the normal request deadline for the provider call. Its outcome can
	// still be recorded if the client disconnects before reading the response.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, connection.WorkspaceID,
		subjectFenceReference{"connection", "", connection.Key}, subjectFenceReference{"row", "_integration_invocations", receipt.MessageID}); err != nil {
		return err
	}
	metadata, err := json.Marshal(map[string]string{"connection_updated_at": connection.UpdatedAt})
	if err != nil {
		return err
	}
	code := ""
	if receipt.Status != integrationsdk.DeliveryStatusSucceeded {
		code = "integration.connection_test_failed"
		if providerCode, ok := connector.ProviderErrorCodeOf(probeErr); ok {
			code = providerCode
		}
	}
	stmt, args, err := query.NewInsertBuilder(s.dialect, "_integration_invocations").Columns(
		"invocation_id", "workspace_id", "connector_key", "provider_key", "connection_key", "operation", "status", "duration_ms", "request_ref", "error", "metadata_json", "created_at", "updated_at",
	).Values(
		receipt.MessageID, connection.WorkspaceID, connection.ConnectorKey, connection.ProviderKey, connection.Key, connectionTestOperation, string(receipt.Status),
		completed.Sub(started).Milliseconds(), requestcontext.RequestID(ctx), code, string(metadata), started.UnixMilli(), completed.UnixMilli(),
	).Build()
	if err != nil {
		return err
	}
	if _, err := s.database.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("record Integration connection test receipt: %w", err)
	}
	return nil
}
