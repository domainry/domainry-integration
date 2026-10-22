package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-foundation/entityid"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

// insertProviderEvent is shared by scheduled Provider tasks and routed
// webhooks. Routed events include the target Connection in their durable
// identity because one provider delivery may legitimately target several
// independently-authorized accounts in the same Workspace.
func insertProviderEvent(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, subjectLifecycle *SubjectLifecyclePersistence, workspaceID, connectorKey, providerKey, connectionKey, ownerUserID string, event connector.BackgroundEvent, routed bool, now time.Time) (bool, error) {
	if err := event.Validate(); err != nil {
		return false, err
	}
	externalID := strings.TrimSpace(event.ExternalID)
	if routed {
		externalID += ":connection:" + strings.TrimSpace(connectionKey)
	}
	payload, err := integrationWebhookEventPayload(event.Payload, connectorKey, connectionKey, ownerUserID)
	if err != nil {
		return false, err
	}
	eventID, err := entityid.Derive("intevt", "integration.provider-event", workspaceID, providerKey, externalID)
	if err != nil {
		return false, err
	}
	if err = guardSubjectWrite(ctx, database, dialect, subjectLifecycle, workspaceID, subjectFenceReference{"row", "_integration_events", eventID}, subjectFenceReference{"connection", "", connectionKey}); err != nil {
		return false, err
	}
	lookup, values, err := query.NewSelectBuilder(dialect, "_integration_events").Columns("event_id").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("provider", providerKey), query.Equal("external_id", externalID))).Build()
	if err != nil {
		return false, err
	}
	var existing string
	lookupErr := database.QueryRowContext(ctx, lookup, values...).Scan(&existing)
	if lookupErr == nil {
		return false, nil
	}
	if lookupErr != sql.ErrNoRows {
		return false, lookupErr
	}
	insert, values, err := query.NewInsertBuilder(dialect, "_integration_events").Columns("event_id", "workspace_id", "provider", "event_type", "external_id", "status", "payload_json", "mapping_key", "target_type", "execution_json", "error", "attempt_count", "next_retry_at", "last_attempt_at", "lease_owner", "lease_expires_at", "fencing_token", "received_at", "updated_at").Values(eventID, workspaceID, providerKey, event.EventType, externalID, "received", string(payload), "", "", "{}", nil, 0, int64(0), int64(0), "", int64(0), 0, now.UnixMilli(), now.UnixMilli()).Build()
	if err != nil {
		return false, err
	}
	if _, err = database.ExecContext(ctx, insert, values...); err != nil {
		return false, fmt.Errorf("accept Integration Provider event: %w", err)
	}
	return true, nil
}

func connectionOwnerUserID(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, workspaceID, connectionKey string) (string, error) {
	statement, values, err := query.NewSelectBuilder(dialect, "_integration_connection_accounts").Columns("owner_user_id").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey))).Limit(1).Build()
	if err != nil {
		return "", err
	}
	var ownerUserID string
	err = database.QueryRowContext(ctx, statement, values...).Scan(&ownerUserID)
	if err == nil && strings.TrimSpace(ownerUserID) != "" {
		return strings.TrimSpace(ownerUserID), nil
	}
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	statement, values, err = query.NewSelectBuilder(dialect, "_integration_connections").Columns("created_by").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey))).Limit(1).Build()
	if err != nil {
		return "", err
	}
	err = database.QueryRowContext(ctx, statement, values...).Scan(&ownerUserID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(ownerUserID), nil
}
