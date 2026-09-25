package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

func (s *ManagementStore) ListWebhookSubscriptions(ctx context.Context, workspaceID string) ([]integrationsdk.WebhookSubscription, error) {
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return nil, err
	}
	where, err := scopedWhere(ctx, workspaceID, "", "")
	if err != nil {
		return nil, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_webhook_subscriptions").Columns(
		"subscription_key", "workspace_id", "name", "connector_key", "connection_key", "event_types_json", "status", "description", "created_by", "created_at", "updated_at", "disabled_at",
	).Where(where).OrderBy(query.Ascending("subscription_key")).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list Integration webhook subscriptions: %w", err)
	}
	defer rows.Close()
	values := []integrationsdk.WebhookSubscription{}
	for rows.Next() {
		value, err := scanWebhookSubscription(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func scanWebhookSubscription(row rowScanner) (integrationsdk.WebhookSubscription, error) {
	var value integrationsdk.WebhookSubscription
	var name, description, createdBy sql.NullString
	var disabledAt sql.NullInt64
	var createdAt, updatedAt int64
	var eventTypesJSON string
	if err := row.Scan(&value.Key, &value.WorkspaceID, &name, &value.ConnectorKey, &value.ConnectionKey, &eventTypesJSON, &value.Status, &description, &createdBy, &createdAt, &updatedAt, &disabledAt); err != nil {
		return value, err
	}
	value.Name, value.Description, value.CreatedBy = name.String, description.String, createdBy.String
	value.CreatedAt, value.UpdatedAt, value.DisabledAt = timestampString(createdAt), timestampString(updatedAt), timestampString(disabledAt.Int64)
	if err := json.Unmarshal([]byte(eventTypesJSON), &value.EventTypes); err != nil {
		return value, fmt.Errorf("decode Integration webhook event types: %w", err)
	}
	return value, nil
}

func (s *ManagementStore) getWebhookSubscription(ctx context.Context, workspaceID, key string) (integrationsdk.WebhookSubscription, error) {
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("subscription_key", strings.TrimSpace(key)))
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_webhook_subscriptions").Columns(
		"subscription_key", "workspace_id", "name", "connector_key", "connection_key", "event_types_json", "status", "description", "created_by", "created_at", "updated_at", "disabled_at",
	).Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	value, err := scanWebhookSubscription(s.database.QueryRowContext(ctx, statement, args...))
	if err == sql.ErrNoRows {
		return value, fmt.Errorf("Integration webhook subscription %q was not found", key)
	}
	return value, err
}

func (s *ManagementStore) UpsertWebhookSubscription(ctx context.Context, workspaceID, key, actorID string, input integrationsdk.WebhookSubscriptionInput) (integrationsdk.WebhookSubscription, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.WebhookSubscription
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.UpsertWebhookSubscription(ctx, workspaceID, key, actorID, input)
			return operationErr
		})
		return value, err
	}
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	key, err = requiredOwnerValue("webhook subscription key", key)
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	input.ConnectorKey, err = requiredOwnerValue("webhook connector key", input.ConnectorKey)
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	input.ConnectionKey, err = requiredOwnerValue("webhook connection key", input.ConnectionKey)
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	if strings.TrimSpace(input.Status) == "" {
		input.Status = "active"
	}
	eventTypesJSON, err := ownerJSON(input.EventTypes, `[]`)
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	now := ownerNow()
	where, err := scopedWhere(ctx, workspaceID, "", "", query.Equal("subscription_key", key))
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	lookup, lookupArgs, err := query.NewSelectBuilder(s.dialect, "_integration_webhook_subscriptions").Columns("id").Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	var id string
	lookupErr := s.database.QueryRowContext(ctx, lookup, lookupArgs...).Scan(&id)
	if lookupErr != nil && lookupErr != sql.ErrNoRows {
		return integrationsdk.WebhookSubscription{}, lookupErr
	}
	if lookupErr == sql.ErrNoRows {
		actorID, _ = scopeOwner(ctx, actorID)
		statement, args, buildErr := query.NewInsertBuilder(s.dialect, "_integration_webhook_subscriptions").Columns(
			"id", "subscription_key", "workspace_id", "name", "connector_key", "connection_key", "event_types_json", "status", "description", "created_by", "created_at", "updated_at", "disabled_at",
		).Values(ownerID("webhook_subscription_", workspaceID, key), key, workspaceID, input.Name, input.ConnectorKey, input.ConnectionKey, eventTypesJSON, input.Status, input.Description, actorID, timestampMillis(now), timestampMillis(now), int64(0)).Build()
		if buildErr != nil {
			return integrationsdk.WebhookSubscription{}, buildErr
		}
		if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationsdk.WebhookSubscription{}, fmt.Errorf("insert Integration webhook subscription: %w", err)
		}
	} else {
		statement, args, buildErr := query.NewUpdateBuilder(s.dialect, "_integration_webhook_subscriptions").Set("name", input.Name).Set("connector_key", input.ConnectorKey).Set("connection_key", input.ConnectionKey).Set("event_types_json", eventTypesJSON).Set("status", input.Status).Set("description", input.Description).Set("disabled_at", int64(0)).Set("updated_at", timestampMillis(now)).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, workspaceID, "_integration_webhook_subscriptions", id), where)).Build()
		if buildErr != nil {
			return integrationsdk.WebhookSubscription{}, buildErr
		}
		if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationsdk.WebhookSubscription{}, fmt.Errorf("update Integration webhook subscription: %w", err)
		}
	}
	return s.getWebhookSubscription(ctx, workspaceID, key)
}

func (s *ManagementStore) DeleteWebhookSubscription(ctx context.Context, workspaceID, key string) error {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return err
	}
	if s.transactions != nil {
		return s.withTransaction(ctx, func(store *ManagementStore) error {
			return store.DeleteWebhookSubscription(ctx, workspaceID, key)
		})
	}
	if err := s.requireScopedCandidate(ctx, "_integration_webhook_subscriptions", "subscription_key", workspaceID, key); err != nil {
		return err
	}
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("subscription_key", strings.TrimSpace(key)))
	if err != nil {
		return err
	}
	statement, args, err := query.NewDeleteBuilder(s.dialect, "_integration_webhook_subscriptions").Where(where).Build()
	if err != nil {
		return err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return fmt.Errorf("delete Integration webhook subscription: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return fmt.Errorf("Integration webhook subscription %q was not found", key)
	}
	return nil
}

func (s *ManagementStore) DisableWebhookSubscription(ctx context.Context, workspaceID, key, _ string) (integrationsdk.WebhookSubscription, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.WebhookSubscription
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.DisableWebhookSubscription(ctx, workspaceID, key, "")
			return operationErr
		})
		return value, err
	}
	if _, err := s.getWebhookSubscription(ctx, workspaceID, key); err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	now := ownerNow()
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("subscription_key", strings.TrimSpace(key)))
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_webhook_subscriptions").Set("status", "disabled").Set("disabled_at", timestampMillis(now)).Set("updated_at", timestampMillis(now)).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, strings.TrimSpace(workspaceID), "_integration_webhook_subscriptions", ownerID("webhook_subscription_", workspaceID, key)), where)).Build()
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return integrationsdk.WebhookSubscription{}, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return integrationsdk.WebhookSubscription{}, fmt.Errorf("Integration webhook subscription %q was not found", key)
	}
	return s.getWebhookSubscription(ctx, workspaceID, key)
}
