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
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

func (s *OperationsStore) AcceptWebhook(ctx context.Context, request integrationmodel.WebhookRequest) (integrationmodel.WebhookReceipt, error) {
	connection, err := s.delivery.connection(ctx, integrationmodel.DeliveryRequest{WorkspaceID: request.WorkspaceID, ConnectorKey: request.ConnectorKey, ConnectionKey: request.ConnectionKey})
	if err != nil {
		return integrationmodel.WebhookReceipt{}, err
	}
	provider, ok := s.delivery.providers.Provider(connection.ConnectorKey, connection.ProviderKey)
	if !ok {
		return integrationmodel.WebhookReceipt{}, fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
	}
	verifier, ok := provider.(connector.WebhookVerifier)
	if !ok {
		return integrationmodel.WebhookReceipt{}, fmt.Errorf("Integration provider %s/%s does not support webhooks", connection.ConnectorKey, connection.ProviderKey)
	}
	secrets, err := s.delivery.secrets.ResolveSecretReferences(ctx, request.WorkspaceID, connection.SecretRefs)
	if err != nil {
		return integrationmodel.WebhookReceipt{}, err
	}
	verified, err := verifier.VerifyWebhook(ctx, connector.VerifyWebhookRequest{
		ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey,
		Connection: connector.Connection{Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs},
		Headers:    request.Headers, Query: request.Query, Secrets: secrets, Body: append([]byte(nil), request.Body...), ReceivedAt: request.ReceivedAt,
	})
	if err != nil {
		return integrationmodel.WebhookReceipt{}, fmt.Errorf("verify Integration webhook: %w", err)
	}
	if verified.Security != nil && !verified.Security.SignatureVerified {
		return integrationmodel.WebhookReceipt{}, fmt.Errorf("Integration webhook signature is not verified")
	}
	if verified.Challenge != "" && strings.TrimSpace(verified.EventType) == "" {
		return integrationmodel.WebhookReceipt{Challenge: verified.Challenge, Format: verified.ChallengeFormat}, nil
	}
	if strings.TrimSpace(verified.EventType) == "" || strings.TrimSpace(verified.ExternalID) == "" || !json.Valid(verified.Payload) {
		return integrationmodel.WebhookReceipt{}, fmt.Errorf("Integration verified webhook event is incomplete")
	}
	verified.Payload, err = integrationWebhookEventPayload(verified.Payload, request.ConnectorKey, request.ConnectionKey)
	if err != nil {
		return integrationmodel.WebhookReceipt{}, err
	}
	if err = s.guardInboundSubject(ctx, request, connection.ProviderKey, verified); err != nil {
		return integrationmodel.WebhookReceipt{}, err
	}
	verified.Payload, err = verifiedInboundPayload(verified.Payload, verified.ExternalIdentity)
	if err != nil {
		return integrationmodel.WebhookReceipt{}, err
	}
	if verified.Security != nil && strings.TrimSpace(verified.Security.Nonce) != "" {
		if err := s.acceptWebhookNonce(ctx, request, verified.Security.Nonce); err != nil {
			return integrationmodel.WebhookReceipt{}, err
		}
	}
	event, inserted, err := s.acceptEvent(ctx, request, connection.ProviderKey, verified)
	if err != nil {
		return integrationmodel.WebhookReceipt{}, err
	}
	if inserted {
		event, err = s.processEvent(ctx, event, verified.ExternalIdentity)
		if err != nil {
			return integrationmodel.WebhookReceipt{Event: event, Challenge: verified.Challenge, Format: verified.ChallengeFormat}, err
		}
	}
	return integrationmodel.WebhookReceipt{Event: event, Challenge: verified.Challenge, Format: verified.ChallengeFormat}, nil
}

func (s *OperationsStore) acceptWebhookNonce(ctx context.Context, request integrationmodel.WebhookRequest, nonce string) error {
	digest := sha256.Sum256([]byte(request.WorkspaceID + "\x00" + request.ConnectorKey + "\x00" + strings.TrimSpace(nonce)))
	now := request.ReceivedAt.UTC()
	statement, args, err := query.NewInsertBuilder(s.dialect, "_integration_webhook_nonces").Columns("id", "workspace_id", "connector_key", "nonce", "request_timestamp", "created_at", "expires_at").Values("nonce:"+hex.EncodeToString(digest[:]), request.WorkspaceID, request.ConnectorKey, strings.TrimSpace(nonce), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), now.Add(15*time.Minute).Format(time.RFC3339Nano)).Build()
	if err != nil {
		return err
	}
	if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
		return fmt.Errorf("Integration webhook nonce was already used: %w", err)
	}
	return nil
}

func (s *OperationsStore) acceptEvent(ctx context.Context, request integrationmodel.WebhookRequest, providerKey string, verified connector.VerifiedWebhook) (integrationmodel.Event, bool, error) {
	digest := sha256.Sum256([]byte(request.WorkspaceID + "\x00" + providerKey + "\x00" + strings.TrimSpace(verified.ExternalID)))
	id := "event:" + hex.EncodeToString(digest[:])
	if err := guardSubjectWrite(ctx, s.database, s.dialect, request.WorkspaceID, subjectFenceReference{"row", "_integration_events", id}, subjectFenceReference{"connection", "", request.ConnectionKey}); err != nil {
		return integrationmodel.Event{}, false, err
	}
	if existing, err := s.GetEvent(ctx, request.WorkspaceID, id); err == nil {
		existing.ConnectorKey, existing.ConnectionKey = request.ConnectorKey, request.ConnectionKey
		return existing, false, nil
	}
	now := request.ReceivedAt.UTC().Format(time.RFC3339Nano)
	statement, args, err := query.NewInsertBuilder(s.dialect, "_integration_events").Columns("id", "workspace_id", "provider", "event_type", "external_id", "status", "payload_json", "error", "attempt_count", "next_retry_at", "last_attempt_at", "lease_owner", "lease_expires_at", "fencing_token", "received_at", "updated_at").Values(id, request.WorkspaceID, providerKey, verified.EventType, verified.ExternalID, "received", string(verified.Payload), nil, 0, "", "", "", "", 0, now, now).Build()
	if err != nil {
		return integrationmodel.Event{}, false, err
	}
	if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
		if existing, readErr := s.GetEvent(ctx, request.WorkspaceID, id); readErr == nil {
			return existing, false, nil
		}
		return integrationmodel.Event{}, false, fmt.Errorf("accept Integration event: %w", err)
	}
	event, err := s.GetEvent(ctx, request.WorkspaceID, id)
	event.ConnectorKey, event.ConnectionKey = request.ConnectorKey, request.ConnectionKey
	return event, true, err
}

func (s *OperationsStore) ListEvents(ctx context.Context, filter integrationmodel.EventQuery) ([]integrationmodel.Event, error) {
	if strings.TrimSpace(filter.WorkspaceID) == "" {
		return nil, fmt.Errorf("Integration event workspace is required")
	}
	where, err := scopedWhere(ctx, strings.TrimSpace(filter.WorkspaceID), "", "")
	if err != nil {
		return nil, err
	}
	predicates := []query.Predicate{where}
	for column, value := range map[string]string{"provider": filter.Provider, "event_type": filter.EventType, "status": filter.Status} {
		if value = strings.TrimSpace(value); value != "" {
			predicates = append(predicates, query.Equal(column, value))
		}
	}
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_events").Columns(eventColumns()...).Where(query.And(predicates...)).OrderBy(query.Descending("received_at")).Limit(limit).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list Integration events: %w", err)
	}
	defer rows.Close()
	values := []integrationmodel.Event{}
	for rows.Next() {
		value, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		value.Execution = s.eventExecution(ctx, value.WorkspaceID, value.ID)
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *OperationsStore) GetEvent(ctx context.Context, workspaceID, id string) (integrationmodel.Event, error) {
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("id", strings.TrimSpace(id)))
	if err != nil {
		return integrationmodel.Event{}, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_events").Columns(eventColumns()...).Where(where).Build()
	if err != nil {
		return integrationmodel.Event{}, err
	}
	value, err := scanEvent(s.database.QueryRowContext(ctx, statement, args...))
	if err == sql.ErrNoRows {
		return integrationmodel.Event{}, fmt.Errorf("Integration event %q was not found", id)
	}
	if err == nil {
		value.Execution = s.eventExecution(ctx, value.WorkspaceID, value.ID)
	}
	return value, err
}

func (s *OperationsStore) ReplayEvent(ctx context.Context, workspaceID, id string) (integrationmodel.Event, error) {
	workspaceID, id = strings.TrimSpace(workspaceID), strings.TrimSpace(id)
	tx, err := s.transactions.BeginTx(ctx, nil)
	if err != nil {
		return integrationmodel.Event{}, err
	}
	defer func() { _ = tx.Rollback() }()
	where, err := scopedWhere(ctx, workspaceID, "", "", query.Equal("id", id))
	if err != nil {
		return integrationmodel.Event{}, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_events").Columns(eventColumns()...).Where(where).Limit(1).Build()
	if err != nil {
		return integrationmodel.Event{}, err
	}
	event, err := scanEvent(tx.QueryRowContext(ctx, statement, args...))
	if err == sql.ErrNoRows {
		return event, fmt.Errorf("Integration event %q was not found", id)
	}
	if err != nil {
		return event, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	statement, args, err = query.NewUpdateBuilder(s.dialect, "_integration_events").Set("status", "received").Set("error", "").Set("next_retry_at", "").Set("updated_at", now).Where(query.And(subjectRowWriteAllowed("_integration_events"), where)).Build()
	if err != nil {
		return event, err
	}
	result, err := tx.ExecContext(ctx, statement, args...)
	if err != nil {
		return event, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return event, fmt.Errorf("Integration event %q changed during replay", id)
	}
	if err := tx.Commit(); err != nil {
		return event, err
	}
	event.Status, event.Error, event.NextRetryAt, event.UpdatedAt = "received", "", "", now
	return s.processEvent(ctx, event, nil)
}

func eventColumns() []string {
	return []string{"id", "workspace_id", "provider", "event_type", "external_id", "status", "payload_json", "error", "attempt_count", "next_retry_at", "last_attempt_at", "received_at", "updated_at"}
}

func scanEvent(row rowScanner) (integrationmodel.Event, error) {
	var value integrationmodel.Event
	var errorText sql.NullString
	var payload string
	err := row.Scan(&value.ID, &value.WorkspaceID, &value.Provider, &value.EventType, &value.ExternalID, &value.Status, &payload, &errorText, &value.AttemptCount, &value.NextRetryAt, &value.LastAttemptAt, &value.ReceivedAt, &value.UpdatedAt)
	if err != nil {
		return value, err
	}
	value.Payload, value.Error = json.RawMessage(payload), errorText.String
	return value, nil
}

func (s *OperationsStore) processEvent(ctx context.Context, event integrationmodel.Event, external *connector.WebhookExternalIdentity) (integrationmodel.Event, error) {
	mapping, found, err := s.eventMapping(ctx, event)
	if err != nil {
		return event, err
	}
	if !found {
		return s.updateEventStatus(ctx, event, "ignored", "")
	}
	if s.triggers == nil {
		event, _ = s.updateEventStatus(ctx, event, "failed", "runtime_trigger_unavailable")
		return event, fmt.Errorf("Integration Runtime TriggerSink is unavailable")
	}
	payload := map[string]any{}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return event, err
	}
	if err := validateIntegrationEventPayload(mapping, payload); err != nil {
		event, _ = s.updateEventStatus(ctx, event, "failed", err.Error())
		return event, err
	}
	inputPaths := mapping.ActionInput
	if mapping.TargetType == "workflow" {
		inputPaths = mapping.WorkflowInput
	} else if mapping.TargetType == "agent_task" {
		inputPaths = mapping.AgentInput
	}
	input := map[string]any{}
	for key, path := range inputPaths {
		if value, ok := integrationPayloadPath(payload, path); ok {
			input[key] = value
		}
	}
	for key, value := range mapping.Payload {
		input[key] = value
	}
	if mapping.TargetType == "workflow" {
		input["integration_event_id"], input["integration_provider"] = event.ID, event.Provider
		input["integration_event_type"], input["integration_external_id"] = event.EventType, event.ExternalID
		input["integration_mapping_key"] = mapping.Key
	}
	objectKey := strings.TrimSpace(mapping.ObjectKey)
	if objectKey == "" && strings.TrimSpace(mapping.ObjectKeyPath) != "" {
		if value, ok := integrationPayloadPath(payload, mapping.ObjectKeyPath); ok {
			objectKey = strings.TrimSpace(fmt.Sprint(value))
		}
	}
	recordID := strings.TrimSpace(mapping.RecordID)
	if recordID == "" && strings.TrimSpace(mapping.RecordIDPath) != "" {
		if value, ok := integrationPayloadPath(payload, mapping.RecordIDPath); ok {
			recordID = strings.TrimSpace(fmt.Sprint(value))
		}
	}
	actionKey := strings.TrimSpace(mapping.ActionKey)
	if actionKey == "" && strings.TrimSpace(mapping.ActionKeyPath) != "" {
		if value, ok := integrationPayloadPath(payload, mapping.ActionKeyPath); ok {
			actionKey = strings.TrimSpace(fmt.Sprint(value))
		}
	}
	relatedTaskID := strings.TrimSpace(mapping.RelatedTaskID)
	if relatedTaskID == "" && strings.TrimSpace(mapping.RelatedTaskIDPath) != "" {
		if value, ok := integrationPayloadPath(payload, mapping.RelatedTaskIDPath); ok {
			relatedTaskID = strings.TrimSpace(fmt.Sprint(value))
		}
	}
	principal := integrationsdk.TriggerPrincipal{}
	identityProvider, identitySubject, identityName := strings.TrimSpace(mapping.ExternalIdentity.Provider), "", ""
	if identityProvider == "" {
		identityProvider = event.Provider
	}
	if external != nil {
		identitySubject, identityName = strings.TrimSpace(external.Subject), strings.TrimSpace(external.Name)
	}
	if identitySubject == "" && strings.TrimSpace(mapping.ExternalIdentity.SubjectPath) != "" {
		if value, ok := integrationPayloadPath(payload, mapping.ExternalIdentity.SubjectPath); ok {
			identitySubject = strings.TrimSpace(fmt.Sprint(value))
		}
	}
	if identityName == "" && strings.TrimSpace(mapping.ExternalIdentity.NamePath) != "" {
		if value, ok := integrationPayloadPath(payload, mapping.ExternalIdentity.NamePath); ok {
			identityName = strings.TrimSpace(fmt.Sprint(value))
		}
	}
	if identitySubject != "" {
		principal.ExternalName = identityName
		if identity, ok := s.resolveExternalIdentity(ctx, event.WorkspaceID, identityProvider, identitySubject); ok {
			principal.ActorID, principal.RoleKey = identity.ActorID, identity.RoleKey
		} else if strings.TrimSpace(mapping.ExternalIdentity.SubjectPath) != "" || mapping.ExternalIdentity.OnUnmapped != "" {
			err := fmt.Errorf("Integration external identity %s/%s is unmapped", identityProvider, identitySubject)
			event, _ = s.updateEventStatus(ctx, event, "failed", err.Error())
			return event, err
		}
	}
	if err := guardSubjectWrite(ctx, s.database, s.dialect, event.WorkspaceID, subjectFenceReference{"row", "_integration_events", event.ID}, subjectFenceReference{"subject", "", principal.ActorID}); err != nil {
		return event, err
	}
	receipt, triggerErr := s.triggers.Trigger(ctx, integrationsdk.TriggerRequest{
		EventID: event.ID, WorkspaceID: event.WorkspaceID, MappingKey: mapping.Key, MappingRevision: integrationEventMappingRevision(mapping), IdempotencyKey: event.ID + ":" + mapping.Key,
		Source: integrationsdk.TriggerSource{Provider: event.Provider, EventType: event.EventType, ExternalID: event.ExternalID, ReceivedAt: event.ReceivedAt},
		Target: integrationsdk.TriggerTarget{
			Type: mapping.TargetType, WorkflowKey: mapping.WorkflowKey, ObjectKey: objectKey, RecordID: recordID, ActionKey: actionKey,
			AgentID: mapping.AgentID, ConversationID: mapping.ConversationID, AgentTaskMode: mapping.AgentTaskMode, RelatedTaskID: relatedTaskID, Input: input,
		}, Principal: principal,
	})
	if triggerErr != nil {
		if strings.TrimSpace(receipt.EventID) == "" {
			receipt.EventID = event.ID
		}
		if strings.TrimSpace(receipt.MappingKey) == "" {
			receipt.MappingKey = mapping.Key
		}
		if strings.TrimSpace(receipt.TargetType) == "" {
			receipt.TargetType = mapping.TargetType
		}
		if strings.TrimSpace(receipt.Status) == "" {
			receipt.Status = "failed"
		}
		if strings.TrimSpace(receipt.ErrorCode) == "" {
			receipt.ErrorCode = "runtime_trigger_failed"
		}
		if err := s.persistExecutionReceipt(ctx, event, mapping, receipt); err != nil {
			return event, err
		}
		event, _ = s.updateEventStatus(ctx, event, "failed", triggerErr.Error())
		return event, triggerErr
	}
	if err := s.persistExecutionReceipt(ctx, event, mapping, receipt); err != nil {
		return event, err
	}
	event, err = s.updateEventStatus(ctx, event, "processed", "")
	if err == nil {
		event.Execution = &integrationmodel.RuntimeExecutionReceipt{EventID: receipt.EventID, MappingKey: receipt.MappingKey, ExecutionID: receipt.ExecutionID, TargetType: receipt.TargetType, Status: receipt.Status, ErrorCode: receipt.ErrorCode, CompletedAt: receipt.CompletedAt}
	}
	return event, err
}

func integrationEventMappingRevision(mapping integrationmodel.EventMappingRequirement) string {
	raw, _ := json.Marshal(mapping)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func (s *OperationsStore) eventMapping(ctx context.Context, event integrationmodel.Event) (integrationmodel.EventMappingRequirement, bool, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_event_mapping_definitions").Columns("payload_json").Where(query.And(query.Equal("object_key", event.WorkspaceID), query.IsNull("disabled_at"))).OrderBy(query.Ascending("resource_key")).Build()
	if err != nil {
		return integrationmodel.EventMappingRequirement{}, false, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return integrationmodel.EventMappingRequirement{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return integrationmodel.EventMappingRequirement{}, false, err
		}
		var mapping integrationmodel.EventMappingRequirement
		if err := json.Unmarshal([]byte(payload), &mapping); err != nil {
			return mapping, false, err
		}
		if !eventMappingMatches(mapping, event) {
			continue
		}
		{
			return mapping, true, nil
		}
	}
	return integrationmodel.EventMappingRequirement{}, false, rows.Err()
}

func (s *OperationsStore) updateEventStatus(ctx context.Context, event integrationmodel.Event, status, errorText string) (integrationmodel.Event, error) {
	now := time.Now().UTC()
	builder := query.NewUpdateBuilder(s.dialect, "_integration_events").Set("status", status).Set("error", errorText).Set("last_attempt_at", now.Format(time.RFC3339Nano)).Set("lease_owner", "").Set("lease_expires_at", "").Set("updated_at", now.Format(time.RFC3339Nano))
	if status == "failed" {
		attempt := event.AttemptCount + 1
		if attempt >= 5 {
			builder = builder.Set("status", "dead_letter").Set("attempt_count", attempt).Set("next_retry_at", "")
		} else {
			builder = builder.Set("attempt_count", attempt).Set("next_retry_at", now.Add(integrationEventRetryDelay(attempt)).Format(time.RFC3339Nano))
		}
	} else {
		builder = builder.Set("next_retry_at", "")
	}
	where, err := scopedWhere(ctx, event.WorkspaceID, "", "", query.Equal("id", event.ID))
	if err != nil {
		return event, err
	}
	statement, args, err := builder.Where(query.And(subjectRowWriteAllowed("_integration_events"), where)).Build()
	if err != nil {
		return event, err
	}
	if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
		return event, err
	}
	return s.GetEvent(ctx, event.WorkspaceID, event.ID)
}

func integrationEventRetryDelay(attempt int) time.Duration {
	switch attempt {
	case 0, 1:
		return time.Minute
	case 2:
		return 5 * time.Minute
	case 3:
		return 15 * time.Minute
	default:
		return time.Hour
	}
}

func (s *OperationsStore) persistExecutionReceipt(ctx context.Context, event integrationmodel.Event, mapping integrationmodel.EventMappingRequirement, receipt integrationsdk.RuntimeExecutionReceipt) error {
	payload, _ := json.Marshal(receipt)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	id := "intent:" + strings.TrimPrefix(event.ID, "event:")
	lookup, args, err := query.NewSelectBuilder(s.dialect, "_integration_event_mapping_intents").Columns("id").Where(query.And(query.Equal("workspace_id", event.WorkspaceID), query.Equal("event_id", event.ID))).Build()
	if err != nil {
		return err
	}
	var current string
	err = s.database.QueryRowContext(ctx, lookup, args...).Scan(&current)
	if err == sql.ErrNoRows {
		statement, values, buildErr := query.NewInsertBuilder(s.dialect, "_integration_event_mapping_intents").Columns("id", "workspace_id", "event_id", "mapping_key", "target_type", "status", "payload_json", "created_at", "updated_at").Values(id, event.WorkspaceID, event.ID, mapping.Key, mapping.TargetType, receipt.Status, string(payload), now, now).Build()
		if buildErr != nil {
			return buildErr
		}
		_, err = s.database.ExecContext(ctx, statement, values...)
		return err
	}
	if err != nil {
		return err
	}
	statement, values, err := query.NewUpdateBuilder(s.dialect, "_integration_event_mapping_intents").Set("mapping_key", mapping.Key).Set("target_type", mapping.TargetType).Set("status", receipt.Status).Set("payload_json", string(payload)).Set("updated_at", now).Where(query.And(subjectRowWriteAllowed("_integration_event_mapping_intents"), query.Equal("id", current))).Build()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, statement, values...)
	return err
}

func (s *OperationsStore) eventExecution(ctx context.Context, workspaceID, eventID string) *integrationmodel.RuntimeExecutionReceipt {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_event_mapping_intents").Columns("payload_json").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("event_id", eventID))).Build()
	if err != nil {
		return nil
	}
	var payload string
	if s.database.QueryRowContext(ctx, statement, args...).Scan(&payload) != nil {
		return nil
	}
	var receipt integrationmodel.RuntimeExecutionReceipt
	if json.Unmarshal([]byte(payload), &receipt) != nil {
		return nil
	}
	return &receipt
}

func (s *OperationsStore) resolveExternalIdentity(ctx context.Context, workspaceID, provider, subject string) (integrationsdk.ExternalIdentity, bool) {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_external_identities").Columns("identity_key", "workspace_id", "provider", "external_subject", "actor_id", "role_key", "status").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("provider", provider), query.Equal("external_subject", subject), query.Equal("status", "active"))).Build()
	if err != nil {
		return integrationsdk.ExternalIdentity{}, false
	}
	var value integrationsdk.ExternalIdentity
	if s.database.QueryRowContext(ctx, statement, args...).Scan(&value.Key, &value.WorkspaceID, &value.Provider, &value.ExternalSubject, &value.ActorID, &value.RoleKey, &value.Status) != nil {
		return integrationsdk.ExternalIdentity{}, false
	}
	return value, true
}

func integrationPayloadPath(payload map[string]any, path string) (any, bool) {
	var current any = payload
	for _, segment := range strings.Split(strings.TrimSpace(path), ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func integrationWebhookEventPayload(raw json.RawMessage, connectorKey, connectionKey string) (json.RawMessage, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("Integration webhook event payload must be a JSON object: %w", err)
	}
	contextValue := map[string]any{}
	contextValue["connector_key"], contextValue["connection_key"] = strings.TrimSpace(connectorKey), strings.TrimSpace(connectionKey)
	payload["_integration_context"] = contextValue
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode Integration webhook event payload: %w", err)
	}
	return encoded, nil
}

func validateIntegrationEventPayload(mapping integrationmodel.EventMappingRequirement, payload map[string]any) error {
	for _, field := range mapping.EventFields {
		value, present := integrationPayloadPath(payload, field.Path)
		if !present {
			if field.Required {
				return fmt.Errorf("Integration event field %q is required", field.Path)
			}
			continue
		}
		if !integrationEventValueMatchesType(value, field.Type) {
			return fmt.Errorf("Integration event field %q must be %s", field.Path, field.Type)
		}
		if len(field.Options) != 0 {
			matched := false
			for _, option := range field.Options {
				matched = matched || strings.TrimSpace(fmt.Sprint(value)) == strings.TrimSpace(option)
			}
			if !matched {
				return fmt.Errorf("Integration event field %q has unsupported value", field.Path)
			}
		}
	}
	return nil
}

func integrationEventValueMatchesType(value any, valueType string) bool {
	switch strings.TrimSpace(strings.ToLower(valueType)) {
	case "text", "string", "email", "url", "date", "datetime", "relation", "user":
		_, ok := value.(string)
		return ok
	case "number", "decimal", "currency", "percent":
		switch value.(type) {
		case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
			return true
		default:
			return false
		}
	case "integer":
		number, ok := value.(float64)
		return ok && number == float64(int64(number))
	case "boolean", "bool":
		_, ok := value.(bool)
		return ok
	case "object", "map":
		_, ok := value.(map[string]any)
		return ok
	case "array", "list":
		_, ok := value.([]any)
		return ok
	default:
		return false
	}
}
