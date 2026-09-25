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
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

type OperationsStore struct {
	database         sqlhost.DBTX
	transactions     modulehost.Database
	dialect          modulehost.Dialect
	delivery         *DeliveryStore
	triggers         integrationsdk.TriggerSink
	definitions      metadatasdk.DefinitionStore
	subjectLifecycle *SubjectLifecyclePersistence
}

func NewOperationsStore(database modulehost.Database, dialect modulehost.Dialect, delivery *DeliveryStore, triggers integrationsdk.TriggerSink, definitions metadatasdk.DefinitionStore, subjectLifecycle ...*SubjectLifecyclePersistence) *OperationsStore {
	return &OperationsStore{database: database, transactions: database, dialect: dialect, delivery: delivery, triggers: triggers, definitions: definitions, subjectLifecycle: subjectLifecyclePersistence(subjectLifecycle)}
}

func (s *OperationsStore) Call(ctx context.Context, request integrationmodel.ProviderCallRequest) (integrationmodel.ProviderCallResult, error) {
	if request.Source.RecordID != "" {
		if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, request.WorkspaceID, subjectFenceReference{"resource", request.Source.ObjectKey, request.Source.RecordID}); err != nil {
			return integrationmodel.ProviderCallResult{}, err
		}
	}
	if request.ActorID != "" {
		if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, request.WorkspaceID, subjectFenceReference{"subject", "", request.ActorID}); err != nil {
			return integrationmodel.ProviderCallResult{}, err
		}
	}
	invocationID := operationInvocationID(request.WorkspaceID, request.RequestID)
	if current, err := s.GetInvocation(ctx, request.WorkspaceID, invocationID); err == nil && current.Status == "succeeded" {
		if request.PersistenceMode == integrationmodel.ProviderCallPersistenceSensitive {
			return integrationmodel.ProviderCallResult{Invocation: current}, nil
		}
		return integrationmodel.ProviderCallResult{Invocation: current, Response: invocationResponse(current.Metadata)}, nil
	}
	connection, err := s.delivery.connection(ctx, integrationmodel.DeliveryRequest{WorkspaceID: request.WorkspaceID, ConnectorKey: request.ConnectorKey, ConnectionKey: request.ConnectionKey})
	if err != nil {
		return integrationmodel.ProviderCallResult{}, err
	}
	provider, ok := s.delivery.providers.Provider(connection.ConnectorKey, connection.ProviderKey)
	if !ok {
		return integrationmodel.ProviderCallResult{}, fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
	}
	operation, ok := providerOperation(provider.Descriptor(), request.Operation)
	if !ok {
		return integrationmodel.ProviderCallResult{}, fmt.Errorf("Integration provider %s/%s does not implement operation %s", connection.ConnectorKey, connection.ProviderKey, request.Operation)
	}
	if operation.Mode != connector.ModeCall {
		return integrationmodel.ProviderCallResult{}, fmt.Errorf("Integration operation %s is not a synchronous call", request.Operation)
	}
	if expected := request.ReadExpectation; expected != nil {
		if expected.ConnectionUpdatedAt == "" || expected.ConnectionUpdatedAt != connection.UpdatedAt || expected.ProviderKey != connection.ProviderKey || expected.ContractSHA256 != operation.ContractSHA256 || operation.Reliability.Effect != connector.EffectRead {
			return integrationmodel.ProviderCallResult{}, fmt.Errorf("Integration account read target changed before execution")
		}
	}
	resolvedSecrets, err := s.delivery.resolveProviderSecrets(ctx, request.WorkspaceID, connection.SecretRefs)
	if err != nil {
		return integrationmodel.ProviderCallResult{}, err
	}
	deliveryRequest := integrationmodel.DeliveryRequest{MessageID: request.RequestID, DeduplicationKey: request.RequestID, WorkspaceID: request.WorkspaceID, ConnectorKey: request.ConnectorKey, ConnectionKey: request.ConnectionKey, Operation: request.Operation, Payload: request.Payload}
	if err := s.prepareProviderInvocation(ctx, invocationID, deliveryRequest, connection, request); err != nil {
		return integrationmodel.ProviderCallResult{}, err
	}
	started := time.Now()
	result, callErr := provider.Call(ctx, connector.CallRequest{
		ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, OperationKey: request.Operation,
		ContractSHA256: operation.ContractSHA256, Mode: operation.Mode, Connection: connector.Connection{Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Name: "", Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs},
		Payload: request.Payload, RequestRef: request.RequestID, Secrets: resolvedSecrets.Values,
		Principal: connector.Principal{UserID: request.ActorID, RoleKey: request.RoleKey, WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, IsAuthenticated: strings.TrimSpace(request.ActorID) != ""},
	})
	callErr = s.delivery.persistProviderSecretUpdates(ctx, request.WorkspaceID, connection.SecretRefs, resolvedSecrets.Versions, result.SecretUpdates, callErr)
	status, errorText := "succeeded", ""
	if callErr != nil {
		status, errorText = "failed", callErr.Error()
	}
	metadataValue := map[string]any{"resource_health": result.ResourceHealth}
	responseRef := result.ResponseRef
	if request.PersistenceMode == integrationmodel.ProviderCallPersistenceSensitive {
		metadataValue = sensitiveInvocationMetadata(request)
		responseRef = ""
		if callErr != nil {
			errorText = "sensitive provider call failed"
		}
	} else {
		metadataValue["request"] = json.RawMessage(request.Payload)
		metadataValue["response"] = json.RawMessage(result.Payload)
		metadataValue["kind"] = "operation-v1"
		metadataValue["actor_id"] = request.ActorID
		metadataValue["source"] = request.Source
	}
	metadata, _ := json.Marshal(metadataValue)
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_invocations").Set("status", status).Set("duration_ms", time.Since(started).Milliseconds()).Set("response_ref", responseRef).Set("error", errorText).Set("metadata_json", string(metadata)).Set("updated_at", time.Now().UTC().UnixMilli()).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, request.WorkspaceID, "_integration_invocations", invocationID), query.And(query.Equal("workspace_id", request.WorkspaceID), query.Equal("id", invocationID)))).Build()
	if err != nil {
		return integrationmodel.ProviderCallResult{}, err
	}
	if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
		return integrationmodel.ProviderCallResult{}, err
	}
	invocation, readErr := s.GetInvocation(ctx, request.WorkspaceID, invocationID)
	if readErr != nil {
		return integrationmodel.ProviderCallResult{}, readErr
	}
	if callErr != nil {
		return integrationmodel.ProviderCallResult{Invocation: invocation}, callErr
	}
	return integrationmodel.ProviderCallResult{Invocation: invocation, Response: append(json.RawMessage(nil), result.Payload...)}, nil
}

func (s *OperationsStore) prepareProviderInvocation(ctx context.Context, invocationID string, deliveryRequest integrationmodel.DeliveryRequest, connection deliveryConnection, request integrationmodel.ProviderCallRequest) error {
	if request.PersistenceMode != integrationmodel.ProviderCallPersistenceSensitive {
		return s.delivery.prepareInvocationWithMetadata(ctx, invocationID, deliveryRequest, connection, map[string]any{"kind": "operation-v1", "actor_id": request.ActorID, "source": request.Source, "message_id": request.RequestID, "payload": json.RawMessage(request.Payload)})
	}
	// Claim before external I/O using an insert-only identity. Failed, in-flight
	// or crash-interrupted sensitive requests cannot be reset to running: a
	// read can be billable, and its unavailable response cannot prove no work
	// occurred. A separately authorized new RequestID is an explicit new call.
	metadata, err := json.Marshal(sensitiveInvocationMetadata(request))
	if err != nil {
		return err
	}
	return s.delivery.insertInvocation(ctx, invocationID, deliveryRequest, connection, metadata)
}

func sensitiveInvocationMetadata(request integrationmodel.ProviderCallRequest) map[string]any {
	return map[string]any{
		"kind":                "operation-v1",
		"actor_id":            request.ActorID,
		"source":              request.Source,
		"message_id":          request.RequestID,
		"payload_persistence": integrationmodel.ProviderCallPersistenceSensitive,
		"masked_destination":  strings.TrimSpace(request.MaskedDestination),
	}
}

func (s *OperationsStore) ListInvocations(ctx context.Context, filter integrationmodel.InvocationQuery) ([]integrationmodel.Invocation, error) {
	if strings.TrimSpace(filter.WorkspaceID) == "" {
		return nil, fmt.Errorf("Integration invocation workspace is required")
	}
	where, err := scopedWhere(ctx, strings.TrimSpace(filter.WorkspaceID), "", "")
	if err != nil {
		return nil, err
	}
	predicates := []query.Predicate{where}
	for column, value := range map[string]string{"connector_key": filter.ConnectorKey, "connection_key": filter.ConnectionKey, "operation": filter.Operation, "status": filter.Status} {
		if value = strings.TrimSpace(value); value != "" {
			predicates = append(predicates, query.Equal(column, value))
		}
	}
	if createdFrom := strings.TrimSpace(filter.CreatedFrom); createdFrom != "" {
		predicates = append(predicates, query.GreaterThanOrEqual("created_at", timestampMillis(createdFrom)))
	}
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_invocations").Columns(invocationColumns()...).Where(query.And(predicates...)).OrderBy(query.Descending("created_at")).Limit(limit).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list Integration invocations: %w", err)
	}
	defer rows.Close()
	values := []integrationmodel.Invocation{}
	for rows.Next() {
		value, err := scanInvocation(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *OperationsStore) GetInvocation(ctx context.Context, workspaceID, id string) (integrationmodel.Invocation, error) {
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("id", strings.TrimSpace(id)))
	if err != nil {
		return integrationmodel.Invocation{}, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_invocations").Columns(invocationColumns()...).Where(where).Build()
	if err != nil {
		return integrationmodel.Invocation{}, err
	}
	value, err := scanInvocation(s.database.QueryRowContext(ctx, statement, args...))
	if err == sql.ErrNoRows {
		return integrationmodel.Invocation{}, fmt.Errorf("Integration invocation %q was not found", id)
	}
	return value, err
}

func invocationColumns() []string {
	return []string{"id", "workspace_id", "connector_key", "provider_key", "connection_key", "operation", "status", "duration_ms", "request_ref", "response_ref", "error", "event_id", "object_key", "record_id", "workflow_execution_id", "metadata_json", "created_at", "updated_at"}
}

func scanInvocation(row rowScanner) (integrationmodel.Invocation, error) {
	var value integrationmodel.Invocation
	var providerKey, connectionKey, requestRef, responseRef, errorText, eventID, objectKey, recordID, workflowID sql.NullString
	var metadataJSON string
	var createdAt, updatedAt int64
	err := row.Scan(&value.ID, &value.WorkspaceID, &value.ConnectorKey, &providerKey, &connectionKey, &value.Operation, &value.Status, &value.DurationMS, &requestRef, &responseRef, &errorText, &eventID, &objectKey, &recordID, &workflowID, &metadataJSON, &createdAt, &updatedAt)
	if err != nil {
		return value, err
	}
	value.ProviderKey, value.ConnectionKey, value.RequestRef, value.ResponseRef, value.Error = providerKey.String, connectionKey.String, requestRef.String, responseRef.String, errorText.String
	value.EventID, value.ObjectKey, value.RecordID, value.WorkflowExecutionID = eventID.String, objectKey.String, recordID.String, workflowID.String
	value.CreatedAt, value.UpdatedAt = timestampString(createdAt), timestampString(updatedAt)
	if strings.TrimSpace(metadataJSON) != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &value.Metadata); err != nil {
			return value, fmt.Errorf("decode Integration invocation metadata: %w", err)
		}
	}
	return value, nil
}

func operationInvocationID(workspaceID, requestID string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(workspaceID) + "\x00" + strings.TrimSpace(requestID)))
	return "call:" + hex.EncodeToString(digest[:])
}

func invocationResponse(metadata map[string]any) json.RawMessage {
	value, ok := metadata["response"]
	if !ok || value == nil {
		return nil
	}
	payload, _ := json.Marshal(value)
	return payload
}
