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
	ormbuilder "github.com/domainry/domainry-orm/query"
)

type DeliveryStore struct {
	database  modulehost.Database
	dialect   modulehost.Dialect
	providers modulehost.ProviderRegistry
	secrets   SecretReferenceResolver
	webPush   *WebPushSubscriptionStore
}

type SecretReferenceResolver interface {
	ResolveSecretReferences(context.Context, string, map[string]string) (map[string]string, error)
}

func NewDeliveryStore(database modulehost.Database, dialect modulehost.Dialect, providers modulehost.ProviderRegistry, secrets SecretReferenceResolver, webPush ...*WebPushSubscriptionStore) *DeliveryStore {
	store := &DeliveryStore{database: database, dialect: dialect, providers: providers, secrets: secrets}
	if len(webPush) != 0 {
		store.webPush = webPush[0]
	}
	return store
}

type deliveryConnection struct {
	Key, WorkspaceID, ConnectorKey, ProviderKey, Status string
	Config                                              map[string]any
	SecretRefs                                          map[string]string
}

func (s *DeliveryStore) Accept(ctx context.Context, request integrationsdk.DeliveryRequest) (integrationsdk.DeliveryReceipt, error) {
	if err := request.Validate(); err != nil {
		return integrationsdk.DeliveryReceipt{}, err
	}
	id := deliveryInvocationID(request.MessageID)
	if receipt, found, err := s.receipt(ctx, request.MessageID, id); err != nil {
		return integrationsdk.DeliveryReceipt{}, err
	} else if found && receipt.Status == integrationsdk.DeliveryStatusSucceeded {
		return receipt, nil
	}
	connection, err := s.connection(ctx, request)
	if err != nil {
		return integrationsdk.DeliveryReceipt{}, err
	}
	provider, ok := s.providers.Provider(connection.ConnectorKey, connection.ProviderKey)
	if !ok {
		return integrationsdk.DeliveryReceipt{}, fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
	}
	operation, ok := providerOperation(provider.Descriptor(), request.Operation)
	if !ok {
		return integrationsdk.DeliveryReceipt{}, fmt.Errorf("Integration provider %s/%s does not implement operation %s", connection.ConnectorKey, connection.ProviderKey, request.Operation)
	}
	secrets, err := s.secrets.ResolveSecretReferences(ctx, request.WorkspaceID, connection.SecretRefs)
	if err != nil {
		return integrationsdk.DeliveryReceipt{}, err
	}
	providerPayload := request.Payload
	if request.ConnectorKey == "notification" && request.Operation == "send" {
		providerPayload, err = s.hydrateWebPushPayload(ctx, request.WorkspaceID, request.Payload)
		if err != nil {
			return integrationsdk.DeliveryReceipt{}, err
		}
	}
	if err := s.prepareInvocation(ctx, id, request, connection); err != nil {
		return integrationsdk.DeliveryReceipt{}, err
	}
	result, callErr := provider.Call(ctx, connector.CallRequest{
		ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, OperationKey: request.Operation,
		ContractSHA256: operation.ContractSHA256, Mode: operation.Mode, Payload: providerPayload,
		RequestRef: request.MessageID, Delivery: true, Secrets: secrets,
		Connection: connector.Connection{Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs},
		Principal:  connector.Principal{WorkspaceID: request.WorkspaceID, RequestID: request.MessageID, IsAuthenticated: true},
	})
	status, errorText := integrationsdk.DeliveryStatusSucceeded, ""
	if callErr != nil {
		status, errorText = integrationsdk.DeliveryStatusFailed, callErr.Error()
	}
	if err := s.finishInvocation(ctx, id, status, result.ResponseRef, errorText); err != nil {
		return integrationsdk.DeliveryReceipt{}, err
	}
	receipt := integrationsdk.DeliveryReceipt{MessageID: request.MessageID, InvocationID: id, Status: status, ResultRef: result.ResponseRef}
	if callErr != nil {
		receipt.ErrorCode = "provider_call_failed"
		return receipt, callErr
	}
	return receipt, nil
}

func (s *DeliveryStore) hydrateWebPushPayload(ctx context.Context, workspaceID string, payload json.RawMessage) (json.RawMessage, error) {
	if s.webPush == nil {
		return nil, fmt.Errorf("Integration Web Push subscription store is unavailable")
	}
	var value map[string]any
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, fmt.Errorf("decode Web Push delivery payload: %w", err)
	}
	id := strings.TrimSpace(fmt.Sprint(value["subscription_id"]))
	material, found, err := s.webPush.material(ctx, workspaceID, id)
	if err != nil {
		return nil, err
	}
	if !found || material.Status != "active" || material.Endpoint == "" {
		return nil, fmt.Errorf("Integration Web Push subscription is unavailable")
	}
	if material.ExpiresAt != "" {
		if expires, parseErr := time.Parse(time.RFC3339, material.ExpiresAt); parseErr == nil && !expires.After(time.Now().UTC()) {
			return nil, fmt.Errorf("Integration Web Push subscription is expired")
		}
	}
	value["endpoint"], value["p256dh"], value["auth"] = material.Endpoint, material.P256DH, material.Auth
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode Web Push delivery payload: %w", err)
	}
	return encoded, nil
}

func (s *DeliveryStore) Query(ctx context.Context, messageID string) (integrationsdk.DeliveryReceipt, error) {
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return integrationsdk.DeliveryReceipt{}, fmt.Errorf("Integration delivery message ID is required")
	}
	receipt, found, err := s.receipt(ctx, messageID, deliveryInvocationID(messageID))
	if err != nil {
		return integrationsdk.DeliveryReceipt{}, err
	}
	if !found {
		return integrationsdk.DeliveryReceipt{}, fmt.Errorf("Integration delivery %q was not found", messageID)
	}
	return receipt, nil
}

func (s *DeliveryStore) connection(ctx context.Context, request integrationsdk.DeliveryRequest) (deliveryConnection, error) {
	predicates := []ormbuilder.Predicate{ormbuilder.Equal("workspace_id", request.WorkspaceID), ormbuilder.Equal("connector_key", request.ConnectorKey), ormbuilder.Equal("status", "active")}
	if request.ConnectionKey != "" {
		predicates = append(predicates, ormbuilder.Equal("connection_key", request.ConnectionKey))
	}
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "_integration_connections").Columns("connection_key", "workspace_id", "connector_key", "provider_key", "status", "config_json", "secret_refs_json").Where(ormbuilder.And(predicates...)).OrderBy(ormbuilder.Ascending("connection_key")).Limit(2).Build()
	if err != nil {
		return deliveryConnection{}, err
	}
	rows, err := s.database.QueryContext(ctx, query, args...)
	if err != nil {
		return deliveryConnection{}, fmt.Errorf("query Integration delivery connection: %w", err)
	}
	defer rows.Close()
	values := []deliveryConnection{}
	for rows.Next() {
		var value deliveryConnection
		var configJSON, refsJSON string
		if err := rows.Scan(&value.Key, &value.WorkspaceID, &value.ConnectorKey, &value.ProviderKey, &value.Status, &configJSON, &refsJSON); err != nil {
			return deliveryConnection{}, err
		}
		if err := json.Unmarshal([]byte(configJSON), &value.Config); err != nil {
			return deliveryConnection{}, err
		}
		if err := json.Unmarshal([]byte(refsJSON), &value.SecretRefs); err != nil {
			return deliveryConnection{}, err
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return deliveryConnection{}, fmt.Errorf("active Integration connection for connector %q was not found", request.ConnectorKey)
	}
	if len(values) > 1 && request.ConnectionKey == "" {
		return deliveryConnection{}, fmt.Errorf("Integration connector %q requires an explicit connection", request.ConnectorKey)
	}
	return values[0], rows.Err()
}

func (s *DeliveryStore) prepareInvocation(ctx context.Context, id string, request integrationsdk.DeliveryRequest, connection deliveryConnection) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	metadata, _ := json.Marshal(map[string]any{"message_id": request.MessageID, "deduplication_key": request.DeduplicationKey, "payload": json.RawMessage(request.Payload)})
	lookup, lookupArgs, err := ormbuilder.NewSelectBuilder(s.dialect, "_integration_invocations").Columns("status").Where(ormbuilder.Equal("id", id)).Build()
	if err != nil {
		return err
	}
	var current string
	err = s.database.QueryRowContext(ctx, lookup, lookupArgs...).Scan(&current)
	if err == nil {
		update, args, buildErr := ormbuilder.NewUpdateBuilder(s.dialect, "_integration_invocations").Set("status", "running").Set("error", nil).Set("updated_at", now).Where(ormbuilder.Equal("id", id)).Build()
		if buildErr != nil {
			return buildErr
		}
		_, execErr := s.database.ExecContext(ctx, update, args...)
		return execErr
	}
	if err != sql.ErrNoRows {
		return err
	}
	query, args, err := ormbuilder.NewInsertBuilder(s.dialect, "_integration_invocations").Columns("id", "workspace_id", "connector_key", "provider_key", "connection_key", "operation", "status", "duration_ms", "request_ref", "response_ref", "error", "event_id", "object_key", "record_id", "workflow_execution_id", "metadata_json", "created_at", "updated_at").Values(id, request.WorkspaceID, request.ConnectorKey, connection.ProviderKey, connection.Key, request.Operation, "running", int64(0), request.MessageID, nil, nil, nil, nil, nil, nil, string(metadata), now, now).Build()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, query, args...)
	return err
}

func (s *DeliveryStore) finishInvocation(ctx context.Context, id string, status integrationsdk.DeliveryStatus, responseRef, errorText string) error {
	query, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "_integration_invocations").Set("status", string(status)).Set("response_ref", responseRef).Set("error", errorText).Set("updated_at", time.Now().UTC().Format(time.RFC3339Nano)).Where(ormbuilder.Equal("id", id)).Build()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, query, args...)
	return err
}

func (s *DeliveryStore) receipt(ctx context.Context, messageID, id string) (integrationsdk.DeliveryReceipt, bool, error) {
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "_integration_invocations").Columns("status", "response_ref", "error").Where(ormbuilder.Equal("id", id)).Build()
	if err != nil {
		return integrationsdk.DeliveryReceipt{}, false, err
	}
	var status string
	var responseRef, errorText sql.NullString
	if err := s.database.QueryRowContext(ctx, query, args...).Scan(&status, &responseRef, &errorText); err == sql.ErrNoRows {
		return integrationsdk.DeliveryReceipt{}, false, nil
	} else if err != nil {
		return integrationsdk.DeliveryReceipt{}, false, err
	}
	receipt := integrationsdk.DeliveryReceipt{MessageID: messageID, InvocationID: id, Status: integrationsdk.DeliveryStatus(status), ResultRef: responseRef.String}
	if errorText.String != "" {
		receipt.ErrorCode = "provider_call_failed"
	}
	return receipt, true, nil
}

func deliveryInvocationID(messageID string) string {
	hash := sha256.Sum256([]byte(strings.TrimSpace(messageID)))
	return "delivery:" + hex.EncodeToString(hash[:])
}
func providerOperation(descriptor connector.ProviderDescriptor, key string) (connector.OperationDescriptor, bool) {
	for _, operation := range descriptor.Operations {
		if operation.Key == key {
			return operation, true
		}
	}
	return connector.OperationDescriptor{}, false
}

var _ integrationsdk.Delivery = (*DeliveryStore)(nil)
