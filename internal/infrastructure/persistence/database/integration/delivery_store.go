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
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

type DeliveryStore struct {
	database         modulehost.Database
	dialect          modulehost.Dialect
	providers        modulehost.ProviderRegistry
	secrets          SecretReferenceResolver
	webPush          *WebPushSubscriptionStore
	subjectLifecycle *SubjectLifecyclePersistence
}

type SecretReferenceResolver interface {
	ResolveSecretReferences(context.Context, string, map[string]string) (map[string]string, error)
}

type providerSecretVersion struct {
	SecretKey, Fingerprint, UpdatedAt string
}

type resolvedProviderSecrets struct {
	Values   map[string]string
	Versions map[string]providerSecretVersion
}

type SecretSnapshotResolver interface {
	ResolveSecretReferencesSnapshot(context.Context, string, map[string]string) (resolvedProviderSecrets, error)
}

type SecretUpdateWriter interface {
	ApplySecretUpdates(context.Context, string, map[string]string, map[string]string) error
}

type ConditionalSecretUpdateWriter interface {
	ApplySecretUpdatesIfCurrent(context.Context, string, map[string]string, map[string]providerSecretVersion, map[string]string) error
}

func NewDeliveryStore(database modulehost.Database, dialect modulehost.Dialect, providers modulehost.ProviderRegistry, secrets SecretReferenceResolver, webPush ...*WebPushSubscriptionStore) *DeliveryStore {
	store := &DeliveryStore{database: database, dialect: dialect, providers: providers, secrets: secrets, subjectLifecycle: NewSubjectLifecyclePersistence()}
	if len(webPush) != 0 {
		store.webPush = webPush[0]
	}
	return store
}

func NewDeliveryStoreWithSubjectLifecycle(database modulehost.Database, dialect modulehost.Dialect, providers modulehost.ProviderRegistry, secrets SecretReferenceResolver, webPush *WebPushSubscriptionStore, subjectLifecycle *SubjectLifecyclePersistence) *DeliveryStore {
	return &DeliveryStore{database: database, dialect: dialect, providers: providers, secrets: secrets, webPush: webPush, subjectLifecycle: subjectLifecyclePersistence([]*SubjectLifecyclePersistence{subjectLifecycle})}
}

type deliveryConnection struct {
	Key, WorkspaceID, ConnectorKey, ProviderKey, Status string
	UpdatedAt                                           string
	Config                                              map[string]any
	SecretRefs                                          map[string]string
}

func (s *DeliveryStore) Accept(ctx context.Context, request integrationmodel.DeliveryRequest) (integrationmodel.DeliveryReceipt, error) {
	id := deliveryInvocationID(request.MessageID)
	if receipt, found, err := s.receipt(ctx, request.MessageID, id); err != nil {
		return integrationmodel.DeliveryReceipt{}, err
	} else if found && receipt.Status == "succeeded" {
		return receipt, nil
	}
	connection, err := s.connection(ctx, request)
	if err != nil {
		return integrationmodel.DeliveryReceipt{}, err
	}
	provider, ok := s.providers.Provider(connection.ConnectorKey, connection.ProviderKey)
	if !ok {
		return integrationmodel.DeliveryReceipt{}, fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
	}
	operation, ok := providerOperation(provider.Descriptor(), request.Operation)
	if !ok {
		return integrationmodel.DeliveryReceipt{}, fmt.Errorf("Integration provider %s/%s does not implement operation %s", connection.ConnectorKey, connection.ProviderKey, request.Operation)
	}
	resolvedSecrets, err := s.resolveProviderSecrets(ctx, request.WorkspaceID, connection.SecretRefs)
	if err != nil {
		return integrationmodel.DeliveryReceipt{}, err
	}
	providerPayload := request.Payload
	if request.ConnectorKey == "notification" && request.Operation == "send" {
		providerPayload, err = s.hydrateWebPushPayload(ctx, request.WorkspaceID, request.Payload)
		if err != nil {
			return integrationmodel.DeliveryReceipt{}, err
		}
	}
	if err := s.prepareInvocation(ctx, id, request, connection); err != nil {
		return integrationmodel.DeliveryReceipt{}, err
	}
	result, callErr := provider.Call(ctx, connector.CallRequest{
		ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, OperationKey: request.Operation,
		ContractSHA256: operation.ContractSHA256, Mode: operation.Mode, Payload: providerPayload,
		RequestRef: request.MessageID, Delivery: true, Secrets: resolvedSecrets.Values,
		Connection: connector.Connection{Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs},
		Principal:  connector.Principal{WorkspaceID: request.WorkspaceID, RequestID: request.MessageID, IsAuthenticated: true},
	})
	callErr = s.persistProviderSecretUpdates(ctx, request.WorkspaceID, connection.SecretRefs, resolvedSecrets.Versions, result.SecretUpdates, callErr)
	status, errorText := "succeeded", ""
	if callErr != nil {
		status, errorText = "failed", callErr.Error()
	}
	if err := s.finishInvocation(ctx, request.WorkspaceID, id, status, result.ResponseRef, errorText); err != nil {
		return integrationmodel.DeliveryReceipt{}, err
	}
	receipt := integrationmodel.DeliveryReceipt{MessageID: request.MessageID, InvocationID: id, Status: status, ResultRef: result.ResponseRef}
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

func (s *DeliveryStore) Query(ctx context.Context, messageID string) (integrationmodel.DeliveryReceipt, error) {
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return integrationmodel.DeliveryReceipt{}, fmt.Errorf("Integration delivery message ID is required")
	}
	receipt, found, err := s.receipt(ctx, messageID, deliveryInvocationID(messageID))
	if err != nil {
		return integrationmodel.DeliveryReceipt{}, err
	}
	if !found {
		return integrationmodel.DeliveryReceipt{}, fmt.Errorf("Integration delivery %q was not found", messageID)
	}
	return receipt, nil
}

func (s *DeliveryStore) connection(ctx context.Context, request integrationmodel.DeliveryRequest) (deliveryConnection, error) {
	predicates := []query.Predicate{query.Equal("workspace_id", request.WorkspaceID), query.Equal("connector_key", request.ConnectorKey), query.Equal("status", "active")}
	if request.ConnectionKey != "" {
		predicates = append(predicates, query.Equal("connection_key", request.ConnectionKey))
	}
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_integration_connections").Columns("connection_key", "workspace_id", "connector_key", "provider_key", "status", "config_json", "secret_refs_json", "updated_at").Where(query.And(predicates...)).OrderBy(query.Ascending("connection_key")).Limit(2).Build()
	if err != nil {
		return deliveryConnection{}, err
	}
	rows, err := s.database.QueryContext(ctx, queryValue, args...)
	if err != nil {
		return deliveryConnection{}, fmt.Errorf("query Integration delivery connection: %w", err)
	}
	defer rows.Close()
	values := []deliveryConnection{}
	for rows.Next() {
		var value deliveryConnection
		var configJSON, refsJSON string
		if err := rows.Scan(&value.Key, &value.WorkspaceID, &value.ConnectorKey, &value.ProviderKey, &value.Status, &configJSON, &refsJSON, &value.UpdatedAt); err != nil {
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

func (s *DeliveryStore) prepareInvocation(ctx context.Context, id string, request integrationmodel.DeliveryRequest, connection deliveryConnection) error {
	metadata := map[string]any{"message_id": request.MessageID, "deduplication_key": request.DeduplicationKey, "payload": json.RawMessage(request.Payload)}
	return s.prepareInvocationWithMetadata(ctx, id, request, connection, metadata)
}

func (s *DeliveryStore) prepareInvocationWithMetadata(ctx context.Context, id string, request integrationmodel.DeliveryRequest, connection deliveryConnection, metadataValue map[string]any) error {
	if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, request.WorkspaceID, subjectFenceReference{"connection", "", connection.Key}, subjectFenceReference{"message", "", request.MessageID}, subjectFenceReference{"row", "_integration_invocations", id}); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	metadata, _ := json.Marshal(metadataValue)
	lookup, lookupArgs, err := query.NewSelectBuilder(s.dialect, "_integration_invocations").Columns("status").Where(query.Equal("id", id)).Build()
	if err != nil {
		return err
	}
	var current string
	err = s.database.QueryRowContext(ctx, lookup, lookupArgs...).Scan(&current)
	if err == nil {
		update, args, buildErr := query.NewUpdateBuilder(s.dialect, "_integration_invocations").Set("status", "running").Set("error", nil).Set("metadata_json", string(metadata)).Set("updated_at", now).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, request.WorkspaceID, "_integration_invocations", id), query.Equal("id", id))).Build()
		if buildErr != nil {
			return buildErr
		}
		result, execErr := s.database.ExecContext(ctx, update, args...)
		if execErr != nil {
			return execErr
		}
		count, execErr := result.RowsAffected()
		if execErr != nil {
			return execErr
		}
		if count != 1 {
			return fmt.Errorf("Integration invocation claim changed")
		}
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}
	return s.insertInvocation(ctx, id, request, connection, metadata)
}

// insertInvocation claims one identity with a unique primary-key insert. The
// synchronous sensitive-call path must not use the delivery retry transition.
func (s *DeliveryStore) insertInvocation(ctx context.Context, id string, request integrationmodel.DeliveryRequest, connection deliveryConnection, metadata []byte) error {
	if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, request.WorkspaceID, subjectFenceReference{"connection", "", connection.Key}, subjectFenceReference{"message", "", request.MessageID}, subjectFenceReference{"row", "_integration_invocations", id}); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	queryValue, args, err := query.NewInsertBuilder(s.dialect, "_integration_invocations").Columns("id", "workspace_id", "connector_key", "provider_key", "connection_key", "operation", "status", "duration_ms", "request_ref", "response_ref", "error", "event_id", "object_key", "record_id", "workflow_execution_id", "metadata_json", "created_at", "updated_at").Values(id, request.WorkspaceID, request.ConnectorKey, connection.ProviderKey, connection.Key, request.Operation, "running", int64(0), request.MessageID, nil, nil, nil, nil, nil, nil, string(metadata), now, now).Build()
	if err != nil {
		return err
	}
	result, err := s.database.ExecContext(ctx, queryValue, args...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("Integration invocation claim changed")
	}
	return nil
}

func (s *DeliveryStore) finishInvocation(ctx context.Context, workspaceID, id, status, responseRef, errorText string) error {
	queryValue, args, err := query.NewUpdateBuilder(s.dialect, "_integration_invocations").Set("status", string(status)).Set("response_ref", responseRef).Set("error", errorText).Set("updated_at", time.Now().UTC().Format(time.RFC3339Nano)).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, workspaceID, "_integration_invocations", id), query.Equal("id", id))).Build()
	if err != nil {
		return err
	}
	result, err := s.database.ExecContext(ctx, queryValue, args...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("Integration invocation completion changed")
	}
	return nil
}

func (s *DeliveryStore) receipt(ctx context.Context, messageID, id string) (integrationmodel.DeliveryReceipt, bool, error) {
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_integration_invocations").Columns("status", "response_ref", "error").Where(query.Equal("id", id)).Build()
	if err != nil {
		return integrationmodel.DeliveryReceipt{}, false, err
	}
	var status string
	var responseRef, errorText sql.NullString
	if err := s.database.QueryRowContext(ctx, queryValue, args...).Scan(&status, &responseRef, &errorText); err == sql.ErrNoRows {
		return integrationmodel.DeliveryReceipt{}, false, nil
	} else if err != nil {
		return integrationmodel.DeliveryReceipt{}, false, err
	}
	receipt := integrationmodel.DeliveryReceipt{MessageID: messageID, InvocationID: id, Status: status, ResultRef: responseRef.String}
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
