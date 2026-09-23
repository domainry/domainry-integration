package integration

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

// ManagementStore is the only owner-side DML implementation for Integration
// configuration. Runtime projections consume it through the SDK and never read
// these tables directly.
type ManagementStore struct {
	database         sqlhost.DBTX
	transactions     modulehost.Database
	dialect          modulehost.Dialect
	cipher           modulehost.SecretMaterialCipher
	delivery         *DeliveryStore
	subjectLifecycle *SubjectLifecyclePersistence
}

func NewManagementStore(database modulehost.Database, dialect modulehost.Dialect, cipher modulehost.SecretMaterialCipher, delivery *DeliveryStore, subjectLifecycle ...*SubjectLifecyclePersistence) *ManagementStore {
	return &ManagementStore{database: database, transactions: database, dialect: dialect, cipher: cipher, delivery: delivery, subjectLifecycle: subjectLifecyclePersistence(subjectLifecycle)}
}

func (s *ManagementStore) withTransaction(ctx context.Context, operation func(*ManagementStore) error) error {
	if s.transactions == nil {
		return operation(s)
	}
	tx, err := s.transactions.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	scoped := *s
	scoped.database, scoped.transactions = tx, nil
	if err := operation(&scoped); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *ManagementStore) requireScopedCandidate(ctx context.Context, table, keyColumn, workspaceID, key string) error {
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal(keyColumn, strings.TrimSpace(key)))
	if err != nil {
		return err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, table).Columns("id").Where(where).Limit(1).Build()
	if err != nil {
		return err
	}
	var id string
	if err := s.database.QueryRowContext(ctx, statement, args...).Scan(&id); err == sql.ErrNoRows {
		return fmt.Errorf("Integration resource was not found")
	} else if err != nil {
		return err
	}
	return nil
}

func ownerNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func ownerID(prefix, workspaceID, key string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(workspaceID) + "\x00" + strings.TrimSpace(key)))
	return prefix + hex.EncodeToString(digest[:16])
}

func ownerJSON(value any, fallback string) (string, error) {
	if value == nil {
		return fallback, nil
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func requiredOwnerValue(label, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("Integration %s is required", label)
	}
	return value, nil
}

func (s *ManagementStore) ListConnections(ctx context.Context, workspaceID string) ([]integrationsdk.Connection, error) {
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return nil, err
	}
	where, err := scopedWhere(ctx, workspaceID, "", "")
	if err != nil {
		return nil, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connections").
		Columns("connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").
		Where(where).OrderBy(query.Ascending("connection_key")).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list Integration connections: %w", err)
	}
	defer rows.Close()
	values := []integrationsdk.Connection{}
	for rows.Next() {
		value, err := scanConnection(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

type rowScanner interface{ Scan(...any) error }

func scanConnection(row rowScanner) (integrationsdk.Connection, error) {
	var value integrationsdk.Connection
	var name, createdBy sql.NullString
	var configJSON, refsJSON string
	if err := row.Scan(&value.Key, &value.WorkspaceID, &value.ConnectorKey, &value.ProviderKey, &name, &value.Status, &configJSON, &refsJSON, &createdBy, &value.CreatedAt, &value.UpdatedAt); err != nil {
		return value, err
	}
	value.Name, value.CreatedBy = name.String, createdBy.String
	if err := json.Unmarshal([]byte(configJSON), &value.Config); err != nil {
		return value, fmt.Errorf("decode Integration connection config: %w", err)
	}
	if err := json.Unmarshal([]byte(refsJSON), &value.SecretRefs); err != nil {
		return value, fmt.Errorf("decode Integration connection secret references: %w", err)
	}
	return value, nil
}

func (s *ManagementStore) GetConnection(ctx context.Context, workspaceID, key string) (integrationsdk.Connection, error) {
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	key, err = requiredOwnerValue("connection key", key)
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	where, err := scopedWhere(ctx, workspaceID, "", "", query.Equal("connection_key", key))
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connections").
		Columns("connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").
		Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	value, err := scanConnection(s.database.QueryRowContext(ctx, statement, args...))
	if err == sql.ErrNoRows {
		return value, fmt.Errorf("Integration connection %q was not found", key)
	}
	return value, err
}

func (s *ManagementStore) UpsertConnection(ctx context.Context, workspaceID, key, actorID string, input integrationsdk.ConnectionInput) (integrationsdk.Connection, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.Connection{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.Connection
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.UpsertConnection(ctx, workspaceID, key, actorID, input)
			return operationErr
		})
		return value, err
	}
	if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, workspaceID, subjectFenceReference{"connection", "", key}, subjectFenceReference{"subject", "", actorID}); err != nil {
		return integrationsdk.Connection{}, err
	}
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	key, err = requiredOwnerValue("connection key", key)
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	input.ConnectorKey, err = requiredOwnerValue("connector key", input.ConnectorKey)
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	input.ProviderKey, err = requiredOwnerValue("provider key", input.ProviderKey)
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	if strings.TrimSpace(input.Status) == "" {
		input.Status = "configured"
	}
	if !validConnectionStatus(input.Status) {
		return integrationsdk.Connection{}, fmt.Errorf("Integration connection %q has invalid status %q", key, input.Status)
	}
	if s.delivery == nil || s.delivery.providers == nil {
		return integrationsdk.Connection{}, fmt.Errorf("Integration Provider registry is unavailable")
	}
	provider, found := s.delivery.providers.Provider(input.ConnectorKey, input.ProviderKey)
	if !found {
		return integrationsdk.Connection{}, fmt.Errorf("Integration provider %s/%s is unavailable", input.ConnectorKey, input.ProviderKey)
	}
	normalized, err := normalizeProviderConnection(provider, connector.Connection{
		Key: key, WorkspaceID: workspaceID, ConnectorKey: input.ConnectorKey, ProviderKey: input.ProviderKey,
		Name: input.Name, Status: input.Status, Config: input.Config, SecretRefs: input.SecretRefs,
	}, input.Status == "active" || input.Status == "inactive")
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	input.Config, input.SecretRefs = normalized.Config, normalized.SecretRefs
	configJSON, err := ownerJSON(input.Config, `{}`)
	if err != nil {
		return integrationsdk.Connection{}, fmt.Errorf("encode Integration connection config: %w", err)
	}
	refsJSON, err := ownerJSON(input.SecretRefs, `{}`)
	if err != nil {
		return integrationsdk.Connection{}, fmt.Errorf("encode Integration connection secret references: %w", err)
	}
	now := ownerNow()
	where, err := scopedWhere(ctx, workspaceID, "", "", query.Equal("connection_key", key))
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	lookup, lookupArgs, err := query.NewSelectBuilder(s.dialect, "_integration_connections").Columns("id").Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	var id string
	lookupErr := s.database.QueryRowContext(ctx, lookup, lookupArgs...).Scan(&id)
	if lookupErr != nil && lookupErr != sql.ErrNoRows {
		return integrationsdk.Connection{}, lookupErr
	}
	if lookupErr == sql.ErrNoRows {
		id = ownerID("connection_", workspaceID, key)
		actorID, _ = scopeOwner(ctx, actorID)
		statement, args, buildErr := query.NewInsertBuilder(s.dialect, "_integration_connections").Columns(
			"id", "connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at",
		).Values(id, key, workspaceID, input.ConnectorKey, input.ProviderKey, input.Name, input.Status, configJSON, refsJSON, actorID, now, now).Build()
		if buildErr != nil {
			return integrationsdk.Connection{}, buildErr
		}
		if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationsdk.Connection{}, fmt.Errorf("insert Integration connection: %w", err)
		}
	} else {
		statement, args, buildErr := query.NewUpdateBuilder(s.dialect, "_integration_connections").
			Set("connector_key", input.ConnectorKey).Set("provider_key", input.ProviderKey).Set("name", input.Name).
			Set("status", input.Status).Set("config_json", configJSON).Set("secret_refs_json", refsJSON).Set("updated_at", now).
			Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, workspaceID, "_integration_connections", id), where)).Build()
		if buildErr != nil {
			return integrationsdk.Connection{}, buildErr
		}
		if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationsdk.Connection{}, fmt.Errorf("update Integration connection: %w", err)
		}
	}
	connection := integrationsdk.Connection{Key: key, WorkspaceID: workspaceID, ConnectorKey: input.ConnectorKey, ProviderKey: input.ProviderKey, Name: input.Name, Status: input.Status, Config: input.Config, SecretRefs: input.SecretRefs}
	if err = s.syncConnectionAccountSecrets(ctx, connection); err != nil {
		return integrationsdk.Connection{}, err
	}
	return s.GetConnection(ctx, workspaceID, key)
}

func (s *ManagementStore) DeleteConnection(ctx context.Context, workspaceID, key string) error {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return err
	}
	if s.transactions != nil {
		return s.withTransaction(ctx, func(store *ManagementStore) error { return store.DeleteConnection(ctx, workspaceID, key) })
	}
	if err := s.requireScopedCandidate(ctx, "_integration_connections", "connection_key", workspaceID, key); err != nil {
		return err
	}
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("connection_key", strings.TrimSpace(key)))
	if err != nil {
		return err
	}
	accountWhere := query.And(query.Equal("workspace_id", strings.TrimSpace(workspaceID)), query.Equal("connection_key", strings.TrimSpace(key)))
	release, releaseArgs, buildErr := query.NewUpdateBuilder(s.dialect, "_integration_secrets").Set("connection_key", "").Where(accountWhere).Build()
	if buildErr != nil {
		return buildErr
	}
	if _, execErr := s.database.ExecContext(ctx, release, releaseArgs...); execErr != nil {
		return execErr
	}
	deleteAccount, deleteAccountArgs, buildErr := query.NewDeleteBuilder(s.dialect, "_integration_connection_accounts").Where(accountWhere).Build()
	if buildErr != nil {
		return buildErr
	}
	if _, execErr := s.database.ExecContext(ctx, deleteAccount, deleteAccountArgs...); execErr != nil {
		return execErr
	}
	statement, args, err := query.NewDeleteBuilder(s.dialect, "_integration_connections").Where(where).Build()
	if err != nil {
		return err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return fmt.Errorf("delete Integration connection: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return fmt.Errorf("Integration connection %q was not found", key)
	}
	return nil
}

func (s *ManagementStore) SetConnectionStatus(ctx context.Context, workspaceID, key, status, _ string) (integrationsdk.Connection, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.Connection{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.Connection
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.SetConnectionStatus(ctx, workspaceID, key, status, "")
			return operationErr
		})
		return value, err
	}
	status, err := requiredOwnerValue("connection status", status)
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	if !validConnectionStatus(status) {
		return integrationsdk.Connection{}, fmt.Errorf("Integration connection %q has invalid status %q", key, status)
	}
	if status == "active" || status == "inactive" {
		connection, readErr := s.GetConnection(ctx, workspaceID, key)
		if readErr != nil {
			return integrationsdk.Connection{}, readErr
		}
		provider, found := s.delivery.providers.Provider(connection.ConnectorKey, connection.ProviderKey)
		if !found {
			return integrationsdk.Connection{}, fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
		}
		if _, validateErr := normalizeProviderConnection(provider, connector.Connection{
			Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey,
			Name: connection.Name, Status: status, Config: connection.Config, SecretRefs: connection.SecretRefs,
		}, true); validateErr != nil {
			return integrationsdk.Connection{}, validateErr
		}
	}
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("connection_key", strings.TrimSpace(key)))
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_connections").Set("status", status).Set("updated_at", ownerNow()).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, strings.TrimSpace(workspaceID), "_integration_connections", ownerID("connection_", workspaceID, key)), where)).Build()
	if err != nil {
		return integrationsdk.Connection{}, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return integrationsdk.Connection{}, fmt.Errorf("update Integration connection status: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return integrationsdk.Connection{}, fmt.Errorf("Integration connection %q was not found", key)
	}
	return s.GetConnection(ctx, workspaceID, key)
}

func (s *ManagementStore) TestConnection(ctx context.Context, workspaceID, key string, request integrationsdk.ConnectionTestRequest) (integrationsdk.ConnectionTestResult, error) {
	connection, err := s.GetConnection(ctx, workspaceID, key)
	if err != nil {
		return integrationsdk.ConnectionTestResult{}, err
	}
	if s.delivery == nil {
		return integrationsdk.ConnectionTestResult{}, fmt.Errorf("Integration delivery is unavailable")
	}
	provider, found := s.delivery.providers.Provider(connection.ConnectorKey, connection.ProviderKey)
	if !found {
		return integrationsdk.ConnectionTestResult{}, fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
	}
	tester, ok := provider.(connector.ConnectionTester)
	if !ok {
		return integrationsdk.ConnectionTestResult{}, fmt.Errorf("Integration provider %s/%s does not support connection testing", connection.ConnectorKey, connection.ProviderKey)
	}
	providerConnection, err := normalizeProviderConnection(provider, connector.Connection{
		Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey,
		Name: connection.Name, Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs,
	}, true)
	if err != nil {
		return integrationsdk.ConnectionTestResult{}, err
	}
	resolvedSecrets, err := s.delivery.resolveProviderSecrets(ctx, workspaceID, connection.SecretRefs)
	if err != nil {
		return integrationsdk.ConnectionTestResult{Connection: connection, Operation: "test_connection"}, err
	}
	result, err := tester.TestConnection(ctx, connector.TestConnectionRequest{
		ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Connection: providerConnection,
		Secrets: resolvedSecrets.Values, Principal: connector.Principal{WorkspaceID: workspaceID, IsAuthenticated: true},
	})
	err = s.delivery.persistProviderSecretUpdates(ctx, workspaceID, connection.SecretRefs, resolvedSecrets.Versions, result.SecretUpdates, err)
	status := integrationsdk.DeliveryStatusSucceeded
	if err != nil || !result.Connected {
		status = integrationsdk.DeliveryStatusFailed
	}
	receipt := integrationsdk.DeliveryReceipt{MessageID: ownerID("connection_test_", workspaceID, key+"\x00"+ownerNow()), Status: status}
	value := integrationsdk.ConnectionTestResult{Connection: connection, Operation: "test_connection", Response: append(json.RawMessage(nil), result.Details...), Receipt: receipt}
	return value, err
}

func randomOwnerToken(prefix string) (string, error) {
	buffer := make([]byte, 32)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(buffer), nil
}
