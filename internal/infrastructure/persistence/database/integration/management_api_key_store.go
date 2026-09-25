package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

func (s *ManagementStore) ListAPIKeys(ctx context.Context, workspaceID string) ([]integrationsdk.APIKey, error) {
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return nil, err
	}
	where, err := scopedWhere(ctx, workspaceID, "", "")
	if err != nil {
		return nil, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns(
		"secret_key", "workspace_id", "name", "display_prefix", "actor_id", "role_key", "scopes_json", "status", "expires_at", "last_used_at", "created_by", "created_at", "updated_at", "disabled_at",
	).Where(query.And(where, query.Equal("credential_type", "api_key"))).OrderBy(query.Ascending("secret_key")).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list Integration API keys: %w", err)
	}
	defer rows.Close()
	values := []integrationsdk.APIKey{}
	for rows.Next() {
		value, err := scanAPIKey(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func scanAPIKey(row rowScanner) (integrationsdk.APIKey, error) {
	var value integrationsdk.APIKey
	var name, createdBy sql.NullString
	var expiresAt, lastUsedAt, createdAt, updatedAt, disabledAt int64
	var scopesJSON string
	if err := row.Scan(&value.Key, &value.WorkspaceID, &name, &value.TokenPrefix, &value.ActorID, &value.RoleKey, &scopesJSON, &value.Status, &expiresAt, &lastUsedAt, &createdBy, &createdAt, &updatedAt, &disabledAt); err != nil {
		return value, err
	}
	value.Name, value.CreatedBy = name.String, createdBy.String
	value.ExpiresAt, value.LastUsedAt, value.CreatedAt, value.UpdatedAt, value.DisabledAt = timestampString(expiresAt), timestampString(lastUsedAt), timestampString(createdAt), timestampString(updatedAt), timestampString(disabledAt)
	if err := json.Unmarshal([]byte(scopesJSON), &value.Scopes); err != nil {
		return value, fmt.Errorf("decode Integration API key scopes: %w", err)
	}
	return value, nil
}

func (s *ManagementStore) getAPIKey(ctx context.Context, workspaceID, key string) (integrationsdk.APIKey, error) {
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("credential_type", "api_key"), query.Equal("secret_key", strings.TrimSpace(key)))
	if err != nil {
		return integrationsdk.APIKey{}, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns(
		"secret_key", "workspace_id", "name", "display_prefix", "actor_id", "role_key", "scopes_json", "status", "expires_at", "last_used_at", "created_by", "created_at", "updated_at", "disabled_at",
	).Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.APIKey{}, err
	}
	value, err := scanAPIKey(s.database.QueryRowContext(ctx, statement, args...))
	if err == sql.ErrNoRows {
		return value, fmt.Errorf("Integration API key %q was not found", key)
	}
	return value, err
}

func newAPIKeyCredential(input integrationsdk.APIKeyInput) (string, string, string, error) {
	token, err := randomOwnerToken("itg_")
	if err != nil {
		return "", "", "", err
	}
	digest := sha256.Sum256([]byte(token))
	prefix := token
	if len(prefix) > 12 {
		prefix = prefix[:12]
	}
	key := strings.TrimSpace(input.Key)
	if key == "" {
		key = "key_" + hex.EncodeToString(digest[:8])
	}
	return key, token, hex.EncodeToString(digest[:]), nil
}

func (s *ManagementStore) CreateAPIKey(ctx context.Context, workspaceID, actorID string, input integrationsdk.APIKeyInput) (integrationsdk.APIKeyCredential, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.APIKeyCredential
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.CreateAPIKey(ctx, workspaceID, actorID, input)
			return operationErr
		})
		return value, err
	}
	if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, workspaceID, subjectFenceReference{"subject", "", input.ActorID}); err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	input.ActorID, err = requiredOwnerValue("API key actor ID", input.ActorID)
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	input.RoleKey, err = requiredOwnerValue("API key role key", input.RoleKey)
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	key, token, tokenHash, err := newAPIKeyCredential(input)
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	scopesJSON, err := ownerJSON(input.Scopes, `[]`)
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	now := ownerNow()
	actorID, _ = scopeOwner(ctx, actorID)
	prefix := token
	if len(prefix) > 12 {
		prefix = prefix[:12]
	}
	statement, args, err := query.NewInsertBuilder(s.dialect, "_integration_secrets").Columns(
		"id", "secret_key", "workspace_id", "credential_type", "kind", "status", "name", "description", "value_ref", "fingerprint", "display_prefix", "lookup_hash", "actor_id", "role_key", "scopes_json", "created_by", "created_at", "updated_at", "disabled_at", "expires_at", "rotated_at", "revoked_at", "last_used_at", "last_tested_at", "last_test_status", "last_test_error",
	).Values(ownerID("api_key_", workspaceID, key), key, workspaceID, "api_key", "api_key", "active", input.Name, nil, nil, nil, prefix, tokenHash, input.ActorID, input.RoleKey, scopesJSON, actorID, timestampMillis(now), timestampMillis(now), int64(0), timestampMillis(input.ExpiresAt), int64(0), int64(0), int64(0), int64(0), "", "").Build()
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
		return integrationsdk.APIKeyCredential{}, fmt.Errorf("create Integration API key: %w", err)
	}
	value, err := s.getAPIKey(ctx, workspaceID, key)
	return integrationsdk.APIKeyCredential{APIKey: value, Token: token}, err
}

func (s *ManagementStore) DisableAPIKey(ctx context.Context, workspaceID, key, _ string) (integrationsdk.APIKey, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.APIKey{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.APIKey
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.DisableAPIKey(ctx, workspaceID, key, "")
			return operationErr
		})
		return value, err
	}
	if _, err := s.getAPIKey(ctx, workspaceID, key); err != nil {
		return integrationsdk.APIKey{}, err
	}
	now := ownerNow()
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("credential_type", "api_key"), query.Equal("secret_key", strings.TrimSpace(key)))
	if err != nil {
		return integrationsdk.APIKey{}, err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_secrets").Set("status", "disabled").Set("disabled_at", timestampMillis(now)).Set("updated_at", timestampMillis(now)).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, strings.TrimSpace(workspaceID), "_integration_secrets", ownerID("api_key_", workspaceID, key)), where)).Build()
	if err != nil {
		return integrationsdk.APIKey{}, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return integrationsdk.APIKey{}, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return integrationsdk.APIKey{}, fmt.Errorf("Integration API key %q was not found", key)
	}
	return s.getAPIKey(ctx, workspaceID, key)
}

func (s *ManagementStore) RotateAPIKey(ctx context.Context, workspaceID, key, _ string) (integrationsdk.APIKeyCredential, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.APIKeyCredential
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.RotateAPIKey(ctx, workspaceID, key, "")
			return operationErr
		})
		return value, err
	}
	current, err := s.getAPIKey(ctx, workspaceID, key)
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	_, token, tokenHash, err := newAPIKeyCredential(integrationsdk.APIKeyInput{Key: key})
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	prefix := token
	if len(prefix) > 12 {
		prefix = prefix[:12]
	}
	now := ownerNow()
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("credential_type", "api_key"), query.Equal("secret_key", strings.TrimSpace(key)))
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_secrets").Set("display_prefix", prefix).Set("lookup_hash", tokenHash).Set("status", "active").Set("disabled_at", int64(0)).Set("rotated_at", timestampMillis(now)).Set("updated_at", timestampMillis(now)).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, strings.TrimSpace(workspaceID), "_integration_secrets", ownerID("api_key_", workspaceID, key)), where)).Build()
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	current.TokenPrefix, current.Status, current.DisabledAt, current.UpdatedAt = prefix, "active", "", now
	return integrationsdk.APIKeyCredential{APIKey: current, Token: token}, nil
}
