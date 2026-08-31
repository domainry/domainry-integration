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
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_api_keys").Columns(
		"api_key", "workspace_id", "name", "token_prefix", "actor_id", "role_key", "scopes_json", "status", "expires_at", "last_used_at", "created_by", "created_at", "updated_at", "disabled_at",
	).Where(query.Equal("workspace_id", workspaceID)).OrderBy(query.Ascending("api_key")).Build()
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
	var name, expiresAt, lastUsedAt, createdBy, disabledAt sql.NullString
	var scopesJSON string
	if err := row.Scan(&value.Key, &value.WorkspaceID, &name, &value.TokenPrefix, &value.ActorID, &value.RoleKey, &scopesJSON, &value.Status, &expiresAt, &lastUsedAt, &createdBy, &value.CreatedAt, &value.UpdatedAt, &disabledAt); err != nil {
		return value, err
	}
	value.Name, value.ExpiresAt, value.LastUsedAt, value.CreatedBy, value.DisabledAt = name.String, expiresAt.String, lastUsedAt.String, createdBy.String, disabledAt.String
	if err := json.Unmarshal([]byte(scopesJSON), &value.Scopes); err != nil {
		return value, fmt.Errorf("decode Integration API key scopes: %w", err)
	}
	return value, nil
}

func (s *ManagementStore) getAPIKey(ctx context.Context, workspaceID, key string) (integrationsdk.APIKey, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_api_keys").Columns(
		"api_key", "workspace_id", "name", "token_prefix", "actor_id", "role_key", "scopes_json", "status", "expires_at", "last_used_at", "created_by", "created_at", "updated_at", "disabled_at",
	).Where(query.And(query.Equal("workspace_id", strings.TrimSpace(workspaceID)), query.Equal("api_key", strings.TrimSpace(key)))).Limit(1).Build()
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
	prefix := token
	if len(prefix) > 12 {
		prefix = prefix[:12]
	}
	statement, args, err := query.NewInsertBuilder(s.dialect, "_integration_api_keys").Columns(
		"id", "api_key", "workspace_id", "name", "token_prefix", "token_hash", "actor_id", "role_key", "scopes_json", "status", "expires_at", "last_used_at", "created_by", "created_at", "updated_at", "disabled_at",
	).Values(ownerID("api_key_", workspaceID, key), key, workspaceID, input.Name, prefix, tokenHash, input.ActorID, input.RoleKey, scopesJSON, "active", input.ExpiresAt, "", actorID, now, now, "").Build()
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
	now := ownerNow()
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_api_keys").Set("status", "disabled").Set("disabled_at", now).Set("updated_at", now).Where(query.And(query.Equal("workspace_id", strings.TrimSpace(workspaceID)), query.Equal("api_key", strings.TrimSpace(key)))).Build()
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
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_api_keys").Set("token_prefix", prefix).Set("token_hash", tokenHash).Set("status", "active").Set("disabled_at", "").Set("updated_at", now).Where(query.And(query.Equal("workspace_id", strings.TrimSpace(workspaceID)), query.Equal("api_key", strings.TrimSpace(key)))).Build()
	if err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
		return integrationsdk.APIKeyCredential{}, err
	}
	current.TokenPrefix, current.Status, current.DisabledAt, current.UpdatedAt = prefix, "active", "", now
	return integrationsdk.APIKeyCredential{APIKey: current, Token: token}, nil
}
