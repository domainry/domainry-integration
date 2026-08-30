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

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormbuilder "github.com/domainry/domainry-orm/query"
)

type WebPushSubscriptionStore struct {
	database modulehost.Database
	dialect  modulehost.Dialect
}

func NewWebPushSubscriptionStore(database modulehost.Database, dialect modulehost.Dialect) *WebPushSubscriptionStore {
	return &WebPushSubscriptionStore{database: database, dialect: dialect}
}

func (s *WebPushSubscriptionStore) Readiness(ctx context.Context, workspaceID string) (integrationsdk.WebPushReadiness, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return integrationsdk.WebPushReadiness{}, fmt.Errorf("Integration Web Push workspace is required")
	}
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "_integration_connections").
		Columns("connection_key", "status", "config_json", "secret_refs_json").
		Where(ormbuilder.And(ormbuilder.Equal("workspace_id", workspaceID), ormbuilder.Equal("connector_key", "notification"), ormbuilder.Equal("provider_key", "web_push"))).
		OrderBy(ormbuilder.Ascending("connection_key")).Limit(1).Build()
	if err != nil {
		return integrationsdk.WebPushReadiness{}, fmt.Errorf("build Integration Web Push readiness: %w", err)
	}
	var connectionKey, status, configJSON, secretRefsJSON string
	if err := s.database.QueryRowContext(ctx, query, args...).Scan(&connectionKey, &status, &configJSON, &secretRefsJSON); err == sql.ErrNoRows {
		return integrationsdk.WebPushReadiness{Status: "unconfigured", Reason: "connection_missing"}, nil
	} else if err != nil {
		return integrationsdk.WebPushReadiness{}, fmt.Errorf("read Integration Web Push connection: %w", err)
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return integrationsdk.WebPushReadiness{}, fmt.Errorf("decode Integration Web Push configuration: %w", err)
	}
	publicKey := strings.TrimSpace(fmt.Sprint(config["vapid_public_key"]))
	result := integrationsdk.WebPushReadiness{PublicKey: publicKey, ConnectionKey: connectionKey, Status: status}
	if publicKey == "" {
		result.Reason = "public_key_missing"
		return result, nil
	}
	var secretRefs map[string]string
	if err := json.Unmarshal([]byte(secretRefsJSON), &secretRefs); err != nil {
		return integrationsdk.WebPushReadiness{}, fmt.Errorf("decode Integration Web Push secret references: %w", err)
	}
	secretKey := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(secretRefs["vapid_private_key"]), "secret:"))
	if secretKey == "" || secretKey == strings.TrimSpace(secretRefs["vapid_private_key"]) {
		result.Reason = "private_key_unbound"
		return result, nil
	}
	secretQuery, secretArgs, err := ormbuilder.NewSelectBuilder(s.dialect, "_integration_secrets").Columns("status").Where(ormbuilder.And(ormbuilder.Equal("workspace_id", workspaceID), ormbuilder.Equal("secret_key", secretKey))).Limit(1).Build()
	if err != nil {
		return integrationsdk.WebPushReadiness{}, fmt.Errorf("build Integration Web Push secret readiness: %w", err)
	}
	var secretStatus string
	if err := s.database.QueryRowContext(ctx, secretQuery, secretArgs...).Scan(&secretStatus); err == sql.ErrNoRows || strings.TrimSpace(secretStatus) != "active" {
		result.Reason = "private_key_unavailable"
		return result, nil
	} else if err != nil {
		return integrationsdk.WebPushReadiness{}, fmt.Errorf("read Integration Web Push secret readiness: %w", err)
	}
	if status != "active" && status != "verified" {
		result.Reason = "connection_not_ready"
		return result, nil
	}
	result.Ready = true
	return result, nil
}

func (s *WebPushSubscriptionStore) List(ctx context.Context, workspaceID, userID string) ([]integrationsdk.WebPushSubscription, error) {
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "_integration_web_push_subscriptions").
		Columns("id", "workspace_id", "user_id", "endpoint_hash", "status", "expires_at", "created_at", "updated_at", "revoked_at").
		Where(ormbuilder.And(ormbuilder.Equal("workspace_id", strings.TrimSpace(workspaceID)), ormbuilder.Equal("user_id", strings.TrimSpace(userID)))).OrderBy(ormbuilder.Descending("created_at")).Build()
	if err != nil {
		return nil, fmt.Errorf("build Web Push subscription list: %w", err)
	}
	rows, err := s.database.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list Web Push subscriptions: %w", err)
	}
	defer rows.Close()
	values := []integrationsdk.WebPushSubscription{}
	for rows.Next() {
		var value integrationsdk.WebPushSubscription
		if err := rows.Scan(&value.ID, &value.WorkspaceID, &value.UserID, &value.EndpointHash, &value.Status, &value.ExpiresAt, &value.CreatedAt, &value.UpdatedAt, &value.RevokedAt); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *WebPushSubscriptionStore) Upsert(ctx context.Context, workspaceID, userID, id string, input integrationsdk.WebPushSubscriptionInput) (integrationsdk.WebPushSubscription, error) {
	workspaceID, userID, id = strings.TrimSpace(workspaceID), strings.TrimSpace(userID), strings.TrimSpace(id)
	endpoint := strings.TrimSpace(input.Endpoint)
	if workspaceID == "" || userID == "" || id == "" || len(id) > 200 || !strings.HasPrefix(endpoint, "https://") || strings.TrimSpace(input.P256DH) == "" || strings.TrimSpace(input.Auth) == "" {
		return integrationsdk.WebPushSubscription{}, fmt.Errorf("Integration Web Push subscription is invalid")
	}
	if input.ExpiresAt != "" {
		if expires, err := time.Parse(time.RFC3339, input.ExpiresAt); err != nil || !expires.After(time.Now().UTC()) {
			return integrationsdk.WebPushSubscription{}, fmt.Errorf("Integration Web Push subscription expiry is invalid")
		}
	}
	hash := sha256.Sum256([]byte(endpoint))
	endpointHash := hex.EncodeToString(hash[:])
	now := time.Now().UTC().Format(time.RFC3339)
	existing, found, err := s.material(ctx, workspaceID, id)
	if err != nil {
		return integrationsdk.WebPushSubscription{}, err
	}
	if found && existing.UserID != userID {
		return integrationsdk.WebPushSubscription{}, fmt.Errorf("Integration Web Push subscription was not found")
	}
	if found {
		statement, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "_integration_web_push_subscriptions").Set("endpoint_hash", endpointHash).Set("endpoint", endpoint).Set("p256dh", strings.TrimSpace(input.P256DH)).Set("auth_secret", strings.TrimSpace(input.Auth)).Set("status", "active").Set("expires_at", input.ExpiresAt).Set("updated_at", now).Set("revoked_at", "").Where(ormbuilder.And(ormbuilder.Equal("workspace_id", workspaceID), ormbuilder.Equal("id", id), ormbuilder.Equal("user_id", userID))).Build()
		if err != nil {
			return integrationsdk.WebPushSubscription{}, err
		}
		if _, err = s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationsdk.WebPushSubscription{}, err
		}
	} else {
		statement, args, err := ormbuilder.NewInsertBuilder(s.dialect, "_integration_web_push_subscriptions").Columns("id", "workspace_id", "user_id", "endpoint_hash", "endpoint", "p256dh", "auth_secret", "status", "expires_at", "created_at", "updated_at", "revoked_at").Values(id, workspaceID, userID, endpointHash, endpoint, strings.TrimSpace(input.P256DH), strings.TrimSpace(input.Auth), "active", input.ExpiresAt, now, now, "").Build()
		if err != nil {
			return integrationsdk.WebPushSubscription{}, err
		}
		if _, err = s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationsdk.WebPushSubscription{}, err
		}
	}
	value, _, err := s.material(ctx, workspaceID, id)
	return value.public(), err
}

func (s *WebPushSubscriptionStore) Revoke(ctx context.Context, workspaceID, userID, id string) (integrationsdk.WebPushSubscription, error) {
	value, found, err := s.material(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(id))
	if err != nil {
		return integrationsdk.WebPushSubscription{}, err
	}
	if !found || value.UserID != strings.TrimSpace(userID) {
		return integrationsdk.WebPushSubscription{}, fmt.Errorf("Integration Web Push subscription was not found")
	}
	if value.Status == "revoked" {
		return value.public(), nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	statement, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "_integration_web_push_subscriptions").Set("endpoint", "").Set("p256dh", "").Set("auth_secret", "").Set("status", "revoked").Set("updated_at", now).Set("revoked_at", now).Where(ormbuilder.And(ormbuilder.Equal("workspace_id", workspaceID), ormbuilder.Equal("id", id), ormbuilder.Equal("user_id", userID))).Build()
	if err != nil {
		return integrationsdk.WebPushSubscription{}, err
	}
	if _, err = s.database.ExecContext(ctx, statement, args...); err != nil {
		return integrationsdk.WebPushSubscription{}, err
	}
	value, _, err = s.material(ctx, workspaceID, id)
	return value.public(), err
}

func (s *WebPushSubscriptionStore) CleanupExpired(ctx context.Context, workspaceID string) (int, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	statement, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "_integration_web_push_subscriptions").Set("endpoint", "").Set("p256dh", "").Set("auth_secret", "").Set("status", "expired").Set("updated_at", now).Where(ormbuilder.And(ormbuilder.Equal("workspace_id", strings.TrimSpace(workspaceID)), ormbuilder.Equal("status", "active"), ormbuilder.NotEqual("expires_at", ""), ormbuilder.LessThan("expires_at", now))).Build()
	if err != nil {
		return 0, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	return int(count), nil
}

type webPushMaterial struct {
	integrationsdk.WebPushSubscription
	Endpoint, P256DH, Auth string
}

func (v webPushMaterial) public() integrationsdk.WebPushSubscription { return v.WebPushSubscription }
func (s *WebPushSubscriptionStore) material(ctx context.Context, workspaceID, id string) (webPushMaterial, bool, error) {
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "_integration_web_push_subscriptions").Columns("id", "workspace_id", "user_id", "endpoint_hash", "endpoint", "p256dh", "auth_secret", "status", "expires_at", "created_at", "updated_at", "revoked_at").Where(ormbuilder.And(ormbuilder.Equal("workspace_id", workspaceID), ormbuilder.Equal("id", id))).Limit(1).Build()
	if err != nil {
		return webPushMaterial{}, false, err
	}
	var v webPushMaterial
	err = s.database.QueryRowContext(ctx, query, args...).Scan(&v.ID, &v.WorkspaceID, &v.UserID, &v.EndpointHash, &v.Endpoint, &v.P256DH, &v.Auth, &v.Status, &v.ExpiresAt, &v.CreatedAt, &v.UpdatedAt, &v.RevokedAt)
	if err == sql.ErrNoRows {
		return webPushMaterial{}, false, nil
	}
	return v, err == nil, err
}

var _ integrationsdk.WebPushSubscriptions = (*WebPushSubscriptionStore)(nil)
