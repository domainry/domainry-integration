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

	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

type WebPushSubscriptionStore struct {
	database modulehost.Database
	dialect  modulehost.Dialect
}

func NewWebPushSubscriptionStore(database modulehost.Database, dialect modulehost.Dialect) *WebPushSubscriptionStore {
	return &WebPushSubscriptionStore{database: database, dialect: dialect}
}

func (s *WebPushSubscriptionStore) Readiness(ctx context.Context, workspaceID string) (integrationmodel.WebPushReadiness, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return integrationmodel.WebPushReadiness{}, fmt.Errorf("Integration Web Push workspace is required")
	}
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_integration_connections").
		Columns("connection_key", "status", "config_json", "secret_refs_json").
		Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("connector_key", "notification"), query.Equal("provider_key", "web_push"))).
		OrderBy(query.Ascending("connection_key")).Limit(1).Build()
	if err != nil {
		return integrationmodel.WebPushReadiness{}, fmt.Errorf("build Integration Web Push readiness: %w", err)
	}
	var connectionKey, status, configJSON, secretRefsJSON string
	if err := s.database.QueryRowContext(ctx, queryValue, args...).Scan(&connectionKey, &status, &configJSON, &secretRefsJSON); err == sql.ErrNoRows {
		return integrationmodel.WebPushReadiness{Status: "unconfigured", Reason: "connection_missing"}, nil
	} else if err != nil {
		return integrationmodel.WebPushReadiness{}, fmt.Errorf("read Integration Web Push connection: %w", err)
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return integrationmodel.WebPushReadiness{}, fmt.Errorf("decode Integration Web Push configuration: %w", err)
	}
	publicKey := strings.TrimSpace(fmt.Sprint(config["vapid_public_key"]))
	result := integrationmodel.WebPushReadiness{PublicKey: publicKey, ConnectionKey: connectionKey, Status: status}
	if publicKey == "" {
		result.Reason = "public_key_missing"
		return result, nil
	}
	var secretRefs map[string]string
	if err := json.Unmarshal([]byte(secretRefsJSON), &secretRefs); err != nil {
		return integrationmodel.WebPushReadiness{}, fmt.Errorf("decode Integration Web Push secret references: %w", err)
	}
	secretKey := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(secretRefs["vapid_private_key"]), "secret:"))
	if secretKey == "" || secretKey == strings.TrimSpace(secretRefs["vapid_private_key"]) {
		result.Reason = "private_key_unbound"
		return result, nil
	}
	secretQuery, secretArgs, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns("status").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", secretKey))).Limit(1).Build()
	if err != nil {
		return integrationmodel.WebPushReadiness{}, fmt.Errorf("build Integration Web Push secret readiness: %w", err)
	}
	var secretStatus string
	if err := s.database.QueryRowContext(ctx, secretQuery, secretArgs...).Scan(&secretStatus); err == sql.ErrNoRows || strings.TrimSpace(secretStatus) != "active" {
		result.Reason = "private_key_unavailable"
		return result, nil
	} else if err != nil {
		return integrationmodel.WebPushReadiness{}, fmt.Errorf("read Integration Web Push secret readiness: %w", err)
	}
	if status != "active" && status != "verified" {
		result.Reason = "connection_not_ready"
		return result, nil
	}
	result.Ready = true
	return result, nil
}

func (s *WebPushSubscriptionStore) List(ctx context.Context, workspaceID, userID string) ([]integrationmodel.WebPushSubscription, error) {
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_integration_web_push_subscriptions").
		Columns("id", "workspace_id", "user_id", "endpoint_hash", "status", "expires_at", "created_at", "updated_at", "revoked_at").
		Where(query.And(query.Equal("workspace_id", strings.TrimSpace(workspaceID)), query.Equal("user_id", strings.TrimSpace(userID)))).OrderBy(query.Descending("created_at")).Build()
	if err != nil {
		return nil, fmt.Errorf("build Web Push subscription list: %w", err)
	}
	rows, err := s.database.QueryContext(ctx, queryValue, args...)
	if err != nil {
		return nil, fmt.Errorf("list Web Push subscriptions: %w", err)
	}
	defer rows.Close()
	values := []integrationmodel.WebPushSubscription{}
	for rows.Next() {
		var value integrationmodel.WebPushSubscription
		if err := rows.Scan(&value.ID, &value.WorkspaceID, &value.UserID, &value.EndpointHash, &value.Status, &value.ExpiresAt, &value.CreatedAt, &value.UpdatedAt, &value.RevokedAt); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *WebPushSubscriptionStore) Upsert(ctx context.Context, workspaceID, userID, id string, input integrationmodel.WebPushSubscriptionInput) (integrationmodel.WebPushSubscription, error) {
	workspaceID, userID, id = strings.TrimSpace(workspaceID), strings.TrimSpace(userID), strings.TrimSpace(id)
	endpoint := strings.TrimSpace(input.Endpoint)
	if workspaceID == "" || userID == "" || id == "" || len(id) > 200 || !strings.HasPrefix(endpoint, "https://") || strings.TrimSpace(input.P256DH) == "" || strings.TrimSpace(input.Auth) == "" {
		return integrationmodel.WebPushSubscription{}, fmt.Errorf("Integration Web Push subscription is invalid")
	}
	if input.ExpiresAt != "" {
		if expires, err := time.Parse(time.RFC3339, input.ExpiresAt); err != nil || !expires.After(time.Now().UTC()) {
			return integrationmodel.WebPushSubscription{}, fmt.Errorf("Integration Web Push subscription expiry is invalid")
		}
	}
	hash := sha256.Sum256([]byte(endpoint))
	endpointHash := hex.EncodeToString(hash[:])
	now := time.Now().UTC().Format(time.RFC3339)
	existing, found, err := s.material(ctx, workspaceID, id)
	if err != nil {
		return integrationmodel.WebPushSubscription{}, err
	}
	if found && existing.UserID != userID {
		return integrationmodel.WebPushSubscription{}, fmt.Errorf("Integration Web Push subscription was not found")
	}
	if found {
		statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_web_push_subscriptions").Set("endpoint_hash", endpointHash).Set("endpoint", endpoint).Set("p256dh", strings.TrimSpace(input.P256DH)).Set("auth_secret", strings.TrimSpace(input.Auth)).Set("status", "active").Set("expires_at", input.ExpiresAt).Set("updated_at", now).Set("revoked_at", "").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("id", id), query.Equal("user_id", userID))).Build()
		if err != nil {
			return integrationmodel.WebPushSubscription{}, err
		}
		if _, err = s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationmodel.WebPushSubscription{}, err
		}
	} else {
		statement, args, err := query.NewInsertBuilder(s.dialect, "_integration_web_push_subscriptions").Columns("id", "workspace_id", "user_id", "endpoint_hash", "endpoint", "p256dh", "auth_secret", "status", "expires_at", "created_at", "updated_at", "revoked_at").Values(id, workspaceID, userID, endpointHash, endpoint, strings.TrimSpace(input.P256DH), strings.TrimSpace(input.Auth), "active", input.ExpiresAt, now, now, "").Build()
		if err != nil {
			return integrationmodel.WebPushSubscription{}, err
		}
		if _, err = s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationmodel.WebPushSubscription{}, err
		}
	}
	value, _, err := s.material(ctx, workspaceID, id)
	return value.public(), err
}

func (s *WebPushSubscriptionStore) Revoke(ctx context.Context, workspaceID, userID, id string) (integrationmodel.WebPushSubscription, error) {
	value, found, err := s.material(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(id))
	if err != nil {
		return integrationmodel.WebPushSubscription{}, err
	}
	if !found || value.UserID != strings.TrimSpace(userID) {
		return integrationmodel.WebPushSubscription{}, fmt.Errorf("Integration Web Push subscription was not found")
	}
	if value.Status == "revoked" {
		return value.public(), nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_web_push_subscriptions").Set("endpoint", "").Set("p256dh", "").Set("auth_secret", "").Set("status", "revoked").Set("updated_at", now).Set("revoked_at", now).Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("id", id), query.Equal("user_id", userID))).Build()
	if err != nil {
		return integrationmodel.WebPushSubscription{}, err
	}
	if _, err = s.database.ExecContext(ctx, statement, args...); err != nil {
		return integrationmodel.WebPushSubscription{}, err
	}
	value, _, err = s.material(ctx, workspaceID, id)
	return value.public(), err
}

func (s *WebPushSubscriptionStore) CleanupExpired(ctx context.Context, workspaceID string) (int, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_web_push_subscriptions").Set("endpoint", "").Set("p256dh", "").Set("auth_secret", "").Set("status", "expired").Set("updated_at", now).Where(query.And(query.Equal("workspace_id", strings.TrimSpace(workspaceID)), query.Equal("status", "active"), query.NotEqual("expires_at", ""), query.LessThan("expires_at", now))).Build()
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
	integrationmodel.WebPushSubscription
	Endpoint, P256DH, Auth string
}

func (v webPushMaterial) public() integrationmodel.WebPushSubscription { return v.WebPushSubscription }
func (s *WebPushSubscriptionStore) material(ctx context.Context, workspaceID, id string) (webPushMaterial, bool, error) {
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_integration_web_push_subscriptions").Columns("id", "workspace_id", "user_id", "endpoint_hash", "endpoint", "p256dh", "auth_secret", "status", "expires_at", "created_at", "updated_at", "revoked_at").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("id", id))).Limit(1).Build()
	if err != nil {
		return webPushMaterial{}, false, err
	}
	var v webPushMaterial
	err = s.database.QueryRowContext(ctx, queryValue, args...).Scan(&v.ID, &v.WorkspaceID, &v.UserID, &v.EndpointHash, &v.Endpoint, &v.P256DH, &v.Auth, &v.Status, &v.ExpiresAt, &v.CreatedAt, &v.UpdatedAt, &v.RevokedAt)
	if err == sql.ErrNoRows {
		return webPushMaterial{}, false, nil
	}
	return v, err == nil, err
}
