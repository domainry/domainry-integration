package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	sdk "github.com/domainry/domainry-integration-sdk"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
	integrationservice "github.com/domainry/domainry-integration/internal/domain/integration/service"
	"github.com/domainry/domainry-orm/query"
)

// Called only after account ownership/current action filtering. The bounded
// owner reads deliberately use the connection's workspace and key, not creator
// identity: a shared or administratively provisioned account has a separate owner.
func (s *ManagementStore) enrichAccountReadiness(ctx context.Context, a *sdk.ConnectionAccount) error {
	out := &sdk.ConnectionAccountReadiness{State: "unconfigured"}
	a.Readiness = out
	if a.Status != "active" {
		out.State = a.Status
		return nil
	}
	if s.delivery == nil || s.delivery.providers == nil {
		out.State = "provider_unavailable"
		return nil
	}
	provider, found := s.delivery.providers.Provider(a.ConnectorKey, a.ProviderKey)
	if !found {
		out.State = "provider_unavailable"
		return nil
	}
	trusted := model.WithAccessScope(ctx, model.AccessScope{WorkspaceID: a.WorkspaceID, ActorID: a.OwnerUserID, PermissionKey: "integration.connection_accounts.readiness", Unrestricted: true})
	// ActorID must be nonempty for shared account read scopes. This is owner-local
	// metadata access after authorization, never a new user grant.
	if a.OwnerUserID == "" {
		trusted = model.WithAccessScope(ctx, model.AccessScope{WorkspaceID: a.WorkspaceID, ActorID: "integration", PermissionKey: "integration.connection_accounts.readiness", Unrestricted: true})
	}
	connection, err := s.GetConnection(trusted, a.WorkspaceID, a.Key)
	if err != nil {
		return err
	}
	if connection.Status != "active" || connection.UpdatedAt != a.UpdatedAt {
		out.State = "changed"
		return nil
	}
	for _, field := range provider.Descriptor().SecretFields {
		if field.Required && strings.TrimSpace(connection.SecretRefs[field.Key]) == "" {
			return nil
		}
	}
	// The resolver consumes all configured references, including optional ones.
	for _, ref := range connection.SecretRefs {
		if !strings.HasPrefix(ref, "secret:") {
			return nil
		}
		key := strings.TrimPrefix(ref, "secret:")
		secret, err := s.getSecret(trusted, a.WorkspaceID, key)
		if err != nil {
			return err
		}
		if secret.Status != "active" {
			out.State = "credential_unavailable"
			return nil
		}
		if !secret.Configured || !strings.HasPrefix(secret.ValueRef, "material:") {
			return nil
		}
		if secret.ExpiresAt != "" {
			expiry, err := time.Parse(time.RFC3339, secret.ExpiresAt)
			if err != nil || !expiry.After(time.Now().UTC()) {
				out.State = "credential_expired"
				return nil
			}
		}
		statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_secret_materials").Columns("secret_key").Where(query.And(query.Equal("workspace_id", a.WorkspaceID), query.Equal("secret_key", key))).Limit(1).Build()
		if err != nil {
			return err
		}
		var material string
		if err = s.database.QueryRowContext(ctx, statement, args...).Scan(&material); err == sql.ErrNoRows {
			return nil
		} else if err != nil {
			return err
		}
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connection_grants").Columns("scopes_json").Where(query.And(query.Equal("workspace_id", a.WorkspaceID), query.Equal("connection_key", a.Key))).Limit(1).Build()
	if err != nil {
		return err
	}
	var scopes string
	err = s.database.QueryRowContext(ctx, statement, args...).Scan(&scopes)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == nil && json.Unmarshal([]byte(scopes), &out.GrantedScopes) != nil {
		return fmt.Errorf("Integration connection grant is invalid")
	}
	out.Available, out.State = true, "configured"
	if alternatives, declared := connector.ResolveOAuthConnectionTestScopes(provider); declared {
		state := integrationservice.ConnectionTestScopeState(out.GrantedScopes, err == nil, alternatives)
		out.Test = &sdk.ConnectionAccountTestEligibility{Allowed: state == "ready", State: state}
		if state != "requirements_invalid" {
			out.Test.ScopeAlternatives = alternatives
		}
	}
	return nil
}

// Provisioned in the same transaction as the OAuth account and receipt. Manual
// connection/application configuration cannot manufacture provider-granted scopes.
func (s *ManagementStore) saveConnectionGrant(ctx context.Context, workspace, key string, scopes []string) error {
	raw, err := json.Marshal(scopes)
	if err != nil {
		return err
	}
	statement, args, err := query.NewInsertBuilder(s.dialect, "_integration_connection_grants").Columns("id", "workspace_id", "connection_key", "scopes_json").Values(key, workspace, key, string(raw)).Build()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, statement, args...)
	return err
}
