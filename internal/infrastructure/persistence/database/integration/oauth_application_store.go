package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

func (s *ManagementStore) oauthApplication(ctx context.Context, workspace, key string) (integrationsdk.OAuthApplication, string, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_oauth_applications").Columns("application_json", "client_ciphertext").Where(query.And(query.Equal("workspace_id", workspace), query.Equal("application_key", key))).Limit(1).Build()
	if err != nil {
		return integrationsdk.OAuthApplication{}, "", err
	}
	var raw, ciphertext string
	if err = s.database.QueryRowContext(ctx, statement, args...).Scan(&raw, &ciphertext); err != nil {
		return integrationsdk.OAuthApplication{}, "", fmt.Errorf("Integration OAuth application is unavailable")
	}
	var value integrationsdk.OAuthApplication
	if err = json.Unmarshal([]byte(raw), &value); err != nil {
		return value, "", err
	}
	return value, ciphertext, nil
}
func (s *ManagementStore) ListOAuthApplications(ctx context.Context, workspace string) ([]integrationsdk.OAuthApplication, error) {
	workspace = strings.TrimSpace(workspace)
	if err := requireAllDataScope(ctx, workspace); err != nil {
		return nil, err
	}
	return s.listOAuthApplications(ctx, workspace)
}
func (s *ManagementStore) listOAuthApplications(ctx context.Context, workspace string) ([]integrationsdk.OAuthApplication, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_oauth_applications").Columns("application_json").Where(query.Equal("workspace_id", workspace)).OrderBy(query.Ascending("application_key")).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []integrationsdk.OAuthApplication{}
	for rows.Next() {
		var raw string
		var value integrationsdk.OAuthApplication
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}
func (s *ManagementStore) UpsertOAuthApplication(ctx context.Context, workspace, key, actor string, input integrationsdk.OAuthApplicationInput) (integrationsdk.OAuthApplication, error) {
	workspace, key, actor = strings.TrimSpace(workspace), strings.TrimSpace(key), strings.TrimSpace(actor)
	if err := requireAllDataScope(ctx, workspace); err != nil {
		return integrationsdk.OAuthApplication{}, err
	}
	if workspace == "" || key == "" || actor == "" || s.cipher == nil || s.delivery == nil {
		return integrationsdk.OAuthApplication{}, fmt.Errorf("Integration OAuth application input is incomplete")
	}
	if s.transactions != nil {
		var value integrationsdk.OAuthApplication
		err := s.withTransaction(ctx, func(tx *ManagementStore) error {
			var e error
			value, e = tx.UpsertOAuthApplication(ctx, workspace, key, actor, input)
			return e
		})
		return value, err
	}
	provider, ok := s.delivery.providers.Provider(input.ConnectorKey, input.ProviderKey)
	if !ok {
		return integrationsdk.OAuthApplication{}, fmt.Errorf("Integration OAuth provider is unavailable")
	}
	authorizer, ok := connector.ResolveOAuthAuthorizer(provider)
	if !ok {
		return integrationsdk.OAuthApplication{}, fmt.Errorf("Integration provider does not support initial OAuth")
	}
	connection, err := normalizeProviderConnection(provider, connector.Connection{WorkspaceID: workspace, ConnectorKey: input.ConnectorKey, ProviderKey: input.ProviderKey, Config: input.ConnectionConfig}, false)
	if err != nil {
		return integrationsdk.OAuthApplication{}, err
	}
	sum := sha256.Sum256([]byte(strings.Repeat("v", 43)))
	if _, err = authorizer.AuthorizationURL(connector.OAuthAuthorizationRequest{Connection: connection, ClientID: input.ClientID, RedirectURI: input.RedirectURI, Scopes: input.Scopes, State: strings.Repeat("s", 43), CodeChallenge: base64.RawURLEncoding.EncodeToString(sum[:])}); err != nil {
		return integrationsdk.OAuthApplication{}, fmt.Errorf("Integration OAuth application protocol configuration is invalid")
	}
	where := query.And(query.Equal("workspace_id", workspace), query.Equal("application_key", key))
	lookup, args, err := query.NewSelectBuilder(s.dialect, "_integration_oauth_applications").Columns("updated_at", "client_ciphertext").Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.OAuthApplication{}, err
	}
	var revisionMillis int64
	var ciphertext string
	lookupErr := s.database.QueryRowContext(ctx, lookup, args...).Scan(&revisionMillis, &ciphertext)
	if lookupErr != nil && lookupErr != sql.ErrNoRows {
		return integrationsdk.OAuthApplication{}, lookupErr
	}
	revision := timestampString(revisionMillis)
	if lookupErr == nil && input.ExpectedUpdatedAt != revision || lookupErr == sql.ErrNoRows && input.ExpectedUpdatedAt != "" {
		return integrationsdk.OAuthApplication{}, fmt.Errorf("Integration OAuth application changed")
	}
	if input.ClientSecret != "" {
		ciphertext, err = s.cipher.EncryptSecretMaterial(ctx, workspace, "oauth_application_"+key, input.ClientSecret)
		if err != nil {
			return integrationsdk.OAuthApplication{}, fmt.Errorf("Integration OAuth application secret encryption failed")
		}
	}
	if ciphertext == "" {
		return integrationsdk.OAuthApplication{}, fmt.Errorf("Integration OAuth client secret is required")
	}
	value := integrationsdk.OAuthApplication{Key: key, WorkspaceID: workspace, ConnectorKey: input.ConnectorKey, ProviderKey: input.ProviderKey, Name: strings.TrimSpace(input.Name), ClientID: input.ClientID, RedirectURI: input.RedirectURI, Scopes: append([]string(nil), input.Scopes...), ConnectionConfig: connection.Config, Configured: true, Enabled: input.Enabled, UpdatedAt: ownerNow()}
	raw, _ := json.Marshal(value)
	var statement string
	if lookupErr == sql.ErrNoRows {
		statement, args, err = query.NewInsertBuilder(s.dialect, "_integration_oauth_applications").Columns("id", "workspace_id", "application_key", "application_json", "client_ciphertext", "updated_at").Values(ownerID("oauth_application_", workspace, key), workspace, key, string(raw), ciphertext, timestampMillis(value.UpdatedAt)).Build()
	} else {
		statement, args, err = query.NewUpdateBuilder(s.dialect, "_integration_oauth_applications").Set("application_json", string(raw)).Set("client_ciphertext", ciphertext).Set("updated_at", timestampMillis(value.UpdatedAt)).Where(query.And(where, query.Equal("updated_at", revisionMillis))).Build()
	}
	if err != nil {
		return integrationsdk.OAuthApplication{}, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return integrationsdk.OAuthApplication{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return integrationsdk.OAuthApplication{}, fmt.Errorf("Integration OAuth application changed")
	}
	return value, nil
}
func (s *ManagementStore) ListOAuthAuthorizationOptions(ctx context.Context, subject integrationsdk.ConnectionAccountSubject) ([]integrationsdk.OAuthAuthorizationOption, error) {
	subject, err := normalizedConnectionAccountSubject(subject)
	if err != nil {
		return nil, err
	}
	if err = requireConnectionAccountSubjectScope(ctx, subject); err != nil {
		return nil, err
	}
	result := []integrationsdk.OAuthAuthorizationOption{}
	if !subject.Access.Personal && !subject.Access.Workspace {
		return result, nil
	}
	values, err := s.listOAuthApplications(ctx, subject.WorkspaceID)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value.Enabled && value.Configured {
			result = append(result, integrationsdk.OAuthAuthorizationOption{Key: value.Key, ConnectorKey: value.ConnectorKey, ProviderKey: value.ProviderKey, Name: value.Name, Scopes: value.Scopes})
		}
	}
	return result, nil
}

var _ integrationsdk.OAuthApplications = (*ManagementStore)(nil)
