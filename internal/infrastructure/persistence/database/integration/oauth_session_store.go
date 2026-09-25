package integration

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

type oauthSessionRecord struct {
	Session            integrationsdk.OAuthAuthorizationSession `json:"session"`
	Name               string                                   `json:"name"`
	Revision           string                                   `json:"-"`
	VerifierCiphertext string                                   `json:"-"`
	ExchangeDeadline   string                                   `json:"-"`
}

func marshalOAuthSession(record oauthSessionRecord) ([]byte, error) {
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	var envelope map[string]json.RawMessage
	if err = json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	var session map[string]json.RawMessage
	if err = json.Unmarshal(envelope["session"], &session); err != nil {
		return nil, err
	}
	// expires_at is canonically persisted by the numeric expires_at column.
	// Keeping the SDK's RFC3339 projection in session_json would create a second,
	// string-encoded source of truth for the same instant.
	delete(session, "expires_at")
	envelope["session"], err = json.Marshal(session)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope)
}

func oauthHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func oauthScopeAllowed(subject integrationsdk.ConnectionAccountSubject, scope integrationsdk.ConnectionAccountScope) bool {
	return scope == integrationsdk.ConnectionAccountScopePersonal && subject.Access.Personal || scope == integrationsdk.ConnectionAccountScopeWorkspace && subject.Access.Workspace
}
func (s *ManagementStore) readOAuthSession(ctx context.Context, subject integrationsdk.ConnectionAccountSubject, where query.Predicate) (oauthSessionRecord, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_oauth_sessions").Columns("session_json", "status", "application_revision", "verifier_ciphertext", "expires_at", "exchange_deadline").Where(query.And(query.Equal("workspace_id", subject.WorkspaceID), query.Equal("user_id", subject.UserID), where)).Limit(1).Build()
	if err != nil {
		return oauthSessionRecord{}, err
	}
	var raw, status, ciphertext string
	var revision, expiresAt, deadline int64
	if err = s.database.QueryRowContext(ctx, statement, args...).Scan(&raw, &status, &revision, &ciphertext, &expiresAt, &deadline); err != nil {
		return oauthSessionRecord{}, fmt.Errorf("Integration OAuth session is unavailable")
	}
	var value oauthSessionRecord
	if err = json.Unmarshal([]byte(raw), &value); err != nil {
		return value, err
	}
	value.Session.Status, value.Session.ExpiresAt, value.Revision, value.VerifierCiphertext, value.ExchangeDeadline = status, timestampString(expiresAt), timestampString(revision), ciphertext, timestampString(deadline)
	if !oauthScopeAllowed(subject, value.Session.Scope) {
		return oauthSessionRecord{}, fmt.Errorf("Integration OAuth session is unavailable")
	}
	return value, nil
}
func (s *ManagementStore) StartOAuthAuthorization(ctx context.Context, subject integrationsdk.ConnectionAccountSubject, input integrationsdk.OAuthAuthorizationInput) (integrationsdk.OAuthAuthorizationSession, error) {
	if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, subject.WorkspaceID, subjectFenceReference{"subject", "", subject.UserID}); err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	subject, err := normalizedConnectionAccountSubject(subject)
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	if err = requireConnectionAccountSubjectScope(ctx, subject); err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	if !oauthScopeAllowed(subject, input.Scope) {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth account scope is not authorized")
	}
	app, _, err := s.oauthApplication(ctx, subject.WorkspaceID, strings.TrimSpace(input.ApplicationKey))
	if err != nil || !app.Enabled {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth application is unavailable")
	}
	if len(input.Scopes) == 0 || len(input.Scopes) > len(app.Scopes) {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth scopes are required")
	}
	allowed := map[string]bool{}
	for _, scope := range app.Scopes {
		allowed[scope] = true
	}
	for _, scope := range input.Scopes {
		if !allowed[scope] {
			return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth scope is not configured")
		}
		delete(allowed, scope)
	}
	provider, ok := s.delivery.providers.Provider(app.ConnectorKey, app.ProviderKey)
	if !ok {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth provider is unavailable")
	}
	authorizer, ok := connector.ResolveOAuthAuthorizer(provider)
	if !ok {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth provider is unavailable")
	}
	id, err := randomOwnerToken("oauth_")
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	state, err := randomOwnerToken("")
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	verifier, err := randomOwnerToken("")
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	sum := sha256.Sum256([]byte(verifier))
	target, err := authorizer.AuthorizationURL(connector.OAuthAuthorizationRequest{Connection: connector.Connection{ConnectorKey: app.ConnectorKey, ProviderKey: app.ProviderKey, WorkspaceID: subject.WorkspaceID, Config: app.ConnectionConfig}, ClientID: app.ClientID, RedirectURI: app.RedirectURI, Scopes: input.Scopes, State: state, CodeChallenge: base64.RawURLEncoding.EncodeToString(sum[:])})
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth authorization configuration is invalid")
	}
	ciphertext, err := s.cipher.EncryptSecretMaterial(ctx, subject.WorkspaceID, id, verifier)
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth session encryption failed")
	}
	value := oauthSessionRecord{Session: integrationsdk.OAuthAuthorizationSession{ID: id, Status: "pending", ApplicationKey: app.Key, Scope: input.Scope, RequestedScopes: append([]string(nil), input.Scopes...), ExpiresAt: time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339Nano)}, Name: strings.TrimSpace(input.Name)}
	raw, err := marshalOAuthSession(value)
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	now := ownerNow()
	statement, args, err := query.NewInsertBuilder(s.dialect, "_integration_oauth_sessions").Columns("id", "workspace_id", "user_id", "application_key", "application_revision", "state_hash", "status", "session_json", "verifier_ciphertext", "expires_at", "exchange_deadline", "created_at", "updated_at").Values(id, subject.WorkspaceID, subject.UserID, app.Key, timestampMillis(app.UpdatedAt), oauthHash(state), "pending", string(raw), ciphertext, timestampMillis(value.Session.ExpiresAt), int64(0), timestampMillis(now), timestampMillis(now)).Build()
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	if _, err = s.database.ExecContext(ctx, statement, args...); err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	value.Session.AuthorizationURL = target
	return value.Session, nil
}
func (s *ManagementStore) updateOAuthSession(ctx context.Context, subject integrationsdk.ConnectionAccountSubject, record oauthSessionRecord, previous string) error {
	raw, err := marshalOAuthSession(record)
	if err != nil {
		return err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_oauth_sessions").Set("status", record.Session.Status).Set("session_json", string(raw)).Set("verifier_ciphertext", "").Set("exchange_deadline", timestampMillis(record.ExchangeDeadline)).Set("updated_at", timestampMillis(ownerNow())).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, subject.WorkspaceID, "_integration_oauth_sessions", record.Session.ID), query.And(query.Equal("workspace_id", subject.WorkspaceID), query.Equal("user_id", subject.UserID), query.Equal("id", record.Session.ID), query.Equal("status", previous)))).Build()
	if err != nil {
		return err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return fmt.Errorf("Integration OAuth session changed")
	}
	return nil
}
func (s *ManagementStore) GetOAuthAuthorization(ctx context.Context, subject integrationsdk.ConnectionAccountSubject, id string) (integrationsdk.OAuthAuthorizationSession, error) {
	subject, err := normalizedConnectionAccountSubject(subject)
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	if err = requireConnectionAccountSubjectScope(ctx, subject); err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	record, err := s.readOAuthSession(ctx, subject, query.Equal("id", strings.TrimSpace(id)))
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	previous := record.Session.Status
	expires, err := time.Parse(time.RFC3339Nano, record.Session.ExpiresAt)
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	if previous == "pending" && !expires.After(time.Now().UTC()) {
		record.Session.Status = "expired"
	}
	if previous == "exchanging" {
		deadline, e := time.Parse(time.RFC3339Nano, record.ExchangeDeadline)
		if e != nil {
			return integrationsdk.OAuthAuthorizationSession{}, e
		}
		if !deadline.After(time.Now().UTC()) {
			record.Session.Status = "needs_reauthorization"
		}
	}
	if previous != record.Session.Status {
		if err = s.updateOAuthSession(ctx, subject, record, previous); err != nil {
			return integrationsdk.OAuthAuthorizationSession{}, err
		}
	}
	if record.Session.Account != nil {
		account, e := s.GetConnectionAccount(ctx, subject, record.Session.Account.Key)
		if e != nil {
			return integrationsdk.OAuthAuthorizationSession{}, e
		}
		record.Session.Account = &account
	}
	return record.Session, nil
}
func (s *ManagementStore) CompleteOAuthAuthorization(ctx context.Context, subject integrationsdk.ConnectionAccountSubject, input integrationsdk.OAuthAuthorizationCallback) (integrationsdk.OAuthAuthorizationSession, error) {
	subject, err := normalizedConnectionAccountSubject(subject)
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	if err = requireConnectionAccountSubjectScope(ctx, subject); err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	if len(input.State) < 32 || len(input.State) > 512 || input.Code == "" && input.Error == "" || input.Code != "" && input.Error != "" {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth callback is invalid")
	}
	record, err := s.readOAuthSession(ctx, subject, query.Equal("state_hash", oauthHash(input.State)))
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	session, err := s.GetOAuthAuthorization(ctx, subject, record.Session.ID)
	if err != nil {
		return session, err
	}
	if session.Status != "pending" {
		return session, nil
	}
	app, ciphertext, err := s.oauthApplication(ctx, subject.WorkspaceID, record.Session.ApplicationKey)
	if err != nil || !app.Enabled || app.UpdatedAt != record.Revision {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth application changed; restart authorization")
	}
	if input.Error != "" {
		record.Session.Status = "rejected"
		err = s.updateOAuthSession(ctx, subject, record, "pending")
		return record.Session, err
	}
	provider, ok := s.delivery.providers.Provider(app.ConnectorKey, app.ProviderKey)
	if !ok {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth provider is unavailable")
	}
	authorizer, ok := connector.ResolveOAuthAuthorizer(provider)
	if !ok {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth provider is unavailable")
	}
	secret, err := s.cipher.DecryptSecretMaterial(ctx, subject.WorkspaceID, "oauth_application_"+app.Key, ciphertext)
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth application credentials are unavailable")
	}
	verifier, err := s.cipher.DecryptSecretMaterial(ctx, subject.WorkspaceID, record.Session.ID, record.VerifierCiphertext)
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth verifier is unavailable")
	}
	record.Session.Status = "exchanging"
	record.ExchangeDeadline = time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	if err = s.updateOAuthSession(ctx, subject, record, "pending"); err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, err
	}
	// A code is single-use. Once claimed, bounded completion may outlive a closed
	// browser so successful credentials can still be persisted. Never retry this I/O.
	completion, cancel := context.WithTimeout(context.WithoutCancel(ctx), 45*time.Second)
	defer cancel()
	tokens, exchangeErr := authorizer.ExchangeAuthorizationCode(completion, connector.OAuthCodeExchangeRequest{Connection: connector.Connection{ConnectorKey: app.ConnectorKey, ProviderKey: app.ProviderKey, WorkspaceID: subject.WorkspaceID, Config: app.ConnectionConfig}, ClientID: app.ClientID, ClientSecret: secret, RedirectURI: app.RedirectURI, RequestedScopes: record.Session.RequestedScopes, Code: input.Code, CodeVerifier: verifier})
	if exchangeErr != nil {
		record.Session.Status = "needs_reauthorization"
		if classification, _ := connector.ErrorClassificationOf(exchangeErr); classification == connector.ErrorPermanent {
			record.Session.Status = "rejected"
		}
		if err = s.updateOAuthSession(completion, subject, record, "exchanging"); err != nil {
			return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth result could not be persisted")
		}
		return record.Session, nil
	}
	if tokens.AccessToken == "" || tokens.RefreshToken == "" || tokens.TokenType != "Bearer" {
		record.Session.Status = "needs_reauthorization"
		err = s.updateOAuthSession(completion, subject, record, "exchanging")
		return record.Session, err
	}
	// Provision a distinct connection and four independently encrypted materials.
	// There is no credential sharing with the administrative OAuth application.
	err = s.withTransaction(completion, func(tx *ManagementStore) error {
		current, e := tx.readOAuthSession(completion, subject, query.Equal("id", record.Session.ID))
		if e != nil || current.Session.Status != "exchanging" {
			return fmt.Errorf("Integration OAuth session changed")
		}
		currentApp, _, e := tx.oauthApplication(completion, subject.WorkspaceID, app.Key)
		if e != nil || !currentApp.Enabled || currentApp.UpdatedAt != record.Revision {
			return fmt.Errorf("Integration OAuth application changed")
		}
		trusted := integrationmodel.WithAccessScope(completion, integrationmodel.AccessScope{WorkspaceID: subject.WorkspaceID, ActorID: subject.UserID, PermissionKey: "integration.oauth_authorizations.complete", Unrestricted: true})
		refs := map[string]string{}
		for field, value := range map[string]string{"access_token": tokens.AccessToken, "refresh_token": tokens.RefreshToken, "client_id": app.ClientID, "client_secret": secret} {
			key := record.Session.ID + "_" + field
			if _, e = tx.UpsertSecret(trusted, subject.WorkspaceID, key, subject.UserID, integrationsdk.SecretInput{Kind: field, Value: value}); e != nil {
				return e
			}
			refs[field] = "secret:" + key
		}
		key := "account_" + record.Session.ID
		config := map[string]any{}
		for k, v := range app.ConnectionConfig {
			config[k] = v
		}
		// Microsoft refresh uses its declared scope field. Restrict it to this grant.
		for _, field := range provider.Descriptor().ConfigFields {
			if field.Key == "scope" {
				config["scope"] = strings.Join(tokens.GrantedScopes, " ")
			}
		}
		if _, e = tx.UpsertConnection(trusted, subject.WorkspaceID, key, subject.UserID, integrationsdk.ConnectionInput{ConnectorKey: app.ConnectorKey, ProviderKey: app.ProviderKey, Name: record.Name, Status: "active", Config: config, SecretRefs: refs}); e != nil {
			return e
		}
		registration := integrationsdk.ConnectionAccountRegistration{Scope: record.Session.Scope}
		if registration.Scope == integrationsdk.ConnectionAccountScopePersonal {
			registration.OwnerUserID = subject.UserID
		}
		account, e := tx.RegisterConnectionAccount(trusted, subject.WorkspaceID, key, subject.UserID, registration)
		if e != nil {
			return e
		}
		if e = tx.saveConnectionGrant(completion, subject.WorkspaceID, key, tokens.GrantedScopes); e != nil {
			return e
		}
		if e = tx.enrichAccountReadiness(completion, &account); e != nil {
			return e
		}
		record.Session.Status = "connected"
		record.Session.GrantedScopes = append([]string(nil), tokens.GrantedScopes...)
		record.Session.Account = &account
		return tx.updateOAuthSession(completion, subject, record, "exchanging")
	})
	if err != nil {
		return integrationsdk.OAuthAuthorizationSession{}, fmt.Errorf("Integration OAuth credentials could not be committed; inspect session before starting a new authorization")
	}
	return record.Session, nil
}

var _ integrationsdk.OAuthAuthorizations = (*ManagementStore)(nil)
