package integration

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

type oauthStoreProvider struct {
	calls              int
	probes             int
	testScopes         [][]string
	testScopesDeclared bool
	during             func()
	last               connector.OAuthCodeExchangeRequest
}

func (p *oauthStoreProvider) OAuthConnectionTestScopes() ([][]string, bool) {
	return p.testScopes, p.testScopesDeclared
}
func (p *oauthStoreProvider) TestConnection(context.Context, connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	p.probes++
	return connector.TestConnectionResult{Connected: true}, nil
}
func (*oauthStoreProvider) Descriptor() connector.ProviderDescriptor {
	fields := []connector.SecretField{}
	for _, key := range []string{"access_token", "refresh_token", "client_id", "client_secret"} {
		fields = append(fields, connector.SecretField{Key: key, Name: key, Required: true, CredentialKind: connector.SecretCredentialGeneric, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound})
	}
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "probe", ProviderRevision: "1.0.0", SecretFields: fields, Operations: []connector.OperationDescriptor{{ConnectorKey: "crm", ProviderKey: "probe", Key: "lookup", ContractSHA256: strings.Repeat("a", 64), Mode: connector.ModeCall, Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}}}
}
func (*oauthStoreProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, errors.New("unexpected business call")
}
func (*oauthStoreProvider) AuthorizationURL(r connector.OAuthAuthorizationRequest) (string, error) {
	if r.ClientID == "" || r.RedirectURI == "" {
		return "", errors.New("incomplete application")
	}
	return "https://issuer.example.test/authorize?" + url.Values{"state": {r.State}, "code_challenge": {r.CodeChallenge}, "redirect_uri": {r.RedirectURI}}.Encode(), nil
}
func (p *oauthStoreProvider) ExchangeAuthorizationCode(_ context.Context, r connector.OAuthCodeExchangeRequest) (connector.OAuthTokens, error) {
	p.calls++
	p.last = r
	if p.during != nil {
		p.during()
	}
	switch r.Code {
	case "unknown":
		return connector.OAuthTokens{}, connector.UncertainError("test.oauth.unknown", errors.New("lost response"))
	case "deny":
		return connector.OAuthTokens{}, connector.PermanentError("test.oauth.denied", errors.New("denied"))
	}
	return connector.OAuthTokens{AccessToken: "private-access", RefreshToken: "private-refresh", TokenType: "Bearer", GrantedScopes: []string{"read"}, ExpiresInSeconds: 3600}, nil
}
func setupOAuthStore(t *testing.T) (*ManagementStore, *oauthStoreProvider, integrationsdk.ConnectionAccountSubject, integrationsdk.OAuthApplication) {
	t.Helper()
	db, dialect := webPushTestDatabase(t, "oauth-sessions")
	provider := &oauthStoreProvider{}
	cipher := &refreshTestCipher{}
	resolver := NewSecretResolver(db, dialect, cipher)
	delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider}, resolver)
	store := NewManagementStore(db, dialect, cipher, delivery)
	subject := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: integrationsdk.ConnectionAccountAccess{Personal: true}}
	app, err := store.UpsertOAuthApplication(t.Context(), "workspace-a", "google", "admin", integrationsdk.OAuthApplicationInput{ConnectorKey: "crm", ProviderKey: "probe", Name: "Test OAuth", ClientID: "client", ClientSecret: "private-client", RedirectURI: "https://product.example.test/callback", Scopes: []string{"read", "write"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	return store, provider, subject, app
}
func startOAuthStore(t *testing.T, s *ManagementStore, subject integrationsdk.ConnectionAccountSubject) (integrationsdk.OAuthAuthorizationSession, string) {
	t.Helper()
	value, err := s.StartOAuthAuthorization(t.Context(), subject, integrationsdk.OAuthAuthorizationInput{ApplicationKey: "google", Name: "My account", Scope: integrationsdk.ConnectionAccountScopePersonal, Scopes: []string{"read"}})
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(value.AuthorizationURL)
	return value, parsed.Query().Get("state")
}
func TestOAuthSessionAtomicallyCreatesPrivateAccountAndReplaysReceipt(t *testing.T) {
	store, provider, subject, _ := setupOAuthStore(t)
	session, state := startOAuthStore(t, store, subject)
	pending, err := store.GetOAuthAuthorization(t.Context(), subject, session.ID)
	if err != nil || pending.AuthorizationURL != "" {
		t.Fatal("session query exposed navigation state", err)
	}
	callback := integrationsdk.OAuthAuthorizationCallback{State: state, Code: "accepted"}
	value, err := store.CompleteOAuthAuthorization(t.Context(), subject, callback)
	if err != nil || value.Status != "connected" || value.Account == nil || value.Account.OwnerUserID != "user-a" {
		t.Fatalf("completed=%v err=%v", value, err)
	}
	replay, err := store.CompleteOAuthAuthorization(t.Context(), subject, callback)
	if err != nil || replay.Account.Key != value.Account.Key || provider.calls != 1 {
		t.Fatal("duplicate callback exchanged code again", err)
	}
	challenge := sha256.Sum256([]byte(provider.last.CodeVerifier))
	parsed, _ := url.Parse(session.AuthorizationURL)
	if base64.RawURLEncoding.EncodeToString(challenge[:]) != parsed.Query().Get("code_challenge") || provider.last.ClientSecret != "private-client" {
		t.Fatal("PKCE or encrypted application secret not preserved")
	}
	list, err := store.ListConnectionAccounts(t.Context(), subject)
	if err != nil || len(list) != 1 {
		t.Fatal("account not committed once", err)
	}
	encoded, _ := json.Marshal(value)
	for _, secret := range []string{state, "private-client", "private-access", "private-refresh", provider.last.CodeVerifier} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("OAuth result exposed secret")
		}
	}
	record, err := store.readOAuthSession(t.Context(), subject, query.Equal("id", session.ID))
	if err != nil || record.VerifierCiphertext != "" {
		t.Fatal("completed verifier was retained", err)
	}
}
func TestOAuthSessionRejectsCrossUserWorkspaceScopeAndState(t *testing.T) {
	store, provider, subject, _ := setupOAuthStore(t)
	session, state := startOAuthStore(t, store, subject)
	for _, other := range []integrationsdk.ConnectionAccountSubject{{WorkspaceID: "workspace-a", UserID: "user-b", Access: subject.Access}, {WorkspaceID: "workspace-b", UserID: "user-a", Access: subject.Access}, {WorkspaceID: "workspace-a", UserID: "user-a"}} {
		if _, err := store.CompleteOAuthAuthorization(t.Context(), other, integrationsdk.OAuthAuthorizationCallback{State: state, Code: "accepted"}); err == nil {
			t.Fatal("foreign callback accepted")
		}
		if _, err := store.GetOAuthAuthorization(t.Context(), other, session.ID); err == nil {
			t.Fatal("foreign session visible")
		}
	}
	if _, err := store.CompleteOAuthAuthorization(t.Context(), subject, integrationsdk.OAuthAuthorizationCallback{State: strings.Repeat("x", 43), Code: "accepted"}); err == nil {
		t.Fatal("forged state accepted")
	}
	if _, err := store.StartOAuthAuthorization(t.Context(), subject, integrationsdk.OAuthAuthorizationInput{ApplicationKey: "google", Scope: integrationsdk.ConnectionAccountScopeWorkspace, Scopes: []string{"read"}}); err == nil {
		t.Fatal("personal grant created workspace session")
	}
	if _, err := store.StartOAuthAuthorization(t.Context(), subject, integrationsdk.OAuthAuthorizationInput{ApplicationKey: "google", Scope: integrationsdk.ConnectionAccountScopePersonal, Scopes: []string{"unconfigured"}}); err == nil {
		t.Fatal("unconfigured scope accepted")
	}
	if provider.calls != 0 {
		t.Fatal("unauthorized callback reached provider")
	}
}
func TestOAuthSessionHandlesRejectionUnknownAndExpiredWithoutReplay(t *testing.T) {
	for _, test := range []struct{ code, status string }{{"deny", "rejected"}, {"unknown", "needs_reauthorization"}, {"cancel", "rejected"}} {
		t.Run(test.code, func(t *testing.T) {
			store, provider, subject, _ := setupOAuthStore(t)
			_, state := startOAuthStore(t, store, subject)
			callback := integrationsdk.OAuthAuthorizationCallback{State: state, Code: test.code}
			if test.code == "cancel" {
				callback.Code = ""
				callback.Error = "access_denied"
			}
			value, err := store.CompleteOAuthAuthorization(t.Context(), subject, callback)
			if err != nil || value.Status != test.status {
				t.Fatalf("result=%v err=%v", value, err)
			}
			count := provider.calls
			if _, err = store.CompleteOAuthAuthorization(t.Context(), subject, callback); err != nil || provider.calls != count {
				t.Fatal("terminal code exchange replayed", err)
			}
			accounts, _ := store.ListConnectionAccounts(t.Context(), subject)
			if len(accounts) != 0 {
				t.Fatal("failed authorization created account")
			}
		})
	}
	store, provider, subject, _ := setupOAuthStore(t)
	session, state := startOAuthStore(t, store, subject)
	if _, err := store.database.ExecContext(t.Context(), "UPDATE _integration_oauth_sessions SET expires_at=? WHERE id=?", time.Now().UTC().Add(-time.Minute).UnixMilli(), session.ID); err != nil {
		t.Fatal(err)
	}
	value, err := store.CompleteOAuthAuthorization(t.Context(), subject, integrationsdk.OAuthAuthorizationCallback{State: state, Code: "accepted"})
	if err != nil || value.Status != "expired" || provider.calls != 0 {
		t.Fatal("expired callback exchanged code", err)
	}
}
func TestOAuthApplicationChangeDuringExchangeRollsBackCredentialsAndNeedsNewAuthorization(t *testing.T) {
	store, provider, subject, app := setupOAuthStore(t)
	session, state := startOAuthStore(t, store, subject)
	provider.during = func() {
		if _, err := store.UpsertOAuthApplication(t.Context(), "workspace-a", app.Key, "admin", integrationsdk.OAuthApplicationInput{ConnectorKey: app.ConnectorKey, ProviderKey: app.ProviderKey, Name: app.Name, ClientID: app.ClientID, RedirectURI: app.RedirectURI, Scopes: app.Scopes, Enabled: false, ExpectedUpdatedAt: app.UpdatedAt}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.CompleteOAuthAuthorization(t.Context(), subject, integrationsdk.OAuthAuthorizationCallback{State: state, Code: "accepted"}); err == nil {
		t.Fatal("disabled application provisioned credentials")
	}
	accounts, _ := store.ListConnectionAccounts(t.Context(), subject)
	if len(accounts) != 0 {
		t.Fatal("failed finalization committed account")
	}
	record, _ := store.readOAuthSession(t.Context(), subject, query.Equal("id", session.ID))
	record.ExchangeDeadline = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	if err := store.updateOAuthSession(t.Context(), subject, record, "exchanging"); err != nil {
		t.Fatal(err)
	}
	value, err := store.GetOAuthAuthorization(t.Context(), subject, session.ID)
	if err != nil || value.Status != "needs_reauthorization" {
		t.Fatal("interrupted exchange not marked unknown", err)
	}
	if _, err = store.CompleteOAuthAuthorization(t.Context(), subject, integrationsdk.OAuthAuthorizationCallback{State: state, Code: "accepted"}); err != nil || provider.calls != 1 {
		t.Fatal("unknown exchange replayed", err)
	}
}

func TestOAuthConcurrentCallbacksAndCancellationPersistOneEncryptedAccount(t *testing.T) {
	store, provider, subject, _ := setupOAuthStore(t)
	session, state := startOAuthStore(t, store, subject)
	var rawApplication, ciphertext, rawSession, verifierCiphertext string
	if err := store.database.QueryRowContext(t.Context(), "SELECT application_json,client_ciphertext FROM _integration_oauth_applications WHERE workspace_id=? AND application_key=?", "workspace-a", "google").Scan(&rawApplication, &ciphertext); err != nil {
		t.Fatal(err)
	}
	if err := store.database.QueryRowContext(t.Context(), "SELECT session_json,verifier_ciphertext FROM _integration_oauth_sessions WHERE id=?", session.ID).Scan(&rawSession, &verifierCiphertext); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rawApplication+ciphertext+rawSession+verifierCiphertext, "private-client") || strings.Contains(rawSession, state) || ciphertext == "" || verifierCiphertext == "" {
		t.Fatal("OAuth session or application material is not private")
	}
	if strings.Contains(rawSession, "expires_at") {
		t.Fatal("OAuth session JSON duplicated numeric expires_at column as a transport string")
	}
	callbacks := make(chan error, 12)
	var workers sync.WaitGroup
	for i := 0; i < 12; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := store.CompleteOAuthAuthorization(t.Context(), subject, integrationsdk.OAuthAuthorizationCallback{State: state, Code: "accepted"})
			callbacks <- err
		}()
	}
	workers.Wait()
	close(callbacks)
	successes := 0
	for err := range callbacks {
		if err == nil {
			successes++
		}
	}
	if successes == 0 || provider.calls != 1 {
		t.Fatalf("concurrent callbacks: successful receipts=%d exchanges=%d", successes, provider.calls)
	}
	accounts, err := store.ListConnectionAccounts(t.Context(), subject)
	if err != nil || len(accounts) != 1 {
		t.Fatal("concurrent callbacks did not commit exactly one account", err)
	}
	// The browser closes during another completed exchange; bounded saving remains.
	_, state = startOAuthStore(t, store, subject)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	provider.during = cancel
	result, err := store.CompleteOAuthAuthorization(ctx, subject, integrationsdk.OAuthAuthorizationCallback{State: state, Code: "accepted"})
	if err != nil || result.Status != "connected" || ctx.Err() == nil {
		t.Fatalf("cancel lost completed token exchange: status=%s err=%v", result.Status, err)
	}
}
