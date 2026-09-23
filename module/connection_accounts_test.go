package module_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration/internal/testsupport/definitionfixture"
	"github.com/domainry/domainry-integration/module"
	ormdialect "github.com/domainry/domainry-orm/dialect"
)

// Uses public Module/SDK boundaries, real HTTP and disk SQLite. Identity bundles
// and the external Provider are deterministic fixtures; no real OAuth is claimed.
func TestPublicConnectionAccountsHTTPIsolationRefreshRevokeAndRestart(t *testing.T) {
	path := t.TempDir() + "/accounts.db"
	var cipherKey [32]byte
	if _, err := rand.Read(cipherKey[:]); err != nil {
		t.Fatal(err)
	}
	var authMu sync.RWMutex
	principals := map[string]identitysdk.Principal{
		"user-a":           accountPrincipal("workspace-a", "user-a", false),
		"user-b":           accountPrincipal("workspace-a", "user-b", false),
		"workspace-reader": accountPrincipal("workspace-a", "user-a", true),
		"other-workspace":  accountPrincipal("workspace-b", "user-a", true),
	}
	var host *rotationHost
	var binding integrationsdk.Binding
	var server *httptest.Server
	open := func() {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		dialect, _ := ormdialect.New(ormdialect.SQLite)
		host = &rotationHost{db: db, dialect: dialect.WithSchema(""), provider: &rotationProvider{}, key: cipherKey, definitions: definitionfixture.NewStore()}
		binding, err = module.NewFactory().OpenModule(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "account-http"}, host)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		adapter := binding.(modulehttp.Provider).HTTPAdapters()[0]
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authMu.RLock()
			principal := principals[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
			authMu.RUnlock()
			adapter.Handler().ServeHTTP(w, r.WithContext(identitysdk.WithRequestIdentity(r.Context(), identitysdk.RequestIdentity{Principal: principal})))
		}))
	}
	closeHost := func() {
		server.Close()
		if err := binding.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := host.db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	open()
	t.Cleanup(func() { server.Close(); _ = binding.Close(context.Background()); _ = host.db.Close() })
	management := binding.(integrationsdk.ManagementBinding).Management()
	admin := binding.(integrationsdk.ConnectionAccountAdministrationBinding).ConnectionAccountAdministration()
	for _, item := range []struct {
		key, owner string
		scope      integrationsdk.ConnectionAccountScope
	}{{"personal-a", "user-a", integrationsdk.ConnectionAccountScopePersonal}, {"personal-b", "user-b", integrationsdk.ConnectionAccountScopePersonal}, {"workspace", "", integrationsdk.ConnectionAccountScopeWorkspace}} {
		if _, err := management.UpsertSecret(t.Context(), "workspace-a", item.key+"-token", "provisioner", integrationsdk.SecretInput{Kind: "token", Value: "original-credential"}); err != nil {
			t.Fatal(err)
		}
		if _, err := management.UpsertConnection(t.Context(), "workspace-a", item.key, "provisioner", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "rotation_probe", Name: item.key, Status: "active", SecretRefs: map[string]string{"token": "secret:" + item.key + "-token"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.RegisterConnectionAccount(t.Context(), "workspace-a", item.key, "provisioner", integrationsdk.ConnectionAccountRegistration{Scope: item.scope, OwnerUserID: item.owner}); err != nil {
			t.Fatal(err)
		}
	}
	request := func(token, method, path string, body any, status int) []byte {
		t.Helper()
		var raw []byte
		if body != nil {
			raw, _ = json.Marshal(body)
		}
		req, err := http.NewRequest(method, server.URL+path, bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != status {
			t.Fatalf("%s %s: status=%d want=%d body=%s", method, path, response.StatusCode, status, data)
		}
		for _, forbidden := range []string{"secret_refs", "config_json", "original-credential", "rotated-credential", "created_by", "ciphertext"} {
			if bytes.Contains(data, []byte(forbidden)) {
				t.Fatalf("HTTP exposed %q", forbidden)
			}
		}
		return data
	}
	list := func(token string, want ...string) {
		t.Helper()
		raw := request(token, "GET", "/integration/connection-accounts?user_id=user-b&workspace_id=workspace-b&allow_workspace=true", nil, 200)
		var value struct {
			Accounts []integrationsdk.ConnectionAccount `json:"accounts"`
		}
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		if len(value.Accounts) != len(want) {
			t.Fatalf("%s list=%s want=%v", token, raw, want)
		}
		for i, key := range want {
			if value.Accounts[i].Key != key {
				t.Fatalf("%s list=%s want=%v", token, raw, want)
			}
		}
	}
	list("user-a", "personal-a")
	list("user-b", "personal-b")
	list("workspace-reader", "personal-a", "workspace")
	list("other-workspace")
	request("missing", "GET", "/integration/connection-accounts", nil, 401)
	request("user-a", "GET", "/integration/connections", nil, 403)
	request("user-a", "GET", "/integration/connection-accounts/personal-b", nil, 400)
	request("user-a", "POST", "/integration/connection-accounts/personal-b/test", map[string]any{}, 400)
	request("user-a", "POST", "/integration/connection-accounts/personal-a/test", map[string]any{"operation": "send_mail", "confirm": true}, 400)
	if host.provider.calls != 0 {
		t.Fatal("unauthorized request reached provider")
	}
	request("user-a", "POST", "/integration/connection-accounts/personal-a/test", map[string]any{}, 400)
	if host.provider.calls != 1 || host.provider.rotations != 1 {
		t.Fatal("refresh fixture not exercised")
	}
	raw := request("user-a", "POST", "/integration/connection-accounts/personal-a/test", map[string]any{}, 200)
	var result integrationsdk.ConnectionAccountTestResult
	if err := json.Unmarshal(raw, &result); err != nil || !result.Connected {
		t.Fatalf("test=%s err=%v", raw, err)
	}
	version := result.Account.UpdatedAt
	authMu.Lock()
	principals["user-a"] = accountPrincipal("workspace-a", "user-a", false)
	principals["user-a"].AccessBundle.FunctionGrants = nil
	authMu.Unlock()
	request("user-a", "GET", "/integration/connection-accounts", nil, 403)
	request("user-a", "POST", "/integration/connection-accounts/personal-a/test", map[string]any{}, 403)
	authMu.Lock()
	principals["user-a"] = accountPrincipal("workspace-a", "user-a", false)
	authMu.Unlock()
	closeHost()
	open()
	list("user-a", "personal-a")
	list("user-b", "personal-b")
	request("user-a", "POST", "/integration/connection-accounts/personal-a/test", map[string]any{}, 200)
	if host.provider.calls != 1 || host.provider.rotations != 0 {
		t.Fatal("restart failed to reuse encrypted refresh")
	}
	request("user-a", "POST", "/integration/connection-accounts/personal-a/revoke", map[string]any{"expected_updated_at": "stale"}, 400)
	request("user-b", "POST", "/integration/connection-accounts/personal-a/revoke", map[string]any{"expected_updated_at": version}, 400)
	workspace := request("workspace-reader", "GET", "/integration/connection-accounts/workspace", nil, 200)
	var workspaceAccount integrationsdk.ConnectionAccount
	if err := json.Unmarshal(workspace, &workspaceAccount); err != nil {
		t.Fatal(err)
	}
	// Read access to shared accounts does not grant shared-account revocation.
	request("workspace-reader", "POST", "/integration/connection-accounts/workspace/revoke", map[string]any{"expected_updated_at": workspaceAccount.UpdatedAt}, 400)
	revoked := request("user-a", "POST", "/integration/connection-accounts/personal-a/revoke", map[string]any{"expected_updated_at": version}, 200)
	replay := request("user-a", "POST", "/integration/connection-accounts/personal-a/revoke", map[string]any{"expected_updated_at": version}, 200)
	if !bytes.Equal(revoked, replay) {
		t.Fatal("lost revoke response replay changed result")
	}
	closeHost()
	open()
	request("user-a", "POST", "/integration/connection-accounts/personal-a/test", map[string]any{}, 400)
	if host.provider.calls != 0 {
		t.Fatal("revoked account reached provider after restart")
	}
	persisted := request("user-a", "GET", "/integration/connection-accounts/personal-a", nil, 200)
	if !bytes.Contains(persisted, []byte(`"status":"revoked"`)) {
		t.Fatal("revocation was not durable")
	}
	if _, err := binding.(integrationsdk.OperationsBinding).Operations().Call(t.Context(), integrationsdk.ProviderCallRequest{RequestID: "after-revoke", WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "personal-a", Operation: "lookup", Payload: json.RawMessage(`{}`), ActorID: "user-a"}); err == nil || host.provider.calls != 0 {
		t.Fatal("revoked connection remained callable")
	}
	t.Log("Public HTTP: personal/workspace isolation, forged subject rejection, exact Identity permissions, rotation after failure, two complete host restarts, stale/foreign revoke rejection, lost-response recovery and revoked invocation rejection passed")
}

func accountPrincipal(workspace, user string, sharedRead bool) identitysdk.Principal {
	bundle := &identitysdk.AccessBundle{ContractVersion: identitysdk.CurrentPolicyBundleVersion, AuthorizationRevision: "account-test-1", ExpiresAt: time.Now().UTC().Add(time.Hour), Subject: identitysdk.Subject{WorkspaceID: identitysdk.WorkspaceID(workspace), SubjectID: identitysdk.SubjectID(user)}}
	for _, action := range []string{"list", "get", "test", "revoke"} {
		scope := identitysdk.DataScopeOwner
		predicate := identitysdk.Predicate{Fact: "owner_user_id", Operator: identitysdk.OperatorEqual, Value: "$subject.id"}
		if sharedRead && action != "revoke" {
			scope = identitysdk.DataScopeAll
			predicate = identitysdk.Predicate{}
		}
		bundle.FunctionGrants = append(bundle.FunctionGrants, identitysdk.FunctionGrant{Resource: "integration.connection_accounts", Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow})
		bundle.DataPolicies = append(bundle.DataPolicies, identitysdk.DataPolicy{Key: "integration.connection_accounts." + action, Resource: "integration.connection_accounts", Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow, DataScopes: []identitysdk.DataScope{scope}, Predicate: predicate})
	}
	return identitysdk.Principal{Known: true, WorkspaceID: workspace, UserID: user, AccessBundle: bundle}
}

func TestConnectionAccountSubjectForPrincipalPreservesCanonicalScope(t *testing.T) {
	owner, err := module.ConnectionAccountSubjectForPrincipal(accountPrincipal("workspace-a", "user-a", false), integrationsdk.ActionIntegrationConnectionAccountsList)
	if err != nil || owner.WorkspaceID != "workspace-a" || owner.UserID != "user-a" || !owner.Access.Personal || owner.Access.Workspace {
		t.Fatalf("owner subject=%#v err=%v", owner, err)
	}
	shared, err := module.ConnectionAccountSubjectForPrincipal(accountPrincipal("workspace-a", "user-a", true), integrationsdk.ActionIntegrationConnectionAccountsList)
	if err != nil || !shared.Access.Personal || !shared.Access.Workspace {
		t.Fatalf("shared subject=%#v err=%v", shared, err)
	}
	if _, err := module.ConnectionAccountSubjectForPrincipal(accountPrincipal("workspace-a", "user-a", false), integrationsdk.ActionIntegrationConnectionAccountsWrite); err == nil {
		t.Fatal("ungranted account action produced a subject")
	}
}
