package saas

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-integration-sdk/remote"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	"github.com/domainry/domainry-orm/migration"
	"github.com/domainry/domainry-orm/query"
	"time"
)

type accountSaaSHost struct{ testHost }

func (h accountSaaSHost) Migrations() modulehost.MigrationRegistrar { return accountSaaSMigrations{h} }

type accountSaaSMigrations struct{ host accountSaaSHost }

func (accountSaaSMigrations) Driver() string { return "sqlite" }
func (accountSaaSMigrations) Schema() string { return "" }
func (m accountSaaSMigrations) ApplyOwnedMigrations(ctx context.Context, _ string, items []modulehost.SchemaMigration) error {
	runner, err := migration.NewRunner(m.host.database, m.host.dialect.(query.Renderer), migration.Options{})
	if err != nil {
		return err
	}
	return runner.Apply(ctx, items)
}
func (testProvider) TestConnection(context.Context, connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	return connector.TestConnectionResult{Connected: true, Details: json.RawMessage(`{"token":"provider-private-diagnostic"}`)}, nil
}

func TestConnectionAccountSaaSProductAuthorizationSurvivesRemoteBoundaryAndRestart(t *testing.T) {
	path := t.TempDir() + "/account-saas.db"
	var service *Service
	var binding integrationsdk.Binding
	var database *sql.DB
	var backend, product *httptest.Server
	open := func() {
		var err error
		database, err = sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		database.SetMaxOpenConns(1)
		dialect, _ := ormdialect.New(ormdialect.SQLite)
		service, err = Open(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "accounts-saas"}, accountSaaSHost{newTestHost(database, dialect.WithSchema(""))}, "account-service-token")
		if err != nil {
			t.Fatal(err)
		}
		backend = httptest.NewServer(service.Handler)
		binding, err = NewFactory(remote.NewFactory(remote.Options{BaseURL: backend.URL, Token: "account-service-token", HTTPClient: backend.Client()})).OpenSaaS(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "product-saas"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		adapter := binding.(modulehttp.Provider).HTTPAdapters()[0]
		product = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			p := saasAccountPrincipal("workspace-a", "user-a", false)
			switch token {
			case "user-b":
				p = saasAccountPrincipal("workspace-a", "user-b", false)
			case "shared-reader":
				p = saasAccountPrincipal("workspace-a", "user-a", true)
			case "other-workspace":
				p = saasAccountPrincipal("workspace-b", "user-a", true)
			case "denied":
				p.AccessBundle.DataPolicies = append(p.AccessBundle.DataPolicies, identitysdk.DataPolicy{Key: "deny-get", Resource: "integration.connection_accounts", Action: "get", Effect: identitysdk.EffectDeny, DataScopes: []identitysdk.DataScope{identitysdk.DataScopeOwner}, Predicate: identitysdk.Predicate{Fact: "owner_user_id", Operator: identitysdk.OperatorEqual, Value: "$subject.id"}})
			case "user-a":
			default:
				p = identitysdk.Principal{}
			}
			if p.Known {
				if err := p.AccessBundle.Validate(time.Now().UTC()); err != nil {
					t.Errorf("invalid account authorization fixture: %v", err)
				}
			}
			adapter.Handler().ServeHTTP(w, r.WithContext(identitysdk.WithRequestIdentity(r.Context(), identitysdk.RequestIdentity{Principal: p})))
		}))
	}
	closeAll := func() {
		product.Close()
		backend.Close()
		_ = binding.Close(context.Background())
		_ = service.Close(context.Background())
		_ = database.Close()
	}
	open()
	t.Cleanup(func() { closeAll() })
	management := binding.(integrationsdk.ManagementBinding).Management()
	admin := binding.(integrationsdk.ConnectionAccountAdministrationBinding).ConnectionAccountAdministration()
	for _, item := range []struct {
		key, owner string
		scope      integrationsdk.ConnectionAccountScope
	}{{"personal-a", "user-a", integrationsdk.ConnectionAccountScopePersonal}, {"personal-b", "user-b", integrationsdk.ConnectionAccountScopePersonal}, {"workspace", "", integrationsdk.ConnectionAccountScopeWorkspace}} {
		if _, err := management.UpsertConnection(t.Context(), "workspace-a", item.key, "provisioner", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Name: item.key, Status: "active"}); err != nil {
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
		req, err := http.NewRequest(method, product.URL+path, bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response, err := product.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		if response.StatusCode != status {
			t.Fatalf("%s %s: status=%d want=%d body=%s", method, path, response.StatusCode, status, data)
		}
		for _, forbidden := range []string{"secret_refs", "config_json", "provider-private-diagnostic", "created_by"} {
			if bytes.Contains(data, []byte(forbidden)) {
				t.Fatalf("SaaS product leaked %s", forbidden)
			}
		}
		return data
	}
	list := func(token string, want int) {
		t.Helper()
		raw := request(token, "GET", "/integration/connection-accounts?user_id=user-b&workspace_id=workspace-b&allow_personal=true&allow_workspace=true", nil, 200)
		var result struct {
			Accounts []integrationsdk.ConnectionAccount `json:"accounts"`
		}
		if err := json.Unmarshal(raw, &result); err != nil || len(result.Accounts) != want {
			t.Fatalf("list=%s err=%v", raw, err)
		}
	}
	list("user-a", 1)
	list("user-b", 1)
	list("shared-reader", 2)
	list("other-workspace", 0)
	request("missing", "GET", "/integration/connection-accounts", nil, 401)
	request("user-a", "GET", "/integration/connection-accounts/personal-b", nil, 400)
	request("denied", "GET", "/integration/connection-accounts/personal-a", nil, 400)
	request("user-a", "GET", "/integration/connections", nil, 403)
	request("user-a", "POST", "/integration/connection-accounts/personal-a/test", map[string]any{"operation": "send_mail"}, 400)
	test := request("user-a", "POST", "/integration/connection-accounts/personal-a/test", map[string]any{}, 200)
	var result integrationsdk.ConnectionAccountTestResult
	if err := json.Unmarshal(test, &result); err != nil || !result.Connected {
		t.Fatalf("test=%s err=%v", test, err)
	}
	closeAll()
	open()
	list("user-a", 1)
	list("shared-reader", 2)
	raw := request("shared-reader", "GET", "/integration/connection-accounts/workspace", nil, 200)
	var workspace integrationsdk.ConnectionAccount
	if err := json.Unmarshal(raw, &workspace); err != nil {
		t.Fatal(err)
	}
	request("shared-reader", "POST", "/integration/connection-accounts/workspace/revoke", map[string]any{"expected_updated_at": workspace.UpdatedAt}, 400)
	request("user-b", "POST", "/integration/connection-accounts/personal-a/revoke", map[string]any{"expected_updated_at": result.Account.UpdatedAt}, 400)
	revoked := request("user-a", "POST", "/integration/connection-accounts/personal-a/revoke", map[string]any{"expected_updated_at": result.Account.UpdatedAt}, 200)
	replay := request("user-a", "POST", "/integration/connection-accounts/personal-a/revoke", map[string]any{"expected_updated_at": result.Account.UpdatedAt}, 200)
	if !bytes.Equal(revoked, replay) {
		t.Fatal("SaaS lost-response retry changed result")
	}
	closeAll()
	open()
	request("user-a", "POST", "/integration/connection-accounts/personal-a/test", map[string]any{}, 400)
	// The backend is a credentialed service boundary, never a browser endpoint.
	unauthorized, err := backend.Client().Get(backend.URL + "/integration/v1/connection-accounts?workspace_id=workspace-a&user_id=user-a&allow_workspace=true")
	if err != nil {
		t.Fatal(err)
	}
	unauthorized.Body.Close()
	if unauthorized.StatusCode != 401 {
		t.Fatal("SaaS backend accepted untrusted authorization projection")
	}
	t.Log("Product HTTP -> SDK remote -> authenticated SaaS HTTP -> SQLite: exact per-action ownership authorization, forged input, explicit deny, safe diagnostics, two host restarts and revoke replay passed")
}

func saasAccountPrincipal(workspace, user string, sharedRead bool) identitysdk.Principal {
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
