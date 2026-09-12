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
	"sync/atomic"
	"testing"

	"github.com/domainry/domainry-connector-sdk/web"
	connectormodule "github.com/domainry/domainry-connectors/module"
	"github.com/domainry/domainry-foundation/modulehttp"
	identity "github.com/domainry/domainry-identity-sdk"
	sdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-integration-sdk/remote"
	moduleassembly "github.com/domainry/domainry-integration/internal/assembly/module"
	"github.com/domainry/domainry-integration/internal/infrastructure/connectortransport"
	"github.com/domainry/domainry-integration/internal/infrastructure/security"
	ormdialect "github.com/domainry/domainry-orm/dialect"
)

type webReadHost struct {
	accountSaaSHost
	registry modulehost.ProviderRegistry
	cipher   modulehost.SecretMaterialCipher
}

func (h webReadHost) Providers() modulehost.ProviderRegistry        { return h.registry }
func (h webReadHost) SecretCipher() modulehost.SecretMaterialCipher { return h.cipher }

type webReadFixture struct {
	t                  *testing.T
	mode, path, origin string
	allowRead          atomic.Bool
	db                 *sql.DB
	binding            sdk.Binding
	owner              *Service
	backend, product   *httptest.Server
}

func (f *webReadFixture) open() {
	f.t.Helper()
	var err error
	f.db, err = sql.Open("sqlite", f.path)
	if err != nil {
		f.t.Fatal(err)
	}
	f.db.SetMaxOpenConns(1)
	dialect, _ := ormdialect.New(ormdialect.SQLite)
	transport, err := connectortransport.NewPublicWeb(f.origin)
	if err != nil {
		f.t.Fatal(err)
	}
	set, err := connectormodule.PublicWebProviders(transport)
	if err != nil {
		f.t.Fatal(err)
	}
	registry, err := connectormodule.NewFactory(connectormodule.Options{Providers: set}).Registry()
	if err != nil {
		f.t.Fatal(err)
	}
	cipher, err := security.NewSecretCipher(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		f.t.Fatal(err)
	}
	host := webReadHost{accountSaaSHost: accountSaaSHost{testHost{database: f.db, dialect: dialect.WithSchema("")}}, registry: registry, cipher: cipher}
	if f.mode == "module" {
		f.binding, err = moduleassembly.NewFactory().OpenModule(f.t.Context(), sdk.ApplicationRef{RuntimeID: "web-module"}, host)
	} else {
		f.owner, err = Open(f.t.Context(), sdk.ApplicationRef{RuntimeID: "web-service"}, host, "web-owner-token")
		if err != nil {
			f.t.Fatal(err)
		}
		f.backend = httptest.NewServer(f.owner.Handler)
		summary, e := f.owner.Binding.CapabilitySummary(f.t.Context())
		if e != nil {
			f.t.Fatal(e)
		}
		f.binding, err = NewFactory(remote.NewFactory(remote.Options{BaseURL: f.backend.URL, Token: "web-owner-token", HTTPClient: f.backend.Client(), CapabilityContractSHA256: summary.Identity.ContractSHA256})).OpenSaaS(f.t.Context(), sdk.ApplicationRef{RuntimeID: "web-product"}, nil)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	adapter := f.binding.(modulehttp.Provider).HTTPAdapters()[0]
	f.product = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		workspace := "workspace-a"
		if user == "other-workspace" {
			workspace = "workspace-b"
		}
		p := saasAccountPrincipal(workspace, user, true)
		if user == "anonymous" {
			p = identity.Principal{}
		} else if f.allowRead.Load() && user != "denied" {
			for _, action := range []string{"read_access", "read"} {
				p.AccessBundle.FunctionGrants = append(p.AccessBundle.FunctionGrants, identity.FunctionGrant{Resource: "integration.connection_accounts", Action: identity.Action(action), Effect: identity.EffectAllow})
				p.AccessBundle.DataPolicies = append(p.AccessBundle.DataPolicies, identity.DataPolicy{Key: "integration.connection_accounts." + action, Resource: "integration.connection_accounts", Action: identity.Action(action), Effect: identity.EffectAllow, DataScopes: []identity.DataScope{identity.DataScopeAll}})
			}
		}
		adapter.Handler().ServeHTTP(w, r.WithContext(identity.WithRequestIdentity(r.Context(), identity.RequestIdentity{Principal: p})))
	}))
}
func (f *webReadFixture) close() {
	if f.product != nil {
		f.product.Close()
		f.product = nil
	}
	if f.backend != nil {
		f.backend.Close()
		f.backend = nil
	}
	if f.binding != nil {
		_ = f.binding.Close(context.Background())
		f.binding = nil
	}
	if f.owner != nil {
		_ = f.owner.Close(context.Background())
		f.owner = nil
	}
	if f.db != nil {
		_ = f.db.Close()
		f.db = nil
	}
}
func (f *webReadFixture) request(user, method, path string, input any, status int) []byte {
	f.t.Helper()
	var raw []byte
	if input != nil {
		raw, _ = json.Marshal(input)
	}
	r, err := http.NewRequest(method, f.product.URL+path, bytes.NewReader(raw))
	if err != nil {
		f.t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+user)
	r.Header.Set("Content-Type", "application/json")
	response, err := f.product.Client().Do(r)
	if err != nil {
		f.t.Fatal(err)
	}
	defer response.Body.Close()
	b, err := io.ReadAll(response.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	if response.StatusCode != status {
		f.t.Fatalf("%s %s: %d want %d: %s", method, path, response.StatusCode, status, b)
	}
	if status == 200 && (strings.HasSuffix(path, "/read") || strings.HasSuffix(path, "/read-access")) && response.Header.Get("Cache-Control") != "no-store" {
		f.t.Fatal("cacheable account response")
	}
	if strings.Contains(string(b), "private-service") || strings.Contains(string(b), "secret_refs") || status != 200 && strings.Contains(string(b), "WEB-CONTENT") {
		f.t.Fatal("private web response exposed")
	}
	return b
}
func webReadRequest(id, operation string, input any) sdk.ConnectionAccountReadRequest {
	payload, _ := json.Marshal(input)
	return sdk.ConnectionAccountReadRequest{RequestID: id, Operation: operation, ContractSHA256: web.OperationSHA256(operation), Payload: payload}
}

func TestWebWorkspaceReadHTTPModuleSaaSAndRestart(t *testing.T) {
	for _, mode := range []string{"module", "saas"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			var fail atomic.Bool
			var expectedToken atomic.Value
			expectedToken.Store("private-service-token")
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer "+expectedToken.Load().(string) {
					t.Error("owner credential injection missing")
					w.WriteHeader(401)
					return
				}
				b, _ := io.ReadAll(r.Body)
				if strings.Contains(string(b), "private-service") {
					t.Error("secret in public body")
				}
				if fail.Load() {
					w.WriteHeader(503)
					io.WriteString(w, "private-service upstream error")
					return
				}
				switch r.URL.Path {
				case "/tool/web_search":
					io.WriteString(w, `{"search_id":"actual-http-search","results":[{"url":"https://example.com/news","title":"今日消息","excerpts":["WEB-CONTENT 排名片段"]},{"url":"https://unapproved.com/","excerpts":["DO-NOT-RETURN"]}]}`)
				case "/tool/web_fetch_jina":
					io.WriteString(w, `{"code":0,"data":{"url":"https://example.com/news","title":"今日消息","content":"WEB-CONTENT 2026-09-11 来源正文","warning":"未提供完整性证明"}}`)
				default:
					t.Error("unexpected web service route")
					w.WriteHeader(404)
				}
			}))
			defer upstream.Close()
			f := &webReadFixture{t: t, mode: mode, path: t.TempDir() + "/web.db", origin: upstream.URL}
			f.allowRead.Store(true)
			t.Cleanup(f.close)
			f.open()
			management := func() sdk.Management { return f.binding.(sdk.ManagementBinding).Management() }
			if _, err := management().UpsertSecret(t.Context(), "workspace-a", "web-token", "administrator", sdk.SecretInput{Kind: "bearer_token", Value: "private-service-token"}); err != nil {
				t.Fatal(err)
			}
			if _, err := management().UpsertConnection(t.Context(), "workspace-a", "public-web", "administrator", sdk.ConnectionInput{ConnectorKey: "web", ProviderKey: "llm_proxy", Status: "active", Name: "公开网页服务", Config: map[string]any{"base_url": upstream.URL, "allowed_source_hosts": []string{"example.com"}}, SecretRefs: map[string]string{"api_token": "secret:web-token"}}); err != nil {
				t.Fatal(err)
			}
			search := webReadRequest("search-1", web.SearchOperationKey, web.SearchRequest{Query: "CURRENT-WEB-QUERY 今日消息", Limit: 2})
			path := "/integration/connection-accounts/public-web"
			f.request("reader-a", "POST", path+"/read", search, 400)
			if calls.Load() != 0 {
				t.Fatal("unregistered service account dispatched")
			}
			account, err := f.binding.(sdk.ConnectionAccountAdministrationBinding).ConnectionAccountAdministration().RegisterConnectionAccount(t.Context(), "workspace-a", "public-web", "administrator", sdk.ConnectionAccountRegistration{Scope: sdk.ConnectionAccountScopeWorkspace})
			if err != nil {
				t.Fatal(err)
			}
			// Administrative registration returns the ownership projection. Current
			// readiness belongs to the current-user get/list boundary.
			if err := json.Unmarshal(f.request("reader-a", "GET", path, nil, 200), &account); err != nil {
				t.Fatal(err)
			}
			if account.Readiness == nil || !account.Readiness.Available || len(account.Readiness.GrantedScopes) != 0 {
				t.Fatal("non-OAuth service readiness invalid", account.Readiness)
			}
			var grants int
			if err := f.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_connection_grants").Scan(&grants); err != nil || grants != 0 {
				t.Fatal("fabricated OAuth grant", grants, err)
			}
			var ciphertext string
			if err := f.db.QueryRowContext(t.Context(), "SELECT ciphertext FROM _integration_secret_materials WHERE workspace_id='workspace-a' AND secret_key='web-token'").Scan(&ciphertext); err != nil || !strings.HasPrefix(ciphertext, "v1.") || strings.Contains(ciphertext, "private-service") {
				t.Fatal("service token not encrypted", err)
			}
			f.request("anonymous", "POST", path+"/read", search, 401)
			f.request("denied", "POST", path+"/read", search, 403)
			f.request("other-workspace", "POST", path+"/read?workspace_id=workspace-a&user_id=reader-a&allow_workspace=true", search, 400)
			f.request("reader-a", "GET", "/integration/connections", nil, 403)
			f.request("reader-a", "POST", path+"/test", map[string]any{}, 400)
			forged := search
			forged.ContractSHA256 = strings.Repeat("d", 64)
			f.request("reader-a", "POST", path+"/read", forged, 400)
			f.request("reader-a", "POST", path+"/read", map[string]any{"request_id": "forged", "operation": search.Operation, "contract_sha256": search.ContractSHA256, "payload": map[string]any{"query": "x"}, "allow_workspace": true}, 400)
			if calls.Load() != 0 {
				t.Fatal("preflight or unsupported probe dispatched")
			}
			f.request("reader-a", "POST", path+"/read-access", search.OperationContract(), 200)
			read := func(user string, in sdk.ConnectionAccountReadRequest) sdk.ConnectionAccountReadResult {
				t.Helper()
				var out sdk.ConnectionAccountReadResult
				if json.Unmarshal(f.request(user, "POST", path+"/read", in, 200), &out) != nil {
					t.Fatal("bad read result")
				}
				return out
			}
			first := read("reader-a", search)
			var results web.SearchResult
			if !first.PayloadAvailable || first.Source.ConnectionKey != "public-web" || first.ReadAt == "" || json.Unmarshal(first.Payload, &results) != nil || len(results.Items) != 1 || !results.Truncated || strings.Contains(string(first.Payload), "DO-NOT-RETURN") {
				t.Fatal("search source lost", first)
			}
			second := read("reader-b", search)
			if !second.PayloadAvailable || second.InvocationID == first.InvocationID || calls.Load() != 2 {
				t.Fatal("workspace readers shared invocation identity")
			}
			fetch := webReadRequest("fetch-1", web.FetchOperationKey, web.FetchRequest{URL: "https://example.com/news"})
			pageResult := read("reader-a", fetch)
			var page web.Page
			if json.Unmarshal(pageResult.Payload, &page) != nil || page.SourceCompleteness != "unknown" || page.Content != "WEB-CONTENT 2026-09-11 来源正文" || len(page.Warnings) != 1 {
				t.Fatal("fetch semantics changed")
			}
			f.allowRead.Store(false)
			f.request("reader-a", "POST", path+"/read", fetch, 403)
			f.allowRead.Store(true)
			if calls.Load() != 3 {
				t.Fatal("revoked permission dispatched")
			}
			f.close()
			f.open()
			replay := read("reader-a", fetch)
			if replay.PayloadAvailable || len(replay.Payload) != 0 || replay.InvocationID != pageResult.InvocationID || calls.Load() != 3 {
				t.Fatal("sensitive response persisted or replayed")
			}
			fetch.RequestID = "fetch-after-restart"
			read("reader-a", fetch)
			if calls.Load() != 4 {
				t.Fatal("fresh read after restart failed")
			}
			if _, err := management().TransitionSecret(t.Context(), "workspace-a", "web-token", "disable", "administrator"); err != nil {
				t.Fatal(err)
			}
			f.request("reader-a", "POST", path+"/read-access", fetch.OperationContract(), 400)
			f.request("reader-a", "POST", path+"/read", fetch, 400)
			if calls.Load() != 4 {
				t.Fatal("disabled secret dispatched")
			}
			expectedToken.Store("private-service-token-rotated")
			if _, err := management().UpsertSecret(t.Context(), "workspace-a", "web-token", "administrator", sdk.SecretInput{Kind: "bearer_token", Value: "private-service-token-rotated"}); err != nil {
				t.Fatal(err)
			}
			fetch.RequestID = "fetch-rotated"
			read("reader-a", fetch)
			if calls.Load() != 5 {
				t.Fatal("rotated credential not resolved")
			}
			fail.Store(true)
			failed := webReadRequest("response-lost", web.FetchOperationKey, web.FetchRequest{URL: "https://example.com/news"})
			f.request("reader-a", "POST", path+"/read", failed, 400)
			f.request("reader-a", "POST", path+"/read", failed, 400)
			if calls.Load() != 6 {
				t.Fatal("failed sensitive identity replayed")
			}
			f.close()
			f.open()
			f.request("reader-a", "POST", path+"/read", failed, 400)
			if calls.Load() != 6 {
				t.Fatal("restart reclaimed failed identity")
			}
			fail.Store(false)
			fetch.RequestID = "explicit-new-read"
			read("reader-a", fetch)
			if calls.Load() != 7 {
				t.Fatal("new request did not execute")
			}
			invocations, err := f.binding.(sdk.OperationsBinding).Operations().ListInvocations(t.Context(), sdk.InvocationQuery{WorkspaceID: "workspace-a"})
			if err != nil {
				t.Fatal(err)
			}
			evidence, _ := json.Marshal(invocations)
			for _, value := range []string{"private-service", "WEB-CONTENT", "CURRENT-WEB-QUERY", "https://example.com/news"} {
				if strings.Contains(string(evidence), value) {
					t.Fatal("sensitive content in owner ledger", value)
				}
			}
			if _, err := management().SetConnectionStatus(t.Context(), "workspace-a", "public-web", "inactive", "administrator"); err != nil {
				t.Fatal(err)
			}
			f.request("reader-a", "POST", path+"/read", fetch, 400)
			if calls.Load() != 7 {
				t.Fatal("inactive service account dispatched")
			}
			if mode == "saas" {
				response, err := f.backend.Client().Get(f.backend.URL + "/integration/v1/connection-accounts?workspace_id=workspace-a&user_id=reader-a&allow_workspace=true")
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				if response.StatusCode != 401 {
					t.Fatal("untrusted SaaS subject accepted")
				}
			}
			t.Logf("%s: 7 actual proxy HTTP requests; workspace readers, zero OAuth grants, AES credential persistence/rotation, two restarts, sensitive replay and revoked owner state verified", mode)
		})
	}
}
