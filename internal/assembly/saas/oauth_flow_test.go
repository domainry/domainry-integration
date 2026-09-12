package saas

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	google "github.com/domainry/domainry-connectors/providers/google_workspace/google"
	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-integration-sdk/remote"
	moduleassembly "github.com/domainry/domainry-integration/internal/assembly/module"
	ormdialect "github.com/domainry/domainry-orm/dialect"
)

type oauthFlowHost struct {
	accountSaaSHost
	registry *connector.Registry
}

func (h oauthFlowHost) Providers() modulehost.ProviderRegistry        { return h.registry }
func (h oauthFlowHost) SecretCipher() modulehost.SecretMaterialCipher { return oauthFlowCipher{} }

type oauthFlowCipher struct{}

func (oauthFlowCipher) EncryptSecretMaterial(_ context.Context, workspace, key, plain string) (string, error) {
	block, _ := aes.NewCipher([]byte(strings.Repeat("k", 32)))
	aead, _ := cipher.NewGCM(block)
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(aead.Seal(nonce, nonce, []byte(plain), []byte(workspace+"\x00"+key))), nil
}
func (oauthFlowCipher) DecryptSecretMaterial(_ context.Context, workspace, key, value string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	block, _ := aes.NewCipher([]byte(strings.Repeat("k", 32)))
	aead, _ := cipher.NewGCM(block)
	if len(raw) < aead.NonceSize() {
		return "", errors.New("invalid ciphertext")
	}
	plain, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], []byte(workspace+"\x00"+key))
	return string(plain), err
}

type oauthFlowTransport struct {
	endpoint string
	client   *http.Client
}

func (t oauthFlowTransport) RoundTripHTTP(ctx context.Context, input connector.HTTPRequest) (connector.HTTPResponse, error) {
	if input.URL != t.endpoint {
		return connector.HTTPResponse{}, errors.New("unapproved test endpoint")
	}
	form, err := url.ParseQuery(string(input.Body))
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	for key, value := range input.SecretForm {
		if form.Has(key) {
			return connector.HTTPResponse{}, errors.New("duplicate private form field")
		}
		form.Set(key, value)
	}
	request, err := http.NewRequestWithContext(ctx, input.Method, input.URL, strings.NewReader(form.Encode()))
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	for key, values := range input.Headers {
		request.Header[key] = values
	}
	response, err := t.client.Do(request)
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, input.MaxResponseBytes+1))
	return connector.HTTPResponse{StatusCode: response.StatusCode, Headers: response.Header, Body: body}, err
}
func (oauthFlowTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestOAuthModuleAndSaaSHTTPUseGoogleProviderAndDurableOneTimeSessions(t *testing.T) {
	for _, mode := range []string{"module", "saas"} {
		t.Run(mode, func(t *testing.T) {
			var issuerMu sync.Mutex
			challenges := map[string]string{}
			calls := 0
			issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				issuerMu.Lock()
				defer issuerMu.Unlock()
				calls++
				if r.Method != "POST" || r.URL.Path != "/token" || r.ParseForm() != nil || r.PostForm.Get("client_id") != "test-client" || r.PostForm.Get("client_secret") != "private-client-secret" {
					t.Error("invalid host-transported token request")
					w.WriteHeader(400)
					return
				}
				code := r.PostForm.Get("code")
				challenge, found := challenges[code]
				sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
				if !found || base64.RawURLEncoding.EncodeToString(sum[:]) != challenge {
					w.WriteHeader(400)
					io.WriteString(w, `{"error":"invalid_grant"}`)
					return
				}
				delete(challenges, code)
				if code == "unknown-code" {
					w.WriteHeader(503)
					io.WriteString(w, `{"error":"temporary_server_failure"}`)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, `{"access_token":"private-access","refresh_token":"private-refresh","token_type":"Bearer","expires_in":3600,"scope":"https://www.googleapis.com/auth/calendar.readonly"}`)
			}))
			defer issuer.Close()
			path := t.TempDir() + "/oauth-host.db"
			var database *sql.DB
			var binding integrationsdk.Binding
			var service *Service
			var backend, product *httptest.Server
			open := func() {
				var err error
				database, err = sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				database.SetMaxOpenConns(1)
				dialect, _ := ormdialect.New(ormdialect.SQLite)
				provider, err := google.New(oauthFlowTransport{endpoint: issuer.URL + "/token", client: issuer.Client()})
				if err != nil {
					t.Fatal(err)
				}
				registry := connector.NewRegistry()
				if err = registry.Register(provider); err != nil {
					t.Fatal(err)
				}
				registry.Freeze()
				host := oauthFlowHost{accountSaaSHost: accountSaaSHost{testHost{database: database, dialect: dialect.WithSchema("")}}, registry: registry}
				if mode == "module" {
					binding, err = moduleassembly.NewFactory().OpenModule(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "oauth-module"}, host)
				} else {
					service, err = Open(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "oauth-service"}, host, "oauth-service-token")
					if err != nil {
						t.Fatal(err)
					}
					backend = httptest.NewServer(service.Handler)
					summary, e := service.Binding.CapabilitySummary(t.Context())
					if e != nil {
						t.Fatal(e)
					}
					binding, err = NewFactory(remote.NewFactory(remote.Options{BaseURL: backend.URL, Token: "oauth-service-token", HTTPClient: backend.Client(), CapabilityContractSHA256: summary.Identity.ContractSHA256})).OpenSaaS(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "oauth-product"}, nil)
				}
				if err != nil {
					t.Fatal(err)
				}
				adapter := binding.(modulehttp.Provider).HTTPAdapters()[0]
				product = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					user := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
					principal := saasAccountPrincipal("workspace-a", user, false)
					for resource, actions := range map[string][]string{"integration.oauth_authorizations": {"options", "start", "get", "complete"}, "integration.oauth_applications": {"list", "upsert"}} {
						if resource == "integration.oauth_applications" && user != "admin" {
							continue
						}
						scope := identitysdk.DataScopeOwner
						predicate := identitysdk.Predicate{Fact: "owner_user_id", Operator: identitysdk.OperatorEqual, Value: "$subject.id"}
						if resource == "integration.oauth_applications" {
							scope = identitysdk.DataScopeAll
							predicate = identitysdk.Predicate{}
						}
						for _, action := range actions {
							principal.AccessBundle.FunctionGrants = append(principal.AccessBundle.FunctionGrants, identitysdk.FunctionGrant{Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow})
							principal.AccessBundle.DataPolicies = append(principal.AccessBundle.DataPolicies, identitysdk.DataPolicy{Key: resource + "." + action, Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow, DataScopes: []identitysdk.DataScope{scope}, Predicate: predicate})
						}
					}
					if err := principal.AccessBundle.Validate(time.Now().UTC()); err != nil {
						t.Error(err)
					}
					adapter.Handler().ServeHTTP(w, r.WithContext(identitysdk.WithRequestIdentity(r.Context(), identitysdk.RequestIdentity{Principal: principal})))
				}))
			}
			closeHost := func() {
				product.Close()
				if backend != nil {
					backend.Close()
					_ = service.Close(context.Background())
				}
				_ = binding.Close(context.Background())
				_ = database.Close()
			}
			open()
			t.Cleanup(func() { closeHost() })
			request := func(user, method, path string, body any, status int) []byte {
				t.Helper()
				raw, _ := json.Marshal(body)
				req, err := http.NewRequest(method, product.URL+path, bytes.NewReader(raw))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Authorization", "Bearer "+user)
				req.Header.Set("Content-Type", "application/json")
				response, err := product.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				data, _ := io.ReadAll(response.Body)
				if response.StatusCode != status {
					t.Fatalf("%s %s returned %d want %d: %s", method, path, response.StatusCode, status, data)
				}
				if status == 200 && strings.Contains(path, "oauth-") && response.Header.Get("Cache-Control") != "no-store" {
					t.Fatal("OAuth response may be cached")
				}
				if strings.Contains(string(data), "private-") {
					t.Fatal("OAuth HTTP response exposed credentials")
				}
				return data
			}
			appInput := integrationsdk.OAuthApplicationInput{ConnectorKey: google.ConnectorKey, ProviderKey: google.ProviderKey, Name: "Google calendar", ClientID: "test-client", ClientSecret: "private-client-secret", RedirectURI: "https://product.example.test/oauth/callback", Scopes: []string{"https://www.googleapis.com/auth/calendar.readonly"}, ConnectionConfig: map[string]any{"token_url": issuer.URL + "/token"}, Enabled: true}
			request("user-a", "PUT", "/integration/oauth-applications/google", appInput, 403)
			request("admin", "PUT", "/integration/oauth-applications/google", appInput, 200)
			options := request("user-a", "GET", "/integration/oauth-authorizations/options", nil, 200)
			if strings.Contains(string(options), "test-client") || strings.Contains(string(options), "token_url") {
				t.Fatal("user option catalog exposed client configuration")
			}
			start := func(code string) (integrationsdk.OAuthAuthorizationSession, integrationsdk.OAuthAuthorizationCallback) {
				t.Helper()
				raw := request("user-a", "POST", "/integration/oauth-authorizations?user_id=user-b&allow_workspace=true", integrationsdk.OAuthAuthorizationInput{ApplicationKey: "google", Name: "My calendar", Scope: integrationsdk.ConnectionAccountScopePersonal, Scopes: appInput.Scopes}, 200)
				var session integrationsdk.OAuthAuthorizationSession
				if err := json.Unmarshal(raw, &session); err != nil {
					t.Fatal(err)
				}
				target, _ := url.Parse(session.AuthorizationURL)
				if target.Host != "accounts.google.com" || target.Query().Get("code_challenge_method") != "S256" {
					t.Fatal("not the actual Google authorization protocol")
				}
				issuerMu.Lock()
				challenges[code] = target.Query().Get("code_challenge")
				issuerMu.Unlock()
				return session, integrationsdk.OAuthAuthorizationCallback{State: target.Query().Get("state"), Code: code}
			}
			session, callback := start("accepted-code")
			request("user-b", "GET", "/integration/oauth-authorizations/"+session.ID, nil, 400)
			request("user-b", "POST", "/integration/oauth-authorizations/callback", callback, 400)
			closeHost()
			open()
			raw := request("user-a", "POST", "/integration/oauth-authorizations/callback", callback, 200)
			var completed integrationsdk.OAuthAuthorizationSession
			if err := json.Unmarshal(raw, &completed); err != nil || completed.Status != "connected" || completed.Account == nil {
				t.Fatalf("callback=%s err=%v", raw, err)
			}
			replay := request("user-a", "POST", "/integration/oauth-authorizations/callback", callback, 200)
			if !bytes.Equal(raw, replay) {
				t.Fatal("repeat callback changed committed receipt")
			}
			closeHost()
			open()
			request("user-a", "POST", "/integration/oauth-authorizations/callback", callback, 200)
			issuerMu.Lock()
			count := calls
			issuerMu.Unlock()
			if count != 1 {
				t.Fatalf("code was exchanged %d times", count)
			}
			unknown, unknownCallback := start("unknown-code")
			raw = request("user-a", "POST", "/integration/oauth-authorizations/callback", unknownCallback, 200)
			if !bytes.Contains(raw, []byte("needs_reauthorization")) {
				t.Fatal("ambiguous exchange not shown explicitly")
			}
			request("user-a", "POST", "/integration/oauth-authorizations/callback", unknownCallback, 200)
			request("user-a", "GET", "/integration/oauth-authorizations/"+unknown.ID, nil, 200)
			issuerMu.Lock()
			count = calls
			issuerMu.Unlock()
			if count != 2 {
				t.Fatal("uncertain authorization code was retried")
			}
			accounts := request("user-a", "GET", "/integration/connection-accounts", nil, 200)
			var catalog struct {
				Count int `json:"count"`
			}
			json.Unmarshal(accounts, &catalog)
			if catalog.Count != 1 {
				t.Fatalf("account count=%s", accounts)
			}
			request("user-a", "POST", "/integration/connection-accounts/"+completed.Account.Key+"/revoke", map[string]string{"expected_updated_at": completed.Account.UpdatedAt}, 200)
			restored := request("user-a", "GET", "/integration/oauth-authorizations/"+session.ID, nil, 200)
			if !bytes.Contains(restored, []byte(`"status":"revoked"`)) {
				t.Fatal("session returned stale account state")
			}
			t.Log("Actual Google Provider + host Transport + local token HTTP: configuration authorization, PKCE, private ciphertext, cross-user rejection, two host restarts, single-use callback, uncertain result without retry, independent account and revoked receipt passed")
		})
	}
}
