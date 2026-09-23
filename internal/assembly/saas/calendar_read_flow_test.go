package saas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
	google "github.com/domainry/domainry-connectors/providers/google_workspace/google"
	microsoft "github.com/domainry/domainry-connectors/providers/microsoft_365/microsoft"
	"github.com/domainry/domainry-foundation/modulehttp"
	identity "github.com/domainry/domainry-identity-sdk"
	sdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/remote"
	moduleassembly "github.com/domainry/domainry-integration/internal/assembly/module"
	ormdialect "github.com/domainry/domainry-orm/dialect"
)

func TestCalendarReadOAuthHTTPModuleSaaSAndRestart(t *testing.T) {
	for _, vendor := range []string{"google", "microsoft"} {
		for _, mode := range []string{"module", "saas"} {
			t.Run(vendor+"/"+mode, func(t *testing.T) {
				var tokenCalls, readCalls atomic.Int32
				var allowRead atomic.Bool
				allowRead.Store(true)
				var challenge atomic.Value
				challenge.Store("")
				scope := "https://www.googleapis.com/auth/calendar.readonly"
				limitedScope := "https://www.googleapis.com/auth/calendar.freebusy"
				connectorKey, providerKey := google.ConnectorKey, google.ProviderKey
				if vendor == "microsoft" {
					scope = "Calendars.Read"
					limitedScope = "Calendars.ReadBasic"
					connectorKey, providerKey = microsoft.ConnectorKey, microsoft.ProviderKey
				}
				var grantedScope atomic.Value
				grantedScope.Store(scope)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.URL.Path == "/token" {
						tokenCalls.Add(1)
						if r.ParseForm() != nil || r.PostForm.Get("client_id") != "client" {
							t.Error("invalid OAuth request")
							w.WriteHeader(400)
							return
						}
						access, refresh := "private-stale", "private-refresh"
						if r.PostForm.Get("grant_type") == "authorization_code" {
							sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
							if base64.RawURLEncoding.EncodeToString(sum[:]) != challenge.Load().(string) {
								t.Error("PKCE mismatch")
								w.WriteHeader(400)
								return
							}
						} else {
							if r.PostForm.Get("refresh_token") != "private-refresh" {
								t.Error("refresh token was not owner resolved")
								w.WriteHeader(400)
								return
							}
							access, refresh = "private-fresh", "private-rotated"
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"access_token": access, "refresh_token": refresh, "token_type": "Bearer", "scope": grantedScope.Load().(string), "expires_in": 3600})
						return
					}
					readCalls.Add(1)
					if r.Header.Get("Authorization") == "Bearer private-stale" {
						w.WriteHeader(401)
						io.WriteString(w, `{}`)
						return
					}
					if r.Header.Get("Authorization") != "Bearer private-fresh" {
						t.Error("calendar bearer did not use persisted refresh")
						w.WriteHeader(401)
						return
					}
					var body string
					if vendor == "google" {
						switch {
						case strings.HasSuffix(r.URL.Path, "/calendarList"):
							body = `{"kind":"calendar#calendarList","items":[{"id":"team","summary":"团队日历","timeZone":"Asia/Shanghai","primary":true}]}`
						case strings.HasSuffix(r.URL.Path, "/freeBusy"):
							body = `{"timeMin":"2026-09-11T09:00:00Z","timeMax":"2026-09-11T18:00:00Z","calendars":{"team":{"busy":[{"start":"2026-09-11T10:00:00Z","end":"2026-09-11T11:00:00Z"}]}}}`
						case strings.HasSuffix(r.URL.Path, "/events/meeting"):
							body = `{"kind":"calendar#event","id":"meeting","summary":"评审","description":"sensitive event body","start":{"dateTime":"2026-09-11T10:00:00Z"},"end":{"dateTime":"2026-09-11T11:00:00Z"}}`
						case strings.HasSuffix(r.URL.Path, "/events"):
							body = `{"kind":"calendar#events","items":[{"id":"holiday","summary":"假期","start":{"date":"2026-09-11"},"end":{"date":"2026-09-12"}}]}`
						}
					} else {
						switch {
						case strings.HasSuffix(r.URL.Path, "/calendars"):
							body = `{"value":[{"id":"team","name":"团队日历","isDefaultCalendar":true}]}`
						case strings.HasSuffix(r.URL.Path, "/events/meeting"):
							body = `{"id":"meeting","subject":"评审","body":{"contentType":"text","content":"sensitive event body"},"isAllDay":false,"isCancelled":false,"showAs":"busy","start":{"dateTime":"2026-09-11T10:00:00","timeZone":"UTC"},"end":{"dateTime":"2026-09-11T11:00:00","timeZone":"UTC"}}`
						case strings.HasSuffix(r.URL.Path, "/calendarView"):
							if strings.Contains(r.URL.Query().Get("$select"), "subject") {
								body = `{"value":[{"id":"holiday","subject":"假期","isAllDay":true,"isCancelled":false,"originalStartTimeZone":"UTC","originalEndTimeZone":"UTC","start":{"dateTime":"2026-09-11T00:00:00","timeZone":"UTC"},"end":{"dateTime":"2026-09-12T00:00:00","timeZone":"UTC"}}]}`
							} else {
								body = `{"value":[{"id":"busy","isCancelled":false,"showAs":"busy","start":{"dateTime":"2026-09-11T10:00:00","timeZone":"UTC"},"end":{"dateTime":"2026-09-11T11:00:00","timeZone":"UTC"}}]}`
							}
						}
					}
					if body == "" {
						t.Error("unexpected calendar endpoint", r.URL.Path)
						w.WriteHeader(404)
						return
					}
					io.WriteString(w, body)
				}))
				defer upstream.Close()
				path := t.TempDir() + "/calendar.db"
				var db *sql.DB
				var binding sdk.Binding
				var owner *Service
				var backend, product *httptest.Server
				open := func() {
					var err error
					db, err = sql.Open("sqlite", path)
					if err != nil {
						t.Fatal(err)
					}
					db.SetMaxOpenConns(1)
					dialect, _ := ormdialect.New(ormdialect.SQLite)
					transport := accountReadFlowTransport{base: upstream.URL, client: upstream.Client()}
					var provider connector.Adapter
					if vendor == "google" {
						provider, err = google.New(transport)
					} else {
						provider, err = microsoft.New(transport)
					}
					if err != nil {
						t.Fatal(err)
					}
					registry := connector.NewRegistry()
					if err = registry.Register(provider); err != nil {
						t.Fatal(err)
					}
					registry.Freeze()
					host := oauthFlowHost{accountSaaSHost: accountSaaSHost{newTestHost(db, dialect.WithSchema(""))}, registry: registry}
					if mode == "module" {
						binding, err = moduleassembly.NewFactory().OpenModule(t.Context(), sdk.ApplicationRef{RuntimeID: "calendar-module"}, host)
					} else {
						owner, err = Open(t.Context(), sdk.ApplicationRef{RuntimeID: "calendar-service"}, host, "test-service-token")
						if err != nil {
							t.Fatal(err)
						}
						backend = httptest.NewServer(owner.Handler)
						binding, err = NewFactory(remote.NewFactory(remote.Options{BaseURL: backend.URL, Token: "test-service-token", HTTPClient: backend.Client()})).OpenSaaS(t.Context(), sdk.ApplicationRef{RuntimeID: "calendar-product"}, nil)
					}
					if err != nil {
						t.Fatal(err)
					}
					adapter := binding.(modulehttp.Provider).HTTPAdapters()[0]
					product = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						user := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
						p := saasAccountPrincipal("workspace-a", user, false)
						if allowRead.Load() {
							for _, action := range []string{"read_access", "read"} {
								p.AccessBundle.FunctionGrants = append(p.AccessBundle.FunctionGrants, identity.FunctionGrant{Resource: "integration.connection_accounts", Action: identity.Action(action), Effect: identity.EffectAllow})
								p.AccessBundle.DataPolicies = append(p.AccessBundle.DataPolicies, identity.DataPolicy{Key: "integration.connection_accounts." + action, Resource: "integration.connection_accounts", Action: identity.Action(action), Effect: identity.EffectAllow, DataScopes: []identity.DataScope{identity.DataScopeOwner}, Predicate: identity.Predicate{Fact: "owner_user_id", Operator: identity.OperatorEqual, Value: "$subject.id"}})
							}
						}
						adapter.Handler().ServeHTTP(w, r.WithContext(identity.WithRequestIdentity(r.Context(), identity.RequestIdentity{Principal: p})))
					}))
				}
				closeHost := func() {
					if product != nil {
						product.Close()
					}
					if backend != nil {
						backend.Close()
						_ = owner.Close(context.Background())
					}
					if binding != nil {
						_ = binding.Close(context.Background())
					}
					if db != nil {
						_ = db.Close()
					}
				}
				open()
				t.Cleanup(closeHost)
				config := map[string]any{"token_url": upstream.URL + "/token", "api_base_url": upstream.URL}
				if vendor == "microsoft" {
					config = map[string]any{"token_url": upstream.URL + "/token", "graph_base_url": upstream.URL + "/v1.0", "tenant_id": "tenant"}
				}
				_, err := binding.(sdk.OAuthApplicationsBinding).OAuthApplications().UpsertOAuthApplication(t.Context(), "workspace-a", "calendar", "admin", sdk.OAuthApplicationInput{ConnectorKey: connectorKey, ProviderKey: providerKey, Name: "Calendar", ClientID: "client", ClientSecret: "private-client-secret", RedirectURI: "https://product.example.test/callback", Scopes: []string{scope, limitedScope}, ConnectionConfig: config, Enabled: true})
				if err != nil {
					t.Fatal(err)
				}
				subject := sdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: sdk.ConnectionAccountAccess{Personal: true}}
				oauth := binding.(sdk.OAuthAuthorizationsBinding).OAuthAuthorizations()
				session, err := oauth.StartOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationInput{ApplicationKey: "calendar", Scope: sdk.ConnectionAccountScopePersonal, Scopes: []string{scope}})
				if err != nil {
					t.Fatal(err)
				}
				authorization, _ := url.Parse(session.AuthorizationURL)
				challenge.Store(authorization.Query().Get("code_challenge"))
				completed, err := oauth.CompleteOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationCallback{State: authorization.Query().Get("state"), Code: "code"})
				if err != nil || completed.Account == nil {
					t.Fatal(completed, err)
				}
				account := completed.Account.Key
				request := func(user, path string, input any, status int) []byte {
					t.Helper()
					raw, _ := json.Marshal(input)
					r, _ := http.NewRequest("POST", product.URL+"/integration/connection-accounts/"+url.PathEscape(account)+path, bytes.NewReader(raw))
					r.Header.Set("Authorization", "Bearer "+user)
					r.Header.Set("Content-Type", "application/json")
					response, err := product.Client().Do(r)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					b, _ := io.ReadAll(response.Body)
					if response.StatusCode != status {
						t.Fatalf("%s got %d want %d (provider reads=%d token calls=%d): %s", path, response.StatusCode, status, readCalls.Load(), tokenCalls.Load(), b)
					}
					if status == 200 && response.Header.Get("Cache-Control") != "no-store" {
						t.Fatal("account read was cacheable")
					}
					if strings.Contains(string(b), "private-") || status != 200 && strings.Contains(string(b), "sensitive event body") {
						t.Fatal("read response leaked credentials or unauthorized event")
					}
					return b
				}
				lookup := sdk.ConnectionAccountReadRequest{RequestID: "list-1", Operation: calendar.ListOperationKey, ContractSHA256: calendar.OperationSHA256(calendar.ListOperationKey), Payload: json.RawMessage(`{}`)}
				request("user-b", "/read?user_id=user-a&allow_personal=true", lookup, 400)
				request("user-a", "/read", map[string]any{"request_id": "forged", "operation": lookup.Operation, "contract_sha256": lookup.ContractSHA256, "payload": map[string]any{}, "scopes": []string{scope}}, 400)
				if readCalls.Load() != 0 {
					t.Fatal("unauthorized read reached upstream")
				}
				request("user-a", "/read-access", lookup.OperationContract(), 200)
				var listed sdk.ConnectionAccountReadResult
				_ = json.Unmarshal(request("user-a", "/read", lookup, 200), &listed)
				var calendars calendar.CalendarsPage
				_ = json.Unmarshal(listed.Payload, &calendars)
				if !listed.PayloadAvailable || len(calendars.Items) != 1 || calendars.Items[0].ID != "team" || tokenCalls.Load() != 2 || readCalls.Load() != 2 {
					t.Fatal(listed, calendars, tokenCalls.Load(), readCalls.Load())
				}
				window := calendar.Window{Start: "2026-09-11T09:00:00Z", End: "2026-09-11T18:00:00Z"}
				for _, test := range []struct {
					key   string
					input any
				}{{calendar.EventsOperationKey, calendar.EventsRequest{CalendarID: "team", Window: window, TimeZone: "Asia/Shanghai"}}, {calendar.EventOperationKey, calendar.EventRequest{CalendarID: "team", EventID: "meeting", TimeZone: "Asia/Shanghai"}}, {calendar.AvailabilityOperationKey, calendar.AvailabilityRequest{CalendarIDs: []string{"team"}, Window: window, TimeZone: "UTC"}}} {
					payload, _ := json.Marshal(test.input)
					var out sdk.ConnectionAccountReadResult
					_ = json.Unmarshal(request("user-a", "/read", sdk.ConnectionAccountReadRequest{RequestID: test.key, Operation: test.key, ContractSHA256: calendar.OperationSHA256(test.key), Payload: payload}, 200), &out)
					if !out.PayloadAvailable {
						t.Fatal("missing read payload")
					}
					switch test.key {
					case calendar.EventsOperationKey:
						var page calendar.EventsPage
						_ = json.Unmarshal(out.Payload, &page)
						if len(page.Items) != 1 || page.Items[0].Start.Date != "2026-09-11" || page.Items[0].End.Date != "2026-09-12" {
							t.Fatal(page)
						}
					case calendar.EventOperationKey:
						var event calendar.Event
						_ = json.Unmarshal(out.Payload, &event)
						if event.Description != "sensitive event body" {
							t.Fatal(event)
						}
					case calendar.AvailabilityOperationKey:
						var free calendar.Availability
						_ = json.Unmarshal(out.Payload, &free)
						if !free.Complete || len(free.Free) != 2 {
							t.Fatal(free)
						}
					}
				}
				calls := readCalls.Load()
				allowRead.Store(false)
				request("user-a", "/read", lookup, 403)
				allowRead.Store(true)
				closeHost()
				open()
				var replay sdk.ConnectionAccountReadResult
				_ = json.Unmarshal(request("user-a", "/read", lookup, 200), &replay)
				if replay.PayloadAvailable || replay.InvocationID != listed.InvocationID || readCalls.Load() != calls {
					t.Fatal("restart replay repeated read or exposed stored body")
				}
				lookup.RequestID = "after-restart"
				request("user-a", "/read", lookup, 200)
				if tokenCalls.Load() != 2 {
					t.Fatal("refreshed credentials were not durable")
				}
				// A second actual OAuth session grants only busy/basic access. The
				// same declared account-read API must allow availability while
				// rejecting event body access before vendor I/O in both topologies.
				grantedScope.Store(limitedScope)
				oauth = binding.(sdk.OAuthAuthorizationsBinding).OAuthAuthorizations()
				limited, err := oauth.StartOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationInput{ApplicationKey: "calendar", Scope: sdk.ConnectionAccountScopePersonal, Scopes: []string{limitedScope}})
				if err != nil {
					t.Fatal(err)
				}
				u, _ := url.Parse(limited.AuthorizationURL)
				challenge.Store(u.Query().Get("code_challenge"))
				limited, err = oauth.CompleteOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationCallback{State: u.Query().Get("state"), Code: "limited-code"})
				if err != nil || limited.Account == nil {
					t.Fatal(limited, err)
				}
				primaryAccount := account
				account = limited.Account.Key
				calls = readCalls.Load()
				body, _ := json.Marshal(calendar.EventRequest{CalendarID: "team", EventID: "meeting", TimeZone: "UTC"})
				request("user-a", "/read", sdk.ConnectionAccountReadRequest{RequestID: "limited-detail", Operation: calendar.EventOperationKey, ContractSHA256: calendar.OperationSHA256(calendar.EventOperationKey), Payload: body}, 400)
				if readCalls.Load() != calls {
					t.Fatal("limited grant reached event body endpoint")
				}
				body, _ = json.Marshal(calendar.AvailabilityRequest{CalendarIDs: []string{"team"}, Window: window, TimeZone: "UTC"})
				var limitedRead sdk.ConnectionAccountReadResult
				_ = json.Unmarshal(request("user-a", "/read", sdk.ConnectionAccountReadRequest{RequestID: "limited-busy", Operation: calendar.AvailabilityOperationKey, ContractSHA256: calendar.OperationSHA256(calendar.AvailabilityOperationKey), Payload: body}, 200), &limitedRead)
				if !limitedRead.PayloadAvailable {
					t.Fatal("limited grant lost availability")
				}
				account = primaryAccount
				invocations, err := binding.(sdk.OperationsBinding).Operations().ListInvocations(t.Context(), sdk.InvocationQuery{WorkspaceID: "workspace-a"})
				if err != nil {
					t.Fatal(err)
				}
				evidence, _ := json.Marshal(invocations)
				if strings.Contains(string(evidence), "sensitive event body") || strings.Contains(string(evidence), "private-") {
					t.Fatal("calendar data entered owner invocation evidence")
				}
				accounts := binding.(sdk.ConnectionAccountsBinding).ConnectionAccounts()
				current, err := accounts.GetConnectionAccount(t.Context(), subject, account)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := accounts.RevokeConnectionAccount(t.Context(), subject, account, current.UpdatedAt); err != nil {
					t.Fatal(err)
				}
				calls = readCalls.Load()
				request("user-a", "/read", lookup, 400)
				if readCalls.Load() != calls {
					t.Fatal("revoked account reached upstream")
				}
				t.Log("real HTTP + SQLite + actual Provider + OAuth PKCE + private refresh; four calendar reads; current-user denial; restart and sensitive replay; revocation passed")
			})
		}
	}
}
