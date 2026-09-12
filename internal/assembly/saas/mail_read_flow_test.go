package saas

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	mail "github.com/domainry/domainry-connector-sdk/mail"
	google "github.com/domainry/domainry-connectors/providers/google_workspace/google"
	microsoft "github.com/domainry/domainry-connectors/providers/microsoft_365/microsoft"
	sdk "github.com/domainry/domainry-integration-sdk"
)

func TestMailReadOAuthHTTPModuleSaaSAndRestart(t *testing.T) {
	for _, vendor := range []string{"google", "microsoft"} {
		for _, mode := range []string{"module", "saas"} {
			t.Run(vendor+"/"+mode, func(t *testing.T) {
				var tokenCalls, readCalls atomic.Int32
				var challenge, granted atomic.Value
				challenge.Store("")
				var missingBody atomic.Bool
				scope, basic, syntax := "https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.metadata", mail.GmailSyntax
				connectorKey, providerKey := google.ConnectorKey, google.ProviderKey
				constructor := google.New
				if vendor == "microsoft" {
					scope, basic, syntax = "Mail.Read", "Mail.ReadBasic", mail.GraphSyntax
					connectorKey, providerKey = microsoft.ConnectorKey, microsoft.ProviderKey
					constructor = microsoft.New
				}
				granted.Store(scope)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.URL.Path == "/token" {
						tokenCalls.Add(1)
						if r.ParseForm() != nil || r.PostForm.Get("client_id") != "client" {
							t.Error("OAuth client missing")
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
								t.Error("owner refresh credential missing")
								w.WriteHeader(400)
								return
							}
							access, refresh = "private-fresh", "private-rotated"
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"access_token": access, "refresh_token": refresh, "token_type": "Bearer", "scope": granted.Load().(string), "expires_in": 3600})
						return
					}
					readCalls.Add(1)
					if r.Method != "GET" {
						t.Error("mail wrote upstream")
						w.WriteHeader(405)
						return
					}
					if r.Header.Get("Authorization") == "Bearer private-stale" {
						w.WriteHeader(401)
						return
					}
					if r.Header.Get("Authorization") != "Bearer private-fresh" {
						t.Error("private credential missing")
						w.WriteHeader(401)
						return
					}
					q := r.URL.Query()
					id := "mail-1"
					if vendor == "google" {
						if r.URL.Path == "/gmail/v1/users/me/messages" {
							if q.Get("maxResults") != "2" || q.Get("includeSpamTrash") != "false" {
								t.Error("unbounded Gmail list")
							}
							if q.Get("pageToken") != "" {
								id = "mail-2"
							}
							body := map[string]any{"messages": []map[string]string{{"id": id, "threadId": "thread"}}, "resultSizeEstimate": 2}
							if q.Get("q") != "" && q.Get("pageToken") == "" {
								body["nextPageToken"] = "page2"
							}
							_ = json.NewEncoder(w).Encode(body)
							return
						}
						if !strings.HasPrefix(r.URL.Path, "/gmail/v1/users/me/messages/mail-") {
							t.Error("unexpected Gmail endpoint")
							w.WriteHeader(404)
							return
						}
						id = strings.TrimPrefix(r.URL.Path, "/gmail/v1/users/me/messages/")
						payload := map[string]any{"mimeType": "text/plain", "headers": []map[string]string{{"name": "From", "value": "sender@example.test"}, {"name": "To", "value": "recipient@example.test"}, {"name": "Reply-To", "value": "reply@example.test"}, {"name": "Subject", "value": "Review"}, {"name": "Message-ID", "value": "<original@example.test>"}, {"name": "Date", "value": "Fri, 11 Sep 2026 09:15:00 +0800"}}}
						if q.Get("format") == "full" && !missingBody.Load() {
							text := "SENSITIVE-MAIL-BODY: reply by Friday"
							payload["body"] = map[string]any{"size": len(text), "data": base64.RawURLEncoding.EncodeToString([]byte(text))}
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "threadId": "thread", "internalDate": "1789089300000", "labelIds": []string{"INBOX"}, "payload": payload})
						return
					}
					if !strings.Contains(r.Header.Get("Prefer"), `IdType="ImmutableId"`) {
						t.Error("Graph unstable identity")
					}
					if q.Get("$skiptoken") != "" {
						id = "mail-2"
					}
					message := map[string]any{"id": id, "conversationId": "thread", "internetMessageId": "<original@example.test>", "subject": "Review", "from": map[string]any{"emailAddress": map[string]string{"address": "sender@example.test"}}, "toRecipients": []any{}, "ccRecipients": []any{}, "replyTo": []any{map[string]any{"emailAddress": map[string]string{"address": "reply@example.test"}}}, "receivedDateTime": "2026-09-11T01:15:00Z", "sentDateTime": "2026-09-11T09:15:00+08:00", "isRead": true, "isDraft": false}
					if r.URL.Path == "/v1.0/me/messages" {
						if q.Get("$top") != "2" || strings.Contains(q.Get("$select"), "body") {
							t.Error("unbounded Graph list")
						}
						body := map[string]any{"value": []any{message}}
						if q.Get("$search") != "" && q.Get("$skiptoken") == "" {
							q.Set("$skiptoken", "page2")
							next := url.URL{Scheme: "http", Host: r.Host, Path: r.URL.Path, RawQuery: q.Encode()}
							body["@odata.nextLink"] = next.String()
						}
						_ = json.NewEncoder(w).Encode(body)
						return
					}
					if r.URL.Path != "/v1.0/me/messages/mail-1" || !strings.Contains(r.Header.Get("Prefer"), `outlook.body-content-type="text"`) {
						t.Error("unexpected Graph detail")
						w.WriteHeader(404)
						return
					}
					if !missingBody.Load() {
						message["body"] = map[string]string{"contentType": "text", "content": "SENSITIVE-MAIL-BODY: reply by Friday"}
					}
					_ = json.NewEncoder(w).Encode(message)
				}))
				t.Cleanup(upstream.Close)
				f := &accountReadFlowFixture{t: t, mode: mode, path: t.TempDir() + "/mail.db", upstream: upstream, provider: constructor}
				f.allowRead.Store(true)
				t.Cleanup(f.close)
				f.open()
				config := map[string]any{"token_url": upstream.URL + "/token", "gmail_base_url": upstream.URL}
				if vendor == "microsoft" {
					config = map[string]any{"token_url": upstream.URL + "/token", "graph_base_url": upstream.URL + "/v1.0", "tenant_id": "tenant"}
				}
				_, err := f.binding.(sdk.OAuthApplicationsBinding).OAuthApplications().UpsertOAuthApplication(t.Context(), "workspace-a", "mail", "admin", sdk.OAuthApplicationInput{ConnectorKey: connectorKey, ProviderKey: providerKey, Name: "Mail", ClientID: "client", ClientSecret: "private-client-secret", RedirectURI: "https://product.example.test/callback", Scopes: []string{scope, basic}, ConnectionConfig: config, Enabled: true})
				if err != nil {
					t.Fatal(err)
				}
				subject := sdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: sdk.ConnectionAccountAccess{Personal: true}}
				connect := func(scope string) string {
					t.Helper()
					granted.Store(scope)
					oauth := f.binding.(sdk.OAuthAuthorizationsBinding).OAuthAuthorizations()
					session, err := oauth.StartOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationInput{ApplicationKey: "mail", Scope: sdk.ConnectionAccountScopePersonal, Scopes: []string{scope}})
					if err != nil {
						t.Fatal(err)
					}
					u, _ := url.Parse(session.AuthorizationURL)
					challenge.Store(u.Query().Get("code_challenge"))
					completed, err := oauth.CompleteOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationCallback{State: u.Query().Get("state"), Code: "code"})
					if err != nil || completed.Account == nil {
						t.Fatal(completed, err)
					}
					return completed.Account.Key
				}
				account := connect(scope)
				makeRead := func(id, key string, payload any) sdk.ConnectionAccountReadRequest {
					b, _ := json.Marshal(payload)
					return sdk.ConnectionAccountReadRequest{RequestID: id, Operation: key, ContractSHA256: mail.OperationSHA256(key), Payload: b}
				}
				read := func(account string, in sdk.ConnectionAccountReadRequest) sdk.ConnectionAccountReadResult {
					t.Helper()
					var out sdk.ConnectionAccountReadResult
					if err := json.Unmarshal(f.request("user-a", account, "/read", in, 200), &out); err != nil {
						t.Fatal(err)
					}
					return out
				}
				lookup := makeRead("list-1", mail.ListOperationKey, mail.PageRequest{Limit: 2})
				f.request("user-b", account, "/read", lookup, 400)
				f.request("user-b", account, "/read?user_id=user-a&allow_personal=true", lookup, 400)
				f.request("user-a", account, "/read", map[string]any{"request_id": "forged", "operation": lookup.Operation, "contract_sha256": lookup.ContractSHA256, "payload": map[string]any{}, "scopes": []string{scope}}, 400)
				if readCalls.Load() != 0 {
					t.Fatal("denied mailbox request dispatched")
				}
				f.request("user-a", account, "/read-access", lookup.OperationContract(), 200)
				listed := read(account, lookup)
				var page mail.MessagesPage
				_ = json.Unmarshal(listed.Payload, &page)
				if !listed.PayloadAvailable || page.Validate(2) != nil || len(page.Items) != 1 || page.Items[0].ID != "mail-1" || tokenCalls.Load() != 2 {
					t.Fatal(listed, page, tokenCalls.Load())
				}
				search := mail.SearchRequest{Query: "subject:review", QuerySyntax: syntax, Limit: 2}
				searched := read(account, makeRead("search-1", mail.SearchOperationKey, search))
				_ = json.Unmarshal(searched.Payload, &page)
				if page.Complete || page.NextCursor == "" {
					t.Fatal(page)
				}
				search.Cursor = page.NextCursor
				searched = read(account, makeRead("search-2", mail.SearchOperationKey, search))
				_ = json.Unmarshal(searched.Payload, &page)
				if !page.Complete || len(page.Items) != 1 || page.Items[0].ID != "mail-2" {
					t.Fatal(page)
				}
				detail := makeRead("detail", mail.ReadOperationKey, mail.ReadRequest{MessageID: "mail-1"})
				messageResult := read(account, detail)
				var message mail.Message
				_ = json.Unmarshal(messageResult.Payload, &message)
				if !message.Body.Complete || !strings.Contains(message.Body.Text, "SENSITIVE-MAIL-BODY") || message.Summary.InternetMessageID != "<original@example.test>" || len(message.Summary.ReplyTo) != 1 {
					t.Fatal(message)
				}
				missingBody.Store(true)
				partial := read(account, makeRead("partial", mail.ReadOperationKey, mail.ReadRequest{MessageID: "mail-1"}))
				_ = json.Unmarshal(partial.Payload, &message)
				if message.Body.Complete || len(message.Body.OmittedReasons) == 0 {
					t.Fatal("missing body declared complete", message)
				}
				missingBody.Store(false)
				calls := readCalls.Load()
				f.allowRead.Store(false)
				f.request("user-a", account, "/read", detail, 403)
				f.allowRead.Store(true)
				if readCalls.Load() != calls {
					t.Fatal("revoked function permission dispatched")
				}
				f.close()
				f.open()
				replay := read(account, detail)
				if replay.PayloadAvailable || replay.InvocationID != messageResult.InvocationID || readCalls.Load() != calls {
					t.Fatal("restart replay disclosed persisted mail or called upstream")
				}
				detail.RequestID = "detail-after-restart"
				read(account, detail)
				if tokenCalls.Load() != 2 {
					t.Fatal("refresh was not persisted")
				}
				basicAccount := connect(basic)
				calls = readCalls.Load()
				for _, in := range []sdk.ConnectionAccountReadRequest{makeRead("basic-body", mail.ReadOperationKey, mail.ReadRequest{MessageID: "mail-1"}), makeRead("basic-search", mail.SearchOperationKey, mail.SearchRequest{Query: "report", QuerySyntax: syntax, Limit: 2})} {
					f.request("user-a", basicAccount, "/read-access", in.OperationContract(), 400)
					f.request("user-a", basicAccount, "/read", in, 400)
				}
				if readCalls.Load() != calls {
					t.Fatal("basic scope reached search/body")
				}
				basicList := read(basicAccount, makeRead("basic-list", mail.ListOperationKey, mail.PageRequest{Limit: 2}))
				_ = json.Unmarshal(basicList.Payload, &page)
				if !basicList.PayloadAvailable || len(page.Items) != 1 {
					t.Fatal("basic scope lost header listing", page)
				}
				invocations, err := f.binding.(sdk.OperationsBinding).Operations().ListInvocations(t.Context(), sdk.InvocationQuery{WorkspaceID: "workspace-a"})
				if err != nil {
					t.Fatal(err)
				}
				evidence, _ := json.Marshal(invocations)
				for _, secret := range []string{"SENSITIVE-MAIL", "subject:review", "private-", "sender@example.test", "original@example.test"} {
					if strings.Contains(string(evidence), secret) {
						t.Fatal("mail data persisted in owner invocation", secret)
					}
				}
				accounts := f.binding.(sdk.ConnectionAccountsBinding).ConnectionAccounts()
				current, err := accounts.GetConnectionAccount(t.Context(), subject, account)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = accounts.RevokeConnectionAccount(t.Context(), subject, account, current.UpdatedAt); err != nil {
					t.Fatal(err)
				}
				calls = readCalls.Load()
				f.request("user-a", account, "/read", detail, 400)
				if readCalls.Load() != calls {
					t.Fatal("revoked account dispatched")
				}
				t.Logf("%s/%s actual Provider, OAuth PKCE/private refresh, HTTP/SQLite, mail list/search/page/body/partial, basic denial, restart/replay and revocation: token_http=%d read_http=%d", vendor, mode, tokenCalls.Load(), readCalls.Load())
			})
		}
	}
}
