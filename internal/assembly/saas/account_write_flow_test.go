package saas

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/domainry/domainry-connector-sdk/calendar"
	"github.com/domainry/domainry-connector-sdk/calendarwrite"
	maildto "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
	google "github.com/domainry/domainry-connectors/providers/google_workspace/google"
	microsoft "github.com/domainry/domainry-connectors/providers/microsoft_365/microsoft"
	sdk "github.com/domainry/domainry-integration-sdk"
)

func TestAccountWriteOAuthHTTPModuleSaaSAndRestart(t *testing.T) {
	for _, vendor := range []string{"google", "microsoft"} {
		for _, mode := range []string{"module", "saas"} {
			t.Run(vendor+"/"+mode, func(t *testing.T) {
				up := &writeFlowUpstream{t: t, vendor: vendor}
				up.challenge.Store("")
				constructor, connectorKey, providerKey := google.New, google.ConnectorKey, google.ProviderKey
				up.scopes = []string{"https://www.googleapis.com/auth/calendar.events", "https://www.googleapis.com/auth/gmail.modify"}
				if vendor == "microsoft" {
					constructor, connectorKey, providerKey = microsoft.New, microsoft.ConnectorKey, microsoft.ProviderKey
					up.scopes = []string{"Calendars.ReadWrite", "User.Read", "Mail.Read", "Mail.Send"}
				}
				server := httptest.NewServer(http.HandlerFunc(up.serve))
				t.Cleanup(server.Close)
				f := &accountReadFlowFixture{t: t, mode: mode, path: t.TempDir() + "/writes.db", upstream: server, provider: constructor}
				t.Cleanup(f.close)
				f.open()
				config := map[string]any{"token_url": server.URL + "/token", "api_base_url": server.URL, "gmail_base_url": server.URL}
				if vendor == "microsoft" {
					config = map[string]any{"token_url": server.URL + "/token", "graph_base_url": server.URL + "/v1.0", "tenant_id": "tenant"}
				}
				_, err := f.binding.(sdk.OAuthApplicationsBinding).OAuthApplications().UpsertOAuthApplication(t.Context(), "workspace-a", "write-app", "admin", sdk.OAuthApplicationInput{ConnectorKey: connectorKey, ProviderKey: providerKey, Name: "Write fixture", ClientID: "client", ClientSecret: "private-client", RedirectURI: "https://product.example.test/callback", Scopes: up.scopes, ConnectionConfig: config, Enabled: true})
				if err != nil {
					t.Fatal(err)
				}
				subject := sdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: sdk.ConnectionAccountAccess{Personal: true}}
				oauth := f.binding.(sdk.OAuthAuthorizationsBinding).OAuthAuthorizations()
				session, err := oauth.StartOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationInput{ApplicationKey: "write-app", Scope: sdk.ConnectionAccountScopePersonal, Scopes: up.scopes})
				if err != nil {
					t.Fatal(err)
				}
				u, _ := url.Parse(session.AuthorizationURL)
				up.challenge.Store(u.Query().Get("code_challenge"))
				completed, err := oauth.CompleteOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationCallback{State: u.Query().Get("state"), Code: "code"})
				if err != nil || completed.Account == nil {
					t.Fatal(completed, err)
				}
				key := completed.Account.Key
				port := func() sdk.ConnectionAccountWrites {
					return f.binding.(sdk.ConnectionAccountWritesBinding).ConnectionAccountWrites()
				}
				request := func(id, operation string, payload any) sdk.ConnectionAccountWriteRequest {
					t.Helper()
					hash := calendarwrite.OperationSHA256(operation)
					if hash == "" {
						hash = mailwrite.OperationSHA256(operation)
					}
					access, err := port().AuthorizeConnectionAccountWrite(t.Context(), subject, key, sdk.ConnectionAccountWriteOperation{Operation: operation, ContractSHA256: hash})
					if err != nil {
						t.Fatal(err)
					}
					raw, _ := json.Marshal(payload)
					return sdk.ConnectionAccountWriteRequest{RequestID: id, ExpectedSource: access.Source, Payload: raw}
				}
				write := func(in sdk.ConnectionAccountWriteRequest, status string) sdk.ConnectionAccountWriteResult {
					t.Helper()
					out, err := port().WriteConnectionAccount(t.Context(), subject, key, in)
					if err != nil || out.Status != status {
						t.Fatal("write", in.ExpectedSource.Operation, out, err)
					}
					return out
				}
				create := request("create-1", calendarwrite.CreateOperationKey, calendarwrite.CreateRequest{CalendarID: "team", Notifications: calendarwrite.NotifyAttendees, Event: calendarwrite.Draft{Title: "PRIVATE-EVENT-TITLE", Description: "PRIVATE-EVENT-BODY", Location: "Room", Start: calendar.Moment{DateTime: "2026-09-12T09:00:00Z", TimeZone: "UTC"}, End: calendar.Moment{DateTime: "2026-09-12T10:00:00Z", TimeZone: "UTC"}, Attendees: []calendarwrite.Attendee{{Address: "guest@example.test", Kind: "required"}}}})
				foreign := subject
				foreign.UserID = "user-b"
				if _, err := port().WriteConnectionAccount(t.Context(), foreign, key, create); err == nil || up.calls.Load() != 0 {
					t.Fatal("foreign subject reached vendor")
				}
				f.request("user-a", key, "/write", create, http.StatusNotFound)
				created := write(create, sdk.AccountWriteSucceeded)
				var event calendarwrite.Result
				if json.Unmarshal(created.Receipt, &event) != nil || event.CalendarID != "team" || event.EventID == "" || up.tokens.Load() != 2 {
					t.Fatal(event, up.tokens.Load())
				}
				clear := ""
				update := request("update-1", calendarwrite.UpdateOperationKey, calendarwrite.UpdateRequest{CalendarID: "team", EventID: event.EventID, ExpectedVersion: event.Version, Scope: calendarwrite.ScopeEvent, Notifications: calendarwrite.NotifyAttendees, Changes: calendarwrite.Patch{Location: &clear}})
				updated := write(update, sdk.AccountWriteSucceeded)
				var changed calendarwrite.Result
				if json.Unmarshal(updated.Receipt, &changed) != nil || changed.EventID != event.EventID || changed.Version == event.Version {
					t.Fatal(changed)
				}
				message := mailwrite.Message{To: []maildto.Address{{Address: "reply@example.test"}}, CC: []maildto.Address{}, BCC: []maildto.Address{{Address: "hidden@example.test"}}, Subject: "Owner test", Text: "PRIVATE-MAIL-BODY"}
				send := request("send-1", mailwrite.SendOperationKey, mailwrite.SendRequest{Message: message})
				sent := write(send, sdk.AccountWriteSucceeded)
				message.Subject = "Re: Owner test"
				reply := request("reply-1", mailwrite.ReplyOperationKey, mailwrite.ReplyRequest{MessageID: "original", Message: message})
				replied := write(reply, sdk.AccountWriteSucceeded)
				for _, value := range []sdk.ConnectionAccountWriteResult{sent, replied} {
					var receipt mailwrite.Result
					if json.Unmarshal(value.Receipt, &receipt) != nil || receipt.Status != "accepted" || receipt.Delivery != "unknown" || vendor == "microsoft" && receipt.MessageID != "" {
						t.Fatal(receipt)
					}
				}
				before := up.calls.Load()
				// Close all handles and reopen the same disk database and SDK binding.
				f.close()
				f.open()
				for _, in := range []sdk.ConnectionAccountWriteRequest{create, update, send, reply} {
					out, err := port().ReadConnectionAccountWriteReceipt(t.Context(), subject, key, in)
					if err != nil || out.Status != sdk.AccountWriteSucceeded {
						t.Fatal("restart receipt", out, err)
					}
					write(in, sdk.AccountWriteSucceeded)
				}
				if up.calls.Load() != before {
					t.Fatal("restart replayed vendor I/O")
				}
				conflict := send
				conflict.Payload = json.RawMessage(strings.Replace(string(send.Payload), "PRIVATE-MAIL-BODY", "changed body", 1))
				if _, err := port().WriteConnectionAccount(t.Context(), subject, key, conflict); err == nil || up.calls.Load() != before {
					t.Fatal("changed request reused identity")
				}
				if _, err := port().ReadConnectionAccountWriteReceipt(t.Context(), subject, key, conflict); err == nil || up.calls.Load() != before {
					t.Fatal("read receipt accepted changed original payload or performed vendor IO")
				}
				missing := send
				missing.RequestID = "never-executed-read-only"
				if result, err := port().ReadConnectionAccountWriteReceipt(t.Context(), subject, key, missing); err != nil || result.Status != sdk.AccountWriteNotFound || up.calls.Load() != before {
					t.Fatal("read receipt created an operation", result, err)
				}
				other := subject
				other.UserID = "other-receipt-reader"
				if result, err := port().ReadConnectionAccountWriteReceipt(t.Context(), other, key, send); err == nil && result.Status == sdk.AccountWriteSucceeded || up.calls.Load() != before {
					t.Fatal("read receipt crossed original actor boundary", result, err)
				}
				up.conflict.Store(true)
				cas := request("update-conflict", calendarwrite.UpdateOperationKey, calendarwrite.UpdateRequest{CalendarID: "team", EventID: event.EventID, ExpectedVersion: changed.Version, Scope: calendarwrite.ScopeEvent, Notifications: calendarwrite.NotifyAttendees, Changes: calendarwrite.Patch{Location: &clear}})
				write(cas, sdk.AccountWriteFailed)
				write(cas, sdk.AccountWriteFailed)
				up.loseSend.Store(true)
				lost := send
				lost.RequestID = "send-disconnected"
				write(lost, sdk.AccountWriteUncertain)
				before = up.calls.Load()
				f.close()
				f.open()
				write(lost, sdk.AccountWriteUncertain)
				if up.calls.Load() != before || up.creates.Load() != 1 || up.patches.Load() != 2 || up.sends.Load() != 3 || up.tokens.Load() != 2 {
					t.Fatal("unexpected replay", up.calls.Load(), up.creates.Load(), up.patches.Load(), up.sends.Load(), up.tokens.Load())
				}
				// Invocation rows retain neither request content nor provider debug data.
				rows, err := f.db.QueryContext(t.Context(), "SELECT metadata_json, COALESCE(response_ref,''), COALESCE(error,'') FROM _integration_invocations")
				if err != nil {
					t.Fatal(err)
				}
				for rows.Next() {
					var metadata, response, failure string
					if err := rows.Scan(&metadata, &response, &failure); err != nil {
						t.Fatal(err)
					}
					for _, text := range []string{"PRIVATE-EVENT", "PRIVATE-MAIL", "guest@example.test", "reply@example.test", "hidden@example.test", "private-fresh", "private-refresh"} {
						if strings.Contains(metadata+response+failure, text) {
							t.Fatal("private content persisted", text)
						}
					}
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				rows.Close()
				accounts := f.binding.(sdk.ConnectionAccountsBinding).ConnectionAccounts()
				account, err := accounts.GetConnectionAccount(t.Context(), subject, key)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := accounts.RevokeConnectionAccount(t.Context(), subject, key, account.UpdatedAt); err != nil {
					t.Fatal(err)
				}
				if _, err := port().ReadConnectionAccountWriteReceipt(t.Context(), subject, key, send); err == nil || up.calls.Load() != before {
					t.Fatal("revoked account exposed receipt")
				}
				t.Logf("%s/%s: OAuth HTTP=%d, vendor HTTP=%d, create=%d PATCH=%d mail=%d; two disk restarts, exact receipt recovery, conflict and unknown write never replayed", vendor, mode, up.tokens.Load(), up.calls.Load(), up.creates.Load(), up.patches.Load(), up.sends.Load())
			})
		}
	}
}

type writeFlowUpstream struct {
	t                                      *testing.T
	vendor                                 string
	scopes                                 []string
	challenge                              atomic.Value
	tokens, calls, creates, patches, sends atomic.Int32
	loseSend, conflict                     atomic.Bool
	mu                                     sync.Mutex
	event                                  map[string]any
}

func (s *writeFlowUpstream) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/token" {
		s.tokens.Add(1)
		if r.ParseForm() != nil || r.PostForm.Get("client_id") != "client" {
			s.t.Error("invalid OAuth client")
			w.WriteHeader(400)
			return
		}
		access, refresh := "private-stale", "private-refresh"
		if r.PostForm.Get("grant_type") == "authorization_code" {
			digest := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
			if base64.RawURLEncoding.EncodeToString(digest[:]) != s.challenge.Load().(string) {
				s.t.Error("PKCE mismatch")
				w.WriteHeader(400)
				return
			}
		} else {
			if r.PostForm.Get("refresh_token") != refresh {
				s.t.Error("refresh not owner resolved")
				w.WriteHeader(400)
				return
			}
			access, refresh = "private-fresh", "private-rotated"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": access, "refresh_token": refresh, "token_type": "Bearer", "scope": strings.Join(s.scopes, " "), "expires_in": 3600})
		return
	}
	s.calls.Add(1)
	if r.Header.Get("Authorization") == "Bearer private-stale" {
		w.WriteHeader(401)
		return
	}
	if r.Header.Get("Authorization") != "Bearer private-fresh" {
		s.t.Error("fresh credential missing")
		w.WriteHeader(403)
		return
	}
	if strings.Contains(r.URL.Path, "supportedTimeZones") {
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []map[string]string{{"alias": "UTC"}}})
		return
	}
	if strings.Contains(r.URL.Path, "/calendars/team/events") {
		s.mu.Lock()
		defer s.mu.Unlock()
		var body map[string]any
		if r.Method != "GET" && json.NewDecoder(r.Body).Decode(&body) != nil {
			s.t.Error("invalid event body")
			w.WriteHeader(400)
			return
		}
		versionKey := "etag"
		if s.vendor == "microsoft" {
			versionKey = "@odata.etag"
		}
		switch r.Method {
		case "POST":
			s.creates.Add(1)
			s.event = body
			if s.vendor == "google" {
				s.event["kind"], s.event["status"], s.event["eventType"] = "calendar#event", "confirmed", "default"
			} else {
				s.event["id"], s.event["type"], s.event["changeKey"] = "created-event", "singleInstance", "change-1"
				s.event["isCancelled"], s.event["isOrganizer"], s.event["hideAttendees"], s.event["isOnlineMeeting"], s.event["showAs"] = false, true, false, false, "busy"
			}
			s.event[versionKey] = `"created"`
			if s.vendor == "microsoft" {
				w.WriteHeader(201)
			}
		case "PATCH":
			s.patches.Add(1)
			if s.event == nil || r.Header.Get("If-Match") != s.event[versionKey] || s.conflict.Load() {
				w.WriteHeader(412)
				return
			}
			if len(body) != 1 || body["location"] == nil {
				s.t.Error("PATCH changed undeclared fields")
				w.WriteHeader(400)
				return
			}
			s.event["location"], s.event[versionKey] = body["location"], `"updated"`
		case "GET":
			if s.event == nil || !strings.HasSuffix(r.URL.Path, "/"+s.event["id"].(string)) {
				w.WriteHeader(404)
				return
			}
		default:
			w.WriteHeader(405)
			return
		}
		_ = json.NewEncoder(w).Encode(s.event)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/profile") {
		_ = json.NewEncoder(w).Encode(map[string]string{"emailAddress": "account@example.test"})
		return
	}
	if strings.HasSuffix(r.URL.Path, "/messages/original") {
		if s.vendor == "google" {
			headers := []map[string]string{{"name": "From", "value": "sender@example.test"}, {"name": "To", "value": "account@example.test"}, {"name": "Subject", "value": "Owner test"}, {"name": "Reply-To", "value": "reply@example.test"}, {"name": "Message-ID", "value": "<original@example.test>"}, {"name": "Date", "value": "Sat, 12 Sep 2026 09:00:00 +0000"}}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "original", "threadId": "thread", "labelIds": []string{"INBOX"}, "internalDate": "1789189200000", "payload": map[string]any{"headers": headers}})
		} else {
			address := func(value string) map[string]any {
				return map[string]any{"emailAddress": map[string]string{"address": value}}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "original", "conversationId": "thread", "internetMessageId": "<original@example.test>", "subject": "Owner test", "from": address("sender@example.test"), "replyTo": []any{address("reply@example.test")}, "toRecipients": []any{address("account@example.test")}, "ccRecipients": []any{}, "receivedDateTime": "2026-09-12T09:00:00Z", "sentDateTime": "2026-09-12T09:00:00Z", "isRead": true, "isDraft": false})
		}
		return
	}
	if r.Method == "POST" && (strings.HasSuffix(r.URL.Path, "/messages/send") || strings.HasSuffix(r.URL.Path, "/sendMail") || strings.HasSuffix(r.URL.Path, "/original/reply")) {
		s.sends.Add(1)
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			s.t.Error("invalid outgoing body")
			w.WriteHeader(400)
			return
		}
		if s.loseSend.Load() {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				s.t.Error(err)
			} else {
				_ = conn.Close()
			}
			return
		}
		if s.vendor == "microsoft" {
			w.WriteHeader(202)
		} else {
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "sent-id", "threadId": "thread"})
		}
		return
	}
	s.t.Error("unexpected provider endpoint", r.Method, r.URL.Path)
	w.WriteHeader(404)
}
