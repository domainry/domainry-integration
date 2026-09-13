package module_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/domainry/domainry-foundation/requestcontext"
	sdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/remote"
	saas "github.com/domainry/domainry-integration/internal/assembly/saas"
	"github.com/domainry/domainry-integration/module"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	"github.com/domainry/domainry-orm/query"
)

// Real owner migrations, encrypted materials and public local/remote ports.
// The external CRM protocol fixture performs no actual vendor I/O.
func TestSubjectErasureOwnerRollbackRetryIsolationAndFencing(t *testing.T) {
	for _, mode := range []string{"module", "saas"} {
		t.Run(mode, func(t *testing.T) {
			db, err := sql.Open("sqlite", t.TempDir()+"/owner.db")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			d, _ := ormdialect.New(ormdialect.SQLite)
			var cipherKey [32]byte
			if _, err = rand.Read(cipherKey[:]); err != nil {
				t.Fatal(err)
			}
			host := &rotationHost{db: db, dialect: d.WithSchema(""), provider: &rotationProvider{}, key: cipherKey}
			var binding sdk.Binding
			if mode == "module" {
				binding, err = module.NewFactory().OpenModule(t.Context(), sdk.ApplicationRef{RuntimeID: "erasure-runtime"}, host)
			} else {
				owner, e := saas.Open(t.Context(), sdk.ApplicationRef{RuntimeID: "owner"}, host, "owner-service-token")
				if e != nil {
					t.Fatal(e)
				}
				defer owner.Close(context.Background())
				server := httptest.NewServer(owner.Handler)
				defer server.Close()
				summary, e := owner.Binding.CapabilitySummary(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				binding, err = saas.NewFactory(remote.NewFactory(remote.Options{BaseURL: server.URL, Token: "owner-service-token", HTTPClient: server.Client(), CapabilityContractSHA256: summary.Identity.ContractSHA256})).OpenSaaS(t.Context(), sdk.ApplicationRef{RuntimeID: "runtime"}, nil)
				// Privileged routes must reject anonymous callers even with a body.
				res, e := server.Client().Post(server.URL+"/integration/v1/subjects/erase", "application/json", strings.NewReader(`{}`))
				if e != nil {
					t.Fatal(e)
				}
				res.Body.Close()
				if res.StatusCode != http.StatusUnauthorized {
					t.Fatal("anonymous erasure route", res.StatusCode)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			defer binding.Close(context.Background())
			management := binding.(sdk.ManagementBinding).Management()
			admin := binding.(sdk.ConnectionAccountAdministrationBinding).ConnectionAccountAdministration()
			for _, item := range []struct {
				ws, key, user string
				scope         sdk.ConnectionAccountScope
			}{{"ws", "alice", "alice", sdk.ConnectionAccountScopePersonal}, {"ws", "bob", "bob", sdk.ConnectionAccountScopePersonal}, {"ws", "shared", "", sdk.ConnectionAccountScopeWorkspace}, {"other", "alice", "alice", sdk.ConnectionAccountScopePersonal}} {
				if _, err = management.UpsertSecret(t.Context(), item.ws, item.key+"-token", "operator", sdk.SecretInput{Kind: "token", Value: "rotated-credential"}); err != nil {
					t.Fatal(err)
				}
				if _, err = management.UpsertConnection(t.Context(), item.ws, item.key, "operator", sdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "rotation_probe", Name: item.key + "@private.example", Status: "active", SecretRefs: map[string]string{"token": "secret:" + item.key + "-token"}}); err != nil {
					t.Fatal(err)
				}
				if _, err = admin.RegisterConnectionAccount(t.Context(), item.ws, item.key, "operator", sdk.ConnectionAccountRegistration{Scope: item.scope, OwnerUserID: item.user}); err != nil {
					t.Fatal(err)
				}
			}
			messageIDs := []string{"message-alice", "message-bob"}
			for i, id := range messageIDs {
				payload, _ := json.Marshal(map[string]string{"destination": "email", "body": []string{"alice@private.example", "bob@private.example"}[i], "_runtime_attachment_base64": "cHJpdmF0ZSBjc3Y="})
				if _, err = binding.Delivery().Accept(t.Context(), sdk.DeliveryRequest{MessageID: id, DeduplicationKey: id, WorkspaceID: "ws", ConnectorKey: "crm", ConnectionKey: "shared", Operation: "lookup", Payload: payload}); err != nil {
					t.Fatal(err)
				}
			}
			// Seed actual source-owned tables using their migrated column shapes.
			tables := []string{"_integration_oauth_sessions", "_integration_web_push_subscriptions", "_integration_api_keys", "_integration_external_identities", "_integration_connector_provider_states", "_integration_connector_provider_commits", "_integration_credential_refresh_leases", "_integration_webhook_subscriptions", "_integration_events", "_integration_event_mapping_intents"}
			for _, table := range tables {
				for _, subject := range []string{"alice", "bob"} {
					for _, workspace := range []string{"ws", "other"} {
						id := table + "-" + workspace + "-" + subject
						values := map[string]any{"workspace_id": workspace, "user_id": subject, "actor_id": subject, "connection_key": subject, "status": "failed", "secret_key": subject + "-token", "event_id": "event-" + subject, "lease_expires_at": "", "exchange_deadline": ""}
						values["provider"] = "rotation_probe"
						values["external_subject"] = "external-" + subject
						for _, field := range []string{"session_json", "payload_json", "endpoint", "p256dh", "auth_secret", "verifier_ciphertext", "external_name", "name", "description"} {
							values[field] = "alice@private.example"
							if subject == "bob" {
								values[field] = "bob@private.example"
							}
						}
						seedIntegrationSubjectRow(t, host, table, id, values)
					}
				}
			}
			webhookIDs := map[string]string{}
			managerInvocationID := ""
			for _, subject := range []string{"alice", "bob"} {
				payload, _ := json.Marshal(map[string]string{"email": subject + "@private.example"})
				result, e := binding.(sdk.OperationsBinding).Operations().Call(t.Context(), sdk.ProviderCallRequest{RequestID: "manager-" + subject, WorkspaceID: "ws", ActorID: "operator", ConnectorKey: "crm", ConnectionKey: "shared", Operation: "lookup", Payload: payload, Source: sdk.InvocationSource{ExecutionID: "root-" + subject, ObjectKey: "member_profile", RecordID: "profile-" + subject}})
				if e != nil {
					t.Fatal(e)
				}
				if subject == "alice" {
					managerInvocationID = result.Invocation.ID
				}
			}
			for _, subject := range []string{"alice", "bob"} {
				raw, _ := json.Marshal(map[string]any{"id": "webhook-" + subject, "sender_id": "external-" + subject, "email": subject + "@private.example", "_integration_context": map[string]any{"connection_key": "bob", "verified_external_subject": "external-bob"}})
				receipt, e := binding.(sdk.OperationsBinding).Operations().AcceptWebhook(t.Context(), sdk.WebhookRequest{WorkspaceID: "ws", ConnectorKey: "crm", ConnectionKey: "shared", Body: raw, ReceivedAt: time.Now().UTC()})
				if e != nil {
					t.Fatal(e)
				}
				webhookIDs[subject] = receipt.Event.ID
			}
			// The sourced event ID is a real Integration ID, not a payload search.
			eventID := "_integration_events-ws-alice"
			execIntegration(t, db, "UPDATE _integration_event_mapping_intents SET event_id=? WHERE id=?", eventID, "_integration_event_mapping_intents-ws-alice")
			request := sdk.SubjectErasureRequest{WorkspaceID: "ws", SubjectID: "alice", RequestID: "erase-alice", PublicationMessageIDs: []string{"message-alice"}, EventIDs: []string{eventID}, Resources: []sdk.SubjectRecordReference{{ObjectKey: "member_profile", RecordID: "profile-alice"}}}
			subjects := binding.(sdk.SubjectLifecycleBinding).SubjectLifecycle()
			trusted := requestcontext.WithWorkspaceID(t.Context(), "ws")
			if _, err = subjects.PrepareSubjectErasure(t.Context(), request); err == nil {
				t.Fatal("untrusted workspace accepted")
			}
			held := request
			held.LegalHolds = json.RawMessage(`[{"id":"hold"}]`)
			if _, err = subjects.PrepareSubjectErasure(trusted, held); err == nil {
				t.Fatal("legal hold accepted")
			}
			// In-flight synchronous delivery blocks cleanup before any owner fence.
			execIntegration(t, db, "UPDATE _integration_invocations SET status='running' WHERE request_ref='message-alice'")
			if _, err = subjects.PrepareSubjectErasure(trusted, request); err == nil {
				t.Fatal("in-flight delivery erased")
			}
			execIntegration(t, db, "UPDATE _integration_invocations SET status='succeeded' WHERE request_ref='message-alice'")
			beforePeer := integrationPeerSnapshot(t, host)
			plan, err := subjects.PrepareSubjectErasure(trusted, request)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(plan), "@private.example") || strings.Contains(string(plan), "rotated-credential") {
				t.Fatal("private payload copied into plan", string(plan))
			}
			if !strings.Contains(string(plan), webhookIDs["alice"]) || strings.Contains(string(plan), webhookIDs["bob"]) {
				t.Fatal("verified external actor event provenance not scoped")
			}
			if !strings.Contains(string(plan), managerInvocationID) {
				t.Fatal("manager/shared connection invocation missed source member")
			}
			changed := request
			changed.PublicationMessageIDs = []string{"message-bob"}
			if _, err = subjects.PrepareSubjectErasure(trusted, changed); err == nil {
				t.Fatal("same request changed ownership provenance")
			}
			for _, mutation := range []string{"new-secret", "new-connection"} {
				if mutation == "new-secret" {
					_, err = management.UpsertSecret(trusted, "ws", "alice-token", "operator", sdk.SecretInput{Kind: "token", Value: "restore-private"})
				} else {
					_, err = management.UpsertConnection(trusted, "ws", "alice", "operator", sdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "rotation_probe", Status: "active"})
				}
				if err == nil {
					t.Fatal("erased ownership recreated", mutation)
				}
			}
			if _, err = binding.Delivery().Accept(trusted, sdk.DeliveryRequest{MessageID: "message-alice", DeduplicationKey: "message-alice", WorkspaceID: "ws", ConnectorKey: "crm", ConnectionKey: "shared", Operation: "lookup", Payload: json.RawMessage(`{"body":"restore-private"}`)}); err == nil {
				t.Fatal("fenced message was sent again")
			}
			var modified map[string]any
			if err = json.Unmarshal(plan, &modified); err != nil {
				t.Fatal(err)
			}
			modified["rows"] = []any{}
			tampered, _ := json.Marshal(modified)
			if _, err = subjects.ErasePreparedSubject(trusted, request, tampered); err == nil {
				t.Fatal("forged plan accepted")
			}
			execIntegration(t, db, "CREATE TRIGGER reject_owned_material_delete BEFORE DELETE ON _integration_secret_materials WHEN OLD.workspace_id='ws' AND OLD.secret_key='alice-token' BEGIN SELECT RAISE(ABORT,'injected cleanup failure'); END")
			if _, err = subjects.ErasePreparedSubject(trusted, request, plan); err == nil {
				t.Fatal("injected transactional failure lost")
			}
			var connectionJSON, name string
			if err = db.QueryRow("SELECT name,config_json FROM _integration_connections WHERE workspace_id='ws' AND connection_key='alice'").Scan(&name, &connectionJSON); err != nil || name != "alice@private.example" {
				t.Fatal("earlier redaction did not roll back", name, err)
			}
			execIntegration(t, db, "DROP TRIGGER reject_owned_material_delete")
			result, err := subjects.ErasePreparedSubject(trusted, request, plan)
			if err != nil {
				t.Fatal(err)
			}
			again, err := subjects.ErasePreparedSubject(trusted, request, plan)
			if err != nil || string(again) != string(result) {
				t.Fatal("durable erasure result retry changed", err)
			}
			if after := integrationPeerSnapshot(t, host); after != beforePeer {
				t.Fatal("peer, shared connection, or other workspace changed")
			}
			incoming, _ := json.Marshal(map[string]string{"id": "after-erasure", "sender_id": "external-alice", "email": "alice@private.example"})
			if _, err = binding.(sdk.OperationsBinding).Operations().AcceptWebhook(trusted, sdk.WebhookRequest{WorkspaceID: "ws", ConnectorKey: "crm", ConnectionKey: "shared", Body: incoming, ReceivedAt: time.Now().UTC()}); err == nil {
				t.Fatal("erased external sender recreated private event")
			}
			if _, err = management.UpsertExternalIdentity(trusted, "ws", "rebound", "operator", sdk.ExternalIdentityInput{Provider: "rotation_probe", ExternalSubject: "external-alice", ExternalSubjectType: "user", ActorID: "bob", RoleKey: "member", Status: "active"}); err == nil {
				t.Fatal("erased external credential rebound")
			}
			if _, err = binding.(sdk.OperationsBinding).Operations().Call(trusted, sdk.ProviderCallRequest{RequestID: "manager-after", WorkspaceID: "ws", ActorID: "operator", ConnectorKey: "crm", ConnectionKey: "shared", Operation: "lookup", Payload: json.RawMessage(`{"email":"alice@private.example"}`), Source: sdk.InvocationSource{ExecutionID: "later", ObjectKey: "member_profile", RecordID: "profile-alice"}}); err == nil {
				t.Fatal("manager recreated erased member invocation")
			}
			var decoded struct{ Rows []struct{ Table, ID string } }
			if err = json.Unmarshal(plan, &decoded); err != nil {
				t.Fatal(err)
			}
			for _, row := range decoded.Rows {
				raw := integrationSubjectRowJSON(t, host, row.Table, row.ID)
				for _, private := range []string{"alice@private.example", "cHJpdmF0ZSBjc3Y=", "rotated-credential"} {
					if strings.Contains(raw, private) {
						t.Fatal("source private copy remains", row.Table, private)
					}
				}
			}
			if _, err = admin.RegisterConnectionAccount(trusted, "ws", "shared", "operator", sdk.ConnectionAccountRegistration{Scope: sdk.ConnectionAccountScopePersonal, OwnerUserID: "alice"}); err == nil {
				t.Fatal("erased user gained another account")
			}
		})
	}
}

func (*rotationProvider) VerifyWebhook(_ context.Context, r connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	var value struct {
		ID     string `json:"id"`
		Sender string `json:"sender_id"`
	}
	if err := json.Unmarshal(r.Body, &value); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	return connector.VerifiedWebhook{EventType: "private-message", ExternalID: value.ID, Payload: json.RawMessage(r.Body), ExternalIdentity: &connector.WebhookExternalIdentity{Subject: value.Sender}}, nil
}
func execIntegration(t *testing.T, db *sql.DB, stmt string, args ...any) {
	t.Helper()
	if _, err := db.Exec(stmt, args...); err != nil {
		t.Fatal(err)
	}
}
func seedIntegrationSubjectRow(t *testing.T, host *rotationHost, table, id string, overrides map[string]any) {
	t.Helper()
	rows, err := host.db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		t.Fatal(err)
	}
	columns := []string{}
	values := []any{}
	for rows.Next() {
		var ordinal, required, primary int
		var name, kind string
		var def sql.NullString
		if err = rows.Scan(&ordinal, &name, &kind, &required, &def, &primary); err != nil {
			t.Fatal(err)
		}
		var value any = id + "-" + name
		if strings.Contains(strings.ToUpper(kind), "INT") {
			value = int64(0)
		}
		if v, ok := overrides[name]; ok {
			value = v
		}
		if name == "id" {
			value = id
		}
		columns = append(columns, name)
		values = append(values, value)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	stmt, args, err := query.NewInsertBuilder(host.dialect, table).Columns(columns...).Values(values...).Build()
	if err != nil {
		t.Fatal(err)
	}
	execIntegration(t, host.db, stmt, args...)
}
func integrationSubjectRowJSON(t *testing.T, host *rotationHost, table, id string) string {
	t.Helper()
	stmt, args, err := query.NewSelectBuilder(host.dialect, table).Projections(query.Project(query.AllColumns())).Where(query.Equal("id", id)).Build()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := host.db.Query(stmt, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return ""
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	values := make([]any, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	if err = rows.Scan(dest...); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(values)
	return string(raw)
}
func integrationPeerSnapshot(t *testing.T, host *rotationHost) string {
	t.Helper()
	tables := []string{"_integration_connections", "_integration_connection_accounts", "_integration_connection_account_secrets", "_integration_connection_grants", "_integration_secrets", "_integration_secret_materials", "_integration_oauth_sessions", "_integration_web_push_subscriptions", "_integration_api_keys", "_integration_external_identities", "_integration_connector_provider_states", "_integration_connector_provider_commits", "_integration_credential_refresh_leases", "_integration_webhook_subscriptions", "_integration_invocations", "_integration_events", "_integration_event_mapping_intents"}
	all := []string{}
	for _, table := range tables {
		stmt, args, err := query.NewSelectBuilder(host.dialect, table).Columns("id").Where(query.Or(query.Equal("workspace_id", "other"), query.Equal("workspace_id", "ws"))).OrderBy(query.Ascending("id")).Build()
		if err != nil {
			t.Fatal(err)
		}
		rows, err := host.db.Query(stmt, args...)
		if err != nil {
			t.Fatal(err)
		}
		ids := []string{}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		for _, id := range ids {
			raw := integrationSubjectRowJSON(t, host, table, id)
			if strings.Contains(id, "bob") || strings.Contains(id, "other") || strings.Contains(raw, "\"other\"") || strings.Contains(raw, "\"bob\"") || strings.Contains(raw, "bob-token") || strings.Contains(raw, "shared-token") || strings.Contains(raw, "shared@private.example") || strings.Contains(raw, "bob@private.example") {
				all = append(all, table+raw)
			}
		}
	}
	raw, _ := json.Marshal(all)
	return string(raw)
}
