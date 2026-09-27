package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

type routedWebhookTestProvider struct{}

type timedGooglePushTestProvider struct{ routedWebhookTestProvider }

func (*timedGooglePushTestProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "google_workspace", ProviderKey: "google", ProviderRevision: "1.0.0"}
}

func (p *timedGooglePushTestProvider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	verified, err := p.routedWebhookTestProvider.VerifyWebhook(ctx, request)
	if err != nil {
		return verified, err
	}
	verified.Security.EventTime, _ = time.Parse(time.RFC3339Nano, firstRoutedWebhookHeader(request.Headers, "X-Publish-Time"))
	return verified, nil
}

func (*routedWebhookTestProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "email", ProviderKey: "google_workspace", ProviderRevision: "1.0.0"}
}

func (*routedWebhookTestProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, nil
}

func (*routedWebhookTestProvider) BackgroundTasks(connector.Connection) []connector.BackgroundTaskDescriptor {
	return []connector.BackgroundTaskDescriptor{{Key: "gmail_sync", StateVersion: 1}}
}

func (*routedWebhookTestProvider) ProcessBackground(context.Context, connector.BackgroundRequest) (connector.BackgroundResult, error) {
	return connector.BackgroundResult{}, nil
}

func (*routedWebhookTestProvider) VerifyWebhook(_ context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	externalID := firstRoutedWebhookHeader(request.Headers, "X-Message-ID")
	return connector.VerifiedWebhook{
		EventType:  "gmail.mailbox.changed",
		ExternalID: externalID,
		Payload: json.RawMessage(`{
			"routes":[{"kind":"email","value":" ADA@Example.com "},{"kind":"open_id","value":"ou_42"}],
			"wake_tasks":["gmail_sync"],
			"events":[{"external_id":"provider-change-1","event_type":"calendar.event.changed","payload":{"event_id":"event-42"}}],
			"data":{"history_id":"842"}
		}`),
		Security: &connector.WebhookSecurityEvidence{SignatureVerified: firstRoutedWebhookHeader(request.Headers, "Authorization") == "Bearer valid"},
	}, nil
}

func firstRoutedWebhookHeader(headers map[string][]string, name string) string {
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func TestRoutedWebhookDeduplicatesRoutesAndCoalescesProviderTaskWake(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "routed-webhook")
	provider := &routedWebhookTestProvider{}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)

	ingress := upsertRoutedWebhookConnection(t, management, "platform", "gmail-push")
	targetA := upsertRoutedWebhookConnection(t, management, "workspace-a", "gmail-a")
	targetB := upsertRoutedWebhookConnection(t, management, "workspace-b", "gmail-b")
	if err := management.replaceProviderAccountRoutes(t.Context(), targetA, json.RawMessage(`{
		"provider_account":{"subject":"google-subject-a","routes":[{"kind":"email","value":"ada@example.com"},{"kind":"open_id","value":"ou_42"}]}
	}`)); err != nil {
		t.Fatal(err)
	}
	if err := management.replaceProviderAccountRoutes(t.Context(), targetB, json.RawMessage(`{
		"provider_account":{"subject":"google-subject-b","routes":[{"kind":"email","value":"ada@example.com"}]}
	}`)); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC)
	future := now.Add(time.Hour).UnixMilli()
	if _, err := database.ExecContext(t.Context(), `UPDATE _integration_provider_runs SET status='processing', lease_owner='worker-a', lease_expires_at=?, due_at=?, wake_sequence=5 WHERE workspace_id=? AND connection_key=? AND run_kind=? AND run_key=?`, future, future, targetA.WorkspaceID, targetA.Key, providerRunKindState, "gmail_sync"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE _integration_provider_runs SET status='failed', due_at=?, wake_sequence=7, attempt_count=4, last_error_code='temporary' WHERE workspace_id=? AND connection_key=? AND run_kind=? AND run_key=?`, future, targetB.WorkspaceID, targetB.Key, providerRunKindState, "gmail_sync"); err != nil {
		t.Fatal(err)
	}

	store := NewRoutedWebhookStore(database, dialect, delivery)
	request := integrationsdk.WebhookRequest{
		WorkspaceID: ingress.WorkspaceID, ConnectorKey: ingress.ConnectorKey, ConnectionKey: ingress.Key,
		Headers: map[string][]string{"Authorization": {"Bearer valid"}, "X-Message-ID": {"message-1"}},
		Body:    []byte(`{"message":"opaque"}`), ReceivedAt: now,
	}
	receipt, err := store.AcceptRoutedWebhook(t.Context(), request)
	if err != nil || receipt.ExternalID != "message-1" || receipt.Routed != 2 || receipt.Duplicate {
		t.Fatalf("receipt=%#v err=%v", receipt, err)
	}
	assertRoutedWebhookTask(t, database, targetA.WorkspaceID, targetA.Key, "processing", "worker-a", 6, now.UnixMilli(), 0, "")
	assertRoutedWebhookTask(t, database, targetB.WorkspaceID, targetB.Key, "ready", "", 8, now.UnixMilli(), 0, "temporary")

	receipt, err = store.AcceptRoutedWebhook(t.Context(), request)
	if err != nil || !receipt.Duplicate || receipt.Routed != 0 {
		t.Fatalf("duplicate receipt=%#v err=%v", receipt, err)
	}
	assertRoutedWebhookTask(t, database, targetA.WorkspaceID, targetA.Key, "processing", "worker-a", 6, now.UnixMilli(), 0, "")
	assertRoutedWebhookTask(t, database, targetB.WorkspaceID, targetB.Key, "ready", "", 8, now.UnixMilli(), 0, "temporary")

	var routeKind, routeHash, payload string
	if err = database.QueryRowContext(t.Context(), `SELECT route_kind,route_hash,payload_json FROM _integration_provider_inbox WHERE workspace_id=? AND external_id=?`, ingress.WorkspaceID, "message-1").Scan(&routeKind, &routeHash, &payload); err != nil {
		t.Fatal(err)
	}
	if routeKind != "multi" || len(routeHash) != 64 || strings.Contains(routeHash, "ada") || payload != `{"history_id":"842"}` {
		t.Fatalf("persisted inbox route_kind=%q route_hash=%q payload=%q", routeKind, routeHash, payload)
	}
	var eventCount int
	if err = database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _integration_events WHERE provider='google_workspace' AND event_type='calendar.event.changed'`).Scan(&eventCount); err != nil || eventCount != 2 {
		t.Fatalf("routed event count=%d err=%v", eventCount, err)
	}
	var eventPayload string
	if err = database.QueryRowContext(t.Context(), `SELECT payload_json FROM _integration_events WHERE workspace_id=? AND provider='google_workspace'`, targetA.WorkspaceID).Scan(&eventPayload); err != nil || !strings.Contains(eventPayload, `"connection_key":"gmail-a"`) || !strings.Contains(eventPayload, `"owner_user_id":"admin"`) {
		t.Fatalf("routed event payload=%q err=%v", eventPayload, err)
	}

	request.Headers = map[string][]string{"Authorization": {"Bearer invalid"}, "X-Message-ID": {"message-2"}}
	if _, err = store.AcceptRoutedWebhook(t.Context(), request); err == nil || !strings.Contains(err.Error(), "signature is not verified") {
		t.Fatalf("invalid signature err=%v", err)
	}
	var inboxCount int
	if err = database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _integration_provider_inbox`).Scan(&inboxCount); err != nil || inboxCount != 1 {
		t.Fatalf("inbox count=%d err=%v", inboxCount, err)
	}
}

func TestRoutedGooglePushStoresVerifiedPublishTimeOnce(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "timed-google-push")
	provider := &timedGooglePushTestProvider{}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)
	ingress, err := management.UpsertConnection(t.Context(), "platform", "google-push", "admin", integrationsdk.ConnectionInput{
		ConnectorKey: "google_workspace", ProviderKey: "google", Name: "Google Push", Status: "active",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 27, 12, 30, 0, 0, time.UTC)
	request := integrationsdk.WebhookRequest{
		WorkspaceID: ingress.WorkspaceID, ConnectorKey: ingress.ConnectorKey, ConnectionKey: ingress.Key,
		Headers: map[string][]string{
			"Authorization": {"Bearer valid"}, "X-Message-ID": {"google-message-1"},
			"X-Publish-Time": {now.Add(-6 * time.Minute).Format(time.RFC3339Nano)},
		},
		Body: []byte(`{"message":"opaque"}`), ReceivedAt: now,
	}
	store := NewRoutedWebhookStore(database, dialect, delivery)
	receipt, err := store.AcceptRoutedWebhook(t.Context(), request)
	if err != nil || receipt.Duplicate || receipt.Routed != 0 {
		t.Fatalf("Google Push receipt=%+v err=%v", receipt, err)
	}
	var published int64
	var status string
	if err := database.QueryRowContext(t.Context(), `SELECT published_at,status FROM _integration_provider_inbox WHERE external_id=?`, "google-message-1").Scan(&published, &status); err != nil {
		t.Fatal(err)
	}
	if published != now.Add(-6*time.Minute).UnixMilli() || status != "unmatched" {
		t.Fatalf("Google Push inbox published_at=%d status=%q", published, status)
	}
	request.Headers["X-Publish-Time"] = []string{now.Add(-time.Minute).Format(time.RFC3339Nano)}
	receipt, err = store.AcceptRoutedWebhook(t.Context(), request)
	if err != nil || !receipt.Duplicate {
		t.Fatalf("duplicate Google Push receipt=%+v err=%v", receipt, err)
	}
	if err := database.QueryRowContext(t.Context(), `SELECT published_at FROM _integration_provider_inbox WHERE external_id=?`, "google-message-1").Scan(&published); err != nil || published != now.Add(-6*time.Minute).UnixMilli() {
		t.Fatalf("deduplicated Google Push published_at=%d err=%v", published, err)
	}
	request.Headers["Authorization"] = []string{"Bearer invalid"}
	request.Headers["X-Message-ID"] = []string{"google-message-2"}
	if _, err := store.AcceptRoutedWebhook(t.Context(), request); err == nil {
		t.Fatal("unverified Google Push was accepted")
	}
	var count int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _integration_provider_inbox`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("Google Push inbox count=%d err=%v", count, err)
	}
}

func TestVerifiedGooglePushPublishedAtRejectsMissingOrInvalidEvidence(t *testing.T) {
	received := time.Date(2026, 9, 27, 12, 30, 0, 0, time.UTC)
	for _, test := range []struct {
		name, connector, provider string
		eventTime                 time.Time
		want                      int64
	}{
		{"valid", "google_workspace", "google", received.Add(-time.Minute), received.Add(-time.Minute).UnixMilli()},
		{"missing", "google_workspace", "google", time.Time{}, 0},
		{"future", "google_workspace", "google", received.Add(time.Second), 0},
		{"older than seven days", "google_workspace", "google", received.Add(-8 * 24 * time.Hour), 0},
		{"another provider", "email", "google_workspace", received.Add(-time.Minute), 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := verifiedGooglePushPublishedAt(test.connector, test.provider, &connector.WebhookSecurityEvidence{EventTime: test.eventTime}, received)
			if got != test.want {
				t.Fatalf("published_at=%d, want %d", got, test.want)
			}
		})
	}
	if got := verifiedGooglePushPublishedAt("google_workspace", "google", nil, received); got != 0 {
		t.Fatalf("nil evidence published_at=%d", got)
	}
}

func upsertRoutedWebhookConnection(t *testing.T, management *ManagementStore, workspaceID, key string) integrationsdk.Connection {
	t.Helper()
	connection, err := management.UpsertConnection(t.Context(), workspaceID, key, "admin", integrationsdk.ConnectionInput{
		ConnectorKey: "email", ProviderKey: "google_workspace", Name: key, Status: "active",
	})
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func assertRoutedWebhookTask(t *testing.T, database *sql.DB, workspaceID, connectionKey, wantStatus, wantLease string, wantWake, wantDue int64, wantAttempts int, wantError string) {
	t.Helper()
	var status, lease, lastError string
	var wake, due int64
	var attempts int
	if err := database.QueryRowContext(t.Context(), `SELECT status,lease_owner,wake_sequence,due_at,attempt_count,last_error_code FROM _integration_provider_runs WHERE workspace_id=? AND connection_key=? AND run_kind=? AND run_key=?`, workspaceID, connectionKey, providerRunKindState, "gmail_sync").Scan(&status, &lease, &wake, &due, &attempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || lease != wantLease || wake != wantWake || due != wantDue || attempts != wantAttempts || lastError != wantError {
		t.Fatalf("task %s/%s status=%q lease=%q wake=%d due=%d attempts=%d error=%q", workspaceID, connectionKey, status, lease, wake, due, attempts, lastError)
	}
}
