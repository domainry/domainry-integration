package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type foreground429Provider struct {
	calls        int
	code         string
	connectorKey string
	providerKey  string
}

func (p *foreground429Provider) Descriptor() connector.ProviderDescriptor {
	connectorKey, providerKey := p.connectorKey, p.providerKey
	if connectorKey == "" {
		connectorKey = "google_workspace"
	}
	if providerKey == "" {
		providerKey = "google"
	}
	return connector.ProviderDescriptor{
		ConnectorKey: connectorKey, ProviderKey: providerKey,
		Operations: []connector.OperationDescriptor{
			{ConnectorKey: connectorKey, ProviderKey: providerKey, Key: "lookup", Mode: connector.ModeCall, ContractSHA256: strings.Repeat("a", 64)},
			{ConnectorKey: connectorKey, ProviderKey: providerKey, Key: "send", Mode: connector.ModeEnqueue, ContractSHA256: strings.Repeat("b", 64)},
		},
	}
}

func (p *foreground429Provider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	p.calls++
	code := p.code
	if code == "" {
		code = "google.http_429"
	}
	return connector.CallResult{}, connector.RetryableError(code, errors.New("private response body"))
}

func TestGoogleHTTP429EventsAggregateAcrossWorkerInstances(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-429-multi-worker")
	now := time.Date(2026, 9, 27, 12, 34, 0, 0, time.UTC)
	for index := 0; index < 2; index++ {
		id := fmt.Sprintf("run-%d", index)
		worker := NewWorkerStore(database, dialect, nil, nil, fmt.Sprintf("worker-%d", index))
		result, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			"workspace-a", providerRunKindState, "poll", "crm", "probe", id, "poll", 1, "", "", `{}`, "processing", "", 0, now.UnixMilli(), worker.workerID, now.Add(time.Minute).UnixMilli(), 5, now.UnixMilli(), now.UnixMilli())
		if err != nil {
			t.Fatal(err)
		}
		runID, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if err := worker.failProviderTask(t.Context(), providerTaskCandidate{id: runID, workspaceID: "workspace-a", fencingToken: 4}, connector.RetryableError("google.http_429", errors.New("private response")), now); err == nil {
			t.Fatal("provider failure was swallowed")
		}
	}
	snapshot, err := NewManagementStore(database, dialect, nil, nil).providerRunSnapshot(t.Context(), now)
	if err != nil || snapshot.GoogleHTTP429Events5m != 2 || snapshot.GoogleHTTP429Failures != 2 {
		t.Fatalf("multi-worker 429 snapshot=%+v err=%v", snapshot, err)
	}
}

func TestGoogleGmailRateLimit403EventsAreNotMislabelledAsHTTP429(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-gmail-rate-limit")
	now := time.Date(2026, 9, 29, 2, 40, 0, 0, time.UTC)
	for index, code := range []string{"google.http_403.rate_limit_exceeded", "google.http_403.user_rate_limit_exceeded"} {
		id := fmt.Sprintf("gmail-run-%d", index)
		worker := NewWorkerStore(database, dialect, nil, nil, fmt.Sprintf("worker-%d", index))
		result, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			"workspace-a", providerRunKindState, "poll", "google_workspace", "google", id, "gmail_sync", 1, "", "", `{}`, "processing", "", 0, now.UnixMilli(), worker.workerID, now.Add(time.Minute).UnixMilli(), 5, now.UnixMilli(), now.UnixMilli())
		if err != nil {
			t.Fatal(err)
		}
		runID, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if err := worker.failProviderTask(t.Context(), providerTaskCandidate{id: runID, workspaceID: "workspace-a", fencingToken: 4}, connector.RetryableError(code, errors.New("private response")), now); err == nil {
			t.Fatal("Gmail rate-limit failure was swallowed")
		}
	}
	snapshot, err := NewManagementStore(database, dialect, nil, nil).providerRunSnapshot(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.GoogleGmailProjectRateLimitFailures != 1 || snapshot.GoogleGmailProjectRateLimitEvents5m != 1 || snapshot.GoogleGmailProjectRateLimitEventsHour != 1 || snapshot.GoogleGmailUserRateLimitFailures != 1 || snapshot.GoogleGmailUserRateLimitEvents5m != 1 || snapshot.GoogleGmailUserRateLimitEventsHour != 1 {
		t.Fatalf("Gmail rate-limit snapshot=%+v", snapshot)
	}
	if snapshot.GoogleHTTP429Failures != 0 || snapshot.GoogleHTTP429Events5m != 0 || snapshot.GoogleHTTP429EventsHour != 0 {
		t.Fatalf("Gmail HTTP 403 rejection was mislabelled as HTTP 429: %+v", snapshot)
	}
}

func TestFeishuRateLimitEventsNormalizeCurrentAndLegacyProviderSignals(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-feishu-rate-limit")
	now := time.Date(2026, 9, 29, 3, 10, 0, 0, time.UTC)
	for index, code := range []string{"feishu.rate_limit_exceeded", "feishu_calendar.rate_limit_exceeded", "feishu_calendar.oauth.rate_limit_exceeded"} {
		id := fmt.Sprintf("feishu-run-%d", index)
		worker := NewWorkerStore(database, dialect, nil, nil, fmt.Sprintf("worker-%d", index))
		result, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			"workspace-a", providerRunKindState, "poll", "appointment_scheduling", "feishu_calendar", id, "calendar_sync", 1, "", "", `{}`, "processing", "", 0, now.UnixMilli(), worker.workerID, now.Add(time.Minute).UnixMilli(), 5, now.UnixMilli(), now.UnixMilli())
		if err != nil {
			t.Fatal(err)
		}
		runID, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if err := worker.failProviderTask(t.Context(), providerTaskCandidate{id: runID, workspaceID: "workspace-a", fencingToken: 4}, connector.RetryableError(code, errors.New("private response")), now); err == nil {
			t.Fatal("Feishu rate-limit failure was swallowed")
		}
	}
	snapshot, err := NewManagementStore(database, dialect, nil, nil).providerRunSnapshot(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.FeishuRateLimitFailures != 3 || snapshot.FeishuRateLimitEvents5m != 3 || snapshot.FeishuRateLimitEventsHour != 3 {
		t.Fatalf("Feishu rate-limit snapshot=%+v", snapshot)
	}
	if snapshot.GoogleHTTP429EventsHour != 0 || snapshot.GoogleGmailProjectRateLimitEventsHour != 0 || snapshot.GoogleGmailUserRateLimitEventsHour != 0 {
		t.Fatalf("Feishu rejection was mixed into Google metrics: %+v", snapshot)
	}
}

func TestGmailHistoryEvidenceRequiresNoCoveringVerifiedPush(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "gmail-history-evidence")
	now := time.Date(2026, 9, 29, 12, 34, 0, 0, time.UTC)
	candidate := providerTaskCandidate{
		id: 1, workspaceID: "workspace-a", connectorKey: "google_workspace", providerKey: "google",
		connectionKey: "gmail-a", taskKey: "gmail_sync", wakeSequence: 1, fencingToken: 4,
	}
	if err := recordProviderHistoryObservations(t.Context(), database, dialect, candidate, []connector.BackgroundObservation{
		{Key: googleGmailHistoryMessagesAddedObservationKey, Value: 3},
		{Key: googleGmailHistoryCursorExpiredObservationKey, Value: 1},
	}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_wake_receipts (workspace_id,inbox_id,connector_key,provider_key,connection_key,task_key,wake_sequence,status,published_at,received_at,completed_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		candidate.workspaceID, int64(1), candidate.connectorKey, candidate.providerKey, candidate.connectionKey, candidate.taskKey, 2, "pending", now.Add(-time.Minute).UnixMilli(), now.UnixMilli(), 0, now.UnixMilli(), now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	candidate.wakeSequence, candidate.fencingToken = 2, 5
	if err := recordProviderHistoryObservations(t.Context(), database, dialect, candidate, []connector.BackgroundObservation{{Key: googleGmailHistoryMessagesAddedObservationKey, Value: 5}}, now); err != nil {
		t.Fatal(err)
	}
	untrusted := candidate
	untrusted.connectorKey, untrusted.providerKey = "custom", "probe"
	if err := recordProviderHistoryObservations(t.Context(), database, dialect, untrusted, []connector.BackgroundObservation{{Key: googleGmailHistoryMessagesAddedObservationKey, Value: 99}}, now); err != nil {
		t.Fatal(err)
	}
	snapshot, err := NewManagementStore(database, dialect, nil, nil).providerRunSnapshot(t.Context(), now)
	if err != nil || snapshot.GoogleGmailHistoryMessagesWithoutPushHour != 3 || snapshot.GoogleGmailHistoryCursorExpiredHour != 1 {
		t.Fatalf("Gmail history evidence snapshot=%+v err=%v", snapshot, err)
	}
}

func TestGoogleHTTP429WindowsCountRepeatedFailuresAndExpire(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-429-windows")
	now := time.Date(2026, 9, 27, 12, 34, 0, 0, time.UTC)
	for _, event := range []struct {
		task string
		at   time.Time
	}{
		{"account-a", now},
		{"account-b", now},
		{"account-a", now}, // Each failed attempt is an event, not a distinct account.
		{"account-c", now.Add(-10 * time.Minute)},
		{"account-d", now.Add(-2 * time.Hour)},
		{"account-expired", now.Add(-8 * 24 * time.Hour)},
	} {
		if err := recordGoogleHTTP429(t.Context(), database, dialect, event.task, event.at); err != nil {
			t.Fatal(err)
		}
	}
	current, hour, err := readGoogleHTTP429Events(t.Context(), database, dialect, now)
	if err != nil || current != 3 || hour != 4 {
		t.Fatalf("429 windows current=%d hour=%d err=%v", current, hour, err)
	}
	worker := NewWorkerStore(database, dialect, nil, nil, "cleanup-worker")
	worker.clock = func() time.Time { return now }
	if _, err := worker.ProcessDueProviderTasks(t.Context(), 25); err != nil {
		t.Fatal(err)
	}
	var expired int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _integration_provider_metric_windows WHERE window_started_at < ?`, now.Add(-providerMetricRetention).UnixMilli()).Scan(&expired); err != nil || expired != 0 {
		t.Fatalf("worker retained expired metric rows=%d err=%v", expired, err)
	}
	if err := cleanupProviderMetricWindows(t.Context(), database, dialect, now.Add(8*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _integration_provider_metric_windows`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expired metric rows=%d err=%v", count, err)
	}
}

func TestGoogleHTTP429ForegroundOperationsAndDeliveriesAreCountedSeparately(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-429-foreground")
	if _, err := database.ExecContext(t.Context(), `INSERT INTO _integration_connections (connection_key,workspace_id,connector_key,provider_key,name,status,config_json,secret_refs_json,created_by,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		"primary", "workspace-a", "google_workspace", "google", "Gmail", "active", `{}`, `{}`, "admin", time.Now().UTC().UnixMilli(), time.Now().UTC().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	provider := &foreground429Provider{}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	operations := NewOperationsStore(database, dialect, delivery, nil, nil)

	if _, err := operations.Call(t.Context(), integrationmodel.ProviderCallRequest{
		RequestID: "foreground-read", WorkspaceID: "workspace-a", ConnectorKey: "google_workspace", ConnectionKey: "primary", Operation: "lookup", Payload: json.RawMessage(`{}`),
	}); err == nil {
		t.Fatal("foreground Provider operation unexpectedly succeeded")
	}
	if _, err := delivery.Accept(t.Context(), integrationmodel.DeliveryRequest{
		MessageID: "foreground-send", DeduplicationKey: "foreground-send", WorkspaceID: "workspace-a", ConnectorKey: "google_workspace", ConnectionKey: "primary", Operation: "send", Payload: json.RawMessage(`{}`),
	}); err == nil {
		t.Fatal("foreground Provider delivery unexpectedly succeeded")
	}

	snapshot, err := NewManagementStore(database, dialect, nil, nil).providerRunSnapshot(t.Context(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.GoogleHTTP429ForegroundEventsHour != 2 || snapshot.GoogleHTTP429EventsHour != 0 || snapshot.GoogleHTTP429Failures != 0 {
		t.Fatalf("foreground/background Google 429 snapshot=%+v", snapshot)
	}
	if provider.calls != 2 {
		t.Fatalf("Provider calls=%d, want 2", provider.calls)
	}
}

func TestGoogleGmailRateLimitForegroundOperationsAndDeliveriesAreCountedSeparately(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-gmail-rate-limit-foreground")
	if _, err := database.ExecContext(t.Context(), `INSERT INTO _integration_connections (connection_key,workspace_id,connector_key,provider_key,name,status,config_json,secret_refs_json,created_by,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		"primary", "workspace-a", "google_workspace", "google", "Gmail", "active", `{}`, `{}`, "admin", time.Now().UTC().UnixMilli(), time.Now().UTC().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	provider := &foreground429Provider{code: "google.http_403.user_rate_limit_exceeded"}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	operations := NewOperationsStore(database, dialect, delivery, nil, nil)

	if _, err := operations.Call(t.Context(), integrationmodel.ProviderCallRequest{
		RequestID: "foreground-read", WorkspaceID: "workspace-a", ConnectorKey: "google_workspace", ConnectionKey: "primary", Operation: "lookup", Payload: json.RawMessage(`{}`),
	}); err == nil {
		t.Fatal("foreground Provider operation unexpectedly succeeded")
	}
	if _, err := delivery.Accept(t.Context(), integrationmodel.DeliveryRequest{
		MessageID: "foreground-send", DeduplicationKey: "foreground-send", WorkspaceID: "workspace-a", ConnectorKey: "google_workspace", ConnectionKey: "primary", Operation: "send", Payload: json.RawMessage(`{}`),
	}); err == nil {
		t.Fatal("foreground Provider delivery unexpectedly succeeded")
	}

	snapshot, err := NewManagementStore(database, dialect, nil, nil).providerRunSnapshot(t.Context(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.GoogleGmailUserRateLimitForegroundEventsHour != 2 || snapshot.GoogleGmailUserRateLimitEventsHour != 0 || snapshot.GoogleGmailUserRateLimitFailures != 0 || snapshot.GoogleGmailProjectRateLimitForegroundEventsHour != 0 {
		t.Fatalf("foreground/background Gmail rate-limit snapshot=%+v", snapshot)
	}
	if snapshot.GoogleHTTP429ForegroundEventsHour != 0 {
		t.Fatalf("Gmail HTTP 403 rejection was mislabelled as foreground HTTP 429: %+v", snapshot)
	}
}

func TestFeishuRateLimitForegroundOperationsAndDeliveriesAreCountedSeparately(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-feishu-rate-limit-foreground")
	if _, err := database.ExecContext(t.Context(), `INSERT INTO _integration_connections (connection_key,workspace_id,connector_key,provider_key,name,status,config_json,secret_refs_json,created_by,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		"primary", "workspace-a", "feishu_collaboration", "feishu", "Feishu", "active", `{}`, `{}`, "admin", time.Now().UTC().UnixMilli(), time.Now().UTC().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	provider := &foreground429Provider{code: "feishu.rate_limit_exceeded", connectorKey: "feishu_collaboration", providerKey: "feishu"}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	operations := NewOperationsStore(database, dialect, delivery, nil, nil)

	if _, err := operations.Call(t.Context(), integrationmodel.ProviderCallRequest{
		RequestID: "foreground-read", WorkspaceID: "workspace-a", ConnectorKey: "feishu_collaboration", ConnectionKey: "primary", Operation: "lookup", Payload: json.RawMessage(`{}`),
	}); err == nil {
		t.Fatal("foreground Provider operation unexpectedly succeeded")
	}
	if _, err := delivery.Accept(t.Context(), integrationmodel.DeliveryRequest{
		MessageID: "foreground-send", DeduplicationKey: "foreground-send", WorkspaceID: "workspace-a", ConnectorKey: "feishu_collaboration", ConnectionKey: "primary", Operation: "send", Payload: json.RawMessage(`{}`),
	}); err == nil {
		t.Fatal("foreground Provider delivery unexpectedly succeeded")
	}

	snapshot, err := NewManagementStore(database, dialect, nil, nil).providerRunSnapshot(t.Context(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.FeishuRateLimitForegroundEventsHour != 2 || snapshot.FeishuRateLimitEventsHour != 0 || snapshot.FeishuRateLimitFailures != 0 {
		t.Fatalf("foreground/background Feishu rate-limit snapshot=%+v", snapshot)
	}
	if provider.calls != 2 {
		t.Fatalf("Provider calls=%d, want 2", provider.calls)
	}
}

func TestGoogleHTTP429MetricWriteFailureRollsBackProviderTaskFailure(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-429-atomic")
	worker := NewWorkerStore(database, dialect, nil, nil, "worker")
	now := time.Date(2026, 9, 27, 12, 34, 0, 0, time.UTC)
	result, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"workspace-a", providerRunKindState, "poll", "crm", "probe", "primary", "poll", 1, "", "", `{}`, "processing", "", 0, now.UnixMilli(), worker.workerID, now.Add(time.Minute).UnixMilli(), 5, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	runID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `CREATE TRIGGER reject_provider_metric BEFORE INSERT ON _integration_provider_metric_windows BEGIN SELECT RAISE(ABORT, 'forced metric failure'); END`); err != nil {
		t.Fatal(err)
	}
	providerErr := connector.RetryableError("google.http_429", errors.New("private response body"))
	if err := worker.failProviderTask(t.Context(), providerTaskCandidate{id: runID, workspaceID: "workspace-a", fencingToken: 4}, providerErr, now); err == nil {
		t.Fatal("metric failure was swallowed")
	}
	var status, errorCode string
	var attempts int
	if err := database.QueryRowContext(t.Context(), `SELECT status,last_error_code,attempt_count FROM _integration_provider_runs WHERE id=?`, runID).Scan(&status, &errorCode, &attempts); err != nil || status != "processing" || errorCode != "" || attempts != 0 {
		t.Fatalf("provider failure committed without metric: status=%q code=%q attempts=%d err=%v", status, errorCode, attempts, err)
	}
}
