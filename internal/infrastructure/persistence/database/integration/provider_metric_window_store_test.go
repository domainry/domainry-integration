package integration

import (
	"errors"
	"fmt"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestGoogleHTTP429EventsAggregateAcrossWorkerInstances(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-429-multi-worker")
	now := time.Date(2026, 9, 27, 12, 34, 0, 0, time.UTC)
	for index := 0; index < 2; index++ {
		id := fmt.Sprintf("run-%d", index)
		worker := NewWorkerStore(database, dialect, nil, nil, fmt.Sprintf("worker-%d", index))
		if _, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (id,workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, "workspace-a", providerRunKindState, "poll", "crm", "probe", id, "poll", 1, "", "", `{}`, "processing", "", 0, now.UnixMilli(), worker.workerID, now.Add(time.Minute).UnixMilli(), 5, now.UnixMilli(), now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
		if err := worker.failProviderTask(t.Context(), providerTaskCandidate{id: id, workspaceID: "workspace-a", fencingToken: 4}, connector.RetryableError("google.http_429", errors.New("private response")), now); err == nil {
			t.Fatal("provider failure was swallowed")
		}
	}
	snapshot, err := NewManagementStore(database, dialect, nil, nil).providerRunSnapshot(t.Context(), now)
	if err != nil || snapshot.GoogleHTTP429Events5m != 2 || snapshot.GoogleHTTP429Failures != 2 {
		t.Fatalf("multi-worker 429 snapshot=%+v err=%v", snapshot, err)
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

func TestGoogleHTTP429MetricWriteFailureRollsBackProviderTaskFailure(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-429-atomic")
	worker := NewWorkerStore(database, dialect, nil, nil, "worker")
	now := time.Date(2026, 9, 27, 12, 34, 0, 0, time.UTC)
	if _, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (id,workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"run", "workspace-a", providerRunKindState, "poll", "crm", "probe", "primary", "poll", 1, "", "", `{}`, "processing", "", 0, now.UnixMilli(), worker.workerID, now.Add(time.Minute).UnixMilli(), 5, now.UnixMilli(), now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `CREATE TRIGGER reject_provider_metric BEFORE INSERT ON _integration_provider_metric_windows BEGIN SELECT RAISE(ABORT, 'forced metric failure'); END`); err != nil {
		t.Fatal(err)
	}
	providerErr := connector.RetryableError("google.http_429", errors.New("private response body"))
	if err := worker.failProviderTask(t.Context(), providerTaskCandidate{id: "run", workspaceID: "workspace-a", fencingToken: 4}, providerErr, now); err == nil {
		t.Fatal("metric failure was swallowed")
	}
	var status, errorCode string
	var attempts int
	if err := database.QueryRowContext(t.Context(), `SELECT status,last_error_code,attempt_count FROM _integration_provider_runs WHERE id='run'`).Scan(&status, &errorCode, &attempts); err != nil || status != "processing" || errorCode != "" || attempts != 0 {
		t.Fatalf("provider failure committed without metric: status=%q code=%q attempts=%d err=%v", status, errorCode, attempts, err)
	}
}
