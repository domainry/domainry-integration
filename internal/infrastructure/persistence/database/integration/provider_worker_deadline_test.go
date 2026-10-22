package integration

import (
	"errors"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestProviderTaskFailurePersistsOnlyValidatedProviderCode(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-task-coded-failure")
	worker := NewWorkerStore(database, dialect, nil, nil, "worker")
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	result, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"workspace-a", providerRunKindState, "poll", "crm", "probe", "primary", "poll", 1, "", "", `{}`, "processing", "", 0, now.UnixMilli(), worker.workerID, now.Add(time.Minute).UnixMilli(), 5, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	runID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	providerErr := connector.RetryableError("google.http_429", errors.New("private response body"))
	if err := worker.failProviderTask(t.Context(), providerTaskCandidate{id: runID, workspaceID: "workspace-a", fencingToken: 4}, providerErr, now); err == nil {
		t.Fatal("provider failure was swallowed")
	}
	var stored string
	if err := database.QueryRowContext(t.Context(), `SELECT last_error_code FROM _integration_provider_runs WHERE id=?`, runID).Scan(&stored); err != nil || stored != "google.http_429" {
		t.Fatalf("stored provider error=%q err=%v", stored, err)
	}
	snapshot, err := NewManagementStore(database, dialect, nil, nil).providerRunSnapshot(t.Context(), now)
	if err != nil || snapshot.GoogleHTTP429Failures != 1 || snapshot.GoogleHTTP429Events5m != 1 || snapshot.GoogleHTTP429EventsHour != 1 {
		t.Fatalf("Google HTTP 429 snapshot=%+v err=%v", snapshot, err)
	}
	if err := worker.failProviderTask(t.Context(), providerTaskCandidate{id: runID, workspaceID: "workspace-a", fencingToken: 4}, providerErr, now); err == nil {
		t.Fatal("stale worker failure was accepted")
	}
	snapshot, err = NewManagementStore(database, dialect, nil, nil).providerRunSnapshot(t.Context(), now)
	if err != nil || snapshot.GoogleHTTP429Events5m != 1 {
		t.Fatalf("stale worker duplicated 429 metric=%+v err=%v", snapshot, err)
	}
}

func TestProviderTaskFailurePreservesLastCommittedSuccess(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-task-last-success")
	worker := NewWorkerStore(database, dialect, nil, nil, "worker")
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	previousSuccess := now.Add(-time.Hour).UnixMilli()
	result, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,last_success_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"workspace-a", providerRunKindState, "poll", "crm", "probe", "primary", "poll", 1, "", "", `{}`, "processing", "", 0, now.UnixMilli(), previousSuccess, worker.workerID, now.Add(time.Minute).UnixMilli(), 5, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	runID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	candidate := providerTaskCandidate{id: runID, workspaceID: "workspace-a", fencingToken: 4}
	if err = worker.failProviderTask(t.Context(), candidate, errors.New("provider temporarily unavailable"), now); err == nil {
		t.Fatal("provider failure was swallowed")
	}
	var status string
	var lastSuccessAt int64
	var attempts int
	if err = database.QueryRowContext(t.Context(), "SELECT status,last_success_at,attempt_count FROM _integration_provider_runs WHERE workspace_id=? AND id=?", "workspace-a", runID).Scan(&status, &lastSuccessAt, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || lastSuccessAt != previousSuccess || attempts != 1 {
		t.Fatalf("failed run status=%q last_success_at=%d attempts=%d", status, lastSuccessAt, attempts)
	}
}

func TestProviderDeadlineComparisonUsesInstants(t *testing.T) {
	now := time.Date(2026, 9, 11, 10, 0, 0, 123100000, time.UTC)
	for _, tc := range []struct {
		due, lease int64
		ready      bool
	}{
		{now.UnixMilli(), 0, true},
		{now.Add(-time.Millisecond).UnixMilli(), 0, true},
		{now.Add(time.Millisecond).UnixMilli(), 0, false},
		{now.UnixMilli(), now.Add(time.Millisecond).UnixMilli(), false},
		{now.UnixMilli(), now.UnixMilli(), true},
	} {
		got, err := providerDeadlineReady(tc.due, tc.lease, now)
		if got != tc.ready || err != nil {
			t.Fatalf("due=%d lease=%d ready=%v err=%v", tc.due, tc.lease, got, err)
		}
	}
	bound := providerDeadlineScanEnd(now)
	if bound != now.UnixMilli() {
		t.Fatalf("scan end=%d want=%d", bound, now.UnixMilli())
	}
}

func TestProviderCommitDeadlinePrecisionAndLeaseGuard(t *testing.T) {
	for _, tc := range []struct {
		name, due, lease string
		claimed          bool
	}{
		{"earlier shortened fraction", "2026-09-11T10:00:00.123Z", "", true},
		{"future millisecond", "2026-09-11T10:00:00.124Z", "", false},
		{"unexpired lease", "2026-09-11T10:00:00.123Z", "2026-09-11T10:00:00.124Z", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dialect := webPushTestDatabase(t, "provider-commit-deadline")
			due, lease := timestampMillis(tc.due), timestampMillis(tc.lease)
			result, err := db.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, "w", providerRunKindCommit, "commit", "crm", "probe", "missing", "poll", 0, "ack", "contract", `{}`, "pending", "", 0, due, "", lease, 0, due, due)
			if err != nil {
				t.Fatal(err)
			}
			runID, err := result.LastInsertId()
			if err != nil {
				t.Fatal(err)
			}
			delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider: &backgroundOperationsTestProvider{}}, emptyDeliveryTestSecrets{})
			worker := NewWorkerStore(db, dialect, delivery, nil, "worker")
			worker.clock = func() time.Time { return time.Date(2026, 9, 11, 10, 0, 0, 123100000, time.UTC) }
			processed, err := worker.processProviderCommits(t.Context(), 10)
			var fence int
			if e := db.QueryRowContext(t.Context(), "SELECT fencing_token FROM _integration_provider_runs WHERE run_kind=? AND id=?", providerRunKindCommit, runID).Scan(&fence); e != nil {
				t.Fatal(e)
			}
			if tc.claimed {
				// The deliberately missing connection fails after the claim. No external
				// Provider is called, but the actual SQL lease must have been acquired.
				if processed != 1 || fence != 1 {
					t.Fatalf("due commit missed: processed=%d fence=%d err=%v", processed, fence, err)
				}
			} else if err != nil || processed != 0 || fence != 0 {
				t.Fatalf("future commit claimed: processed=%d fence=%d err=%v", processed, fence, err)
			}
		})
	}
}
