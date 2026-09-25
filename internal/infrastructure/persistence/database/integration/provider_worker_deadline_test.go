package integration

import (
	"testing"
	"time"
)

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
			_, err := db.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (id,workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, "commit", "w", providerRunKindCommit, "commit", "crm", "probe", "missing", "poll", 0, "ack", "contract", `{}`, "pending", "", 0, due, "", lease, 0, due, due)
			if err != nil {
				t.Fatal(err)
			}
			delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider: &backgroundOperationsTestProvider{}}, emptyDeliveryTestSecrets{})
			worker := NewWorkerStore(db, dialect, delivery, nil, "worker")
			worker.clock = func() time.Time { return time.Date(2026, 9, 11, 10, 0, 0, 123100000, time.UTC) }
			processed, err := worker.processProviderCommits(t.Context(), 10)
			var fence int
			if e := db.QueryRowContext(t.Context(), "SELECT fencing_token FROM _integration_provider_runs WHERE run_kind=? AND id=?", providerRunKindCommit, "commit").Scan(&fence); e != nil {
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
