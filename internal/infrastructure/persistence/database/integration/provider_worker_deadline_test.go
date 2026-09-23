package integration

import (
	"testing"
	"time"
)

func TestProviderDeadlineComparisonUsesInstants(t *testing.T) {
	now := time.Date(2026, 9, 11, 10, 0, 0, 123100000, time.UTC)
	for _, tc := range []struct {
		due, lease     string
		ready, invalid bool
	}{
		{"2026-09-11T10:00:00.123Z", "", true, false},
		{"2026-09-11T10:00:00Z", "", true, false},
		{"2026-09-11T10:00:00.1231Z", "", true, false},
		{"2026-09-11T10:00:00.1232Z", "", false, false},
		{"2026-09-11T10:00:00.123Z", "2026-09-11T10:00:00.1232Z", false, false},
		{"2026-09-11T10:00:00.123Z", "2026-09-11T10:00:00.123Z", true, false},
		{"bad", "", false, true},
		{"2026-09-11T10:00:00Z", "bad", false, true},
	} {
		got, err := providerDeadlineReady(tc.due, tc.lease, now)
		if got != tc.ready || (err != nil) != tc.invalid {
			t.Fatalf("due=%s lease=%s ready=%v err=%v", tc.due, tc.lease, got, err)
		}
	}
	bound := providerDeadlineScanEnd(now)
	for _, within := range []string{"2026-09-11T10:00:00Z", "2026-09-11T10:00:00.123Z", "2026-09-11T10:00:00.999999999Z"} {
		if within >= bound {
			t.Fatalf("current second excluded: %s", within)
		}
	}
	if "2026-09-11T10:00:01.000000001Z" < bound {
		t.Fatal("next second included")
	}
}

func TestProviderCommitDeadlinePrecisionAndLeaseGuard(t *testing.T) {
	for _, tc := range []struct {
		name, due, lease string
		claimed          bool
	}{
		{"earlier shortened fraction", "2026-09-11T10:00:00.123Z", "", true},
		{"future fraction", "2026-09-11T10:00:00.1232Z", "", false},
		{"unexpired lease", "2026-09-11T10:00:00.123Z", "2026-09-11T10:00:00.1232Z", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dialect := webPushTestDatabase(t, "provider-commit-deadline")
			_, err := db.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (id,workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, "commit", "w", providerRunKindCommit, "commit", "crm", "probe", "missing", "poll", 0, "ack", "contract", `{}`, "pending", "", 0, tc.due, "", tc.lease, 0, tc.due, tc.due)
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
