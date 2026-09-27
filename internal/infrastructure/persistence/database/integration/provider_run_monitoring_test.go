package integration

import (
	"fmt"
	"testing"
	"time"
)

func TestProviderRunSnapshotAggregatesOnlyStateRunsAndCurrentRateWindow(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-run-monitoring")
	store := NewManagementStore(database, dialect, nil, nil)
	now := time.Date(2026, 9, 27, 12, 34, 45, 0, time.UTC)
	for index, run := range []struct {
		kind, status string
		due, lease   time.Time
	}{
		{providerRunKindState, "ready", now.Add(-10 * time.Minute), time.Time{}},
		{providerRunKindState, "ready", now.Add(time.Hour), time.Time{}},
		{providerRunKindState, "failed", now.Add(-5 * time.Minute), time.Time{}},
		{providerRunKindState, "failed", now.Add(time.Minute), time.Time{}},
		{providerRunKindState, "processing", now.Add(-time.Minute), now.Add(-time.Second)},
		{providerRunKindState, "processing", now.Add(-time.Minute), now.Add(time.Minute)},
		{providerRunKindState, "dead_letter", time.Time{}, time.Time{}},
		{providerRunKindCommit, "pending", now.Add(-time.Hour), time.Time{}},
	} {
		id := fmt.Sprintf("run-%d", index)
		due, lease := int64(0), int64(0)
		if !run.due.IsZero() {
			due = run.due.UnixMilli()
		}
		if !run.lease.IsZero() {
			lease = run.lease.UnixMilli()
		}
		if _, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_runs (id,workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, "workspace-a", run.kind, "poll", "crm", "probe", id, "poll", 1, "", "", `{}`, run.status, "", 0, due, "", lease, 0, now.UnixMilli(), now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_rate_limits (id,workspace_id,scope_kind,scope_key,window_started_at,used_count,updated_at) VALUES (?,?,?,?,?,?,?)`, "global", "_platform", "global", "all", now.Truncate(time.Minute).UnixMilli(), 432, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.providerRunSnapshot(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ReadyDue != 1 || snapshot.Failed != 2 || snapshot.FailedDue != 1 || snapshot.Processing != 2 || snapshot.ExpiredProcessing != 1 || snapshot.DeadLetter != 1 || snapshot.GlobalRateUsed != 432 || snapshot.GlobalRateLimit != providerGlobalRatePerMinute {
		t.Fatalf("provider run snapshot=%+v", snapshot)
	}
	if snapshot.OldestRunnableDueAt != now.Add(-10*time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("oldest due=%q", snapshot.OldestRunnableDueAt)
	}
	snapshot, err = store.providerRunSnapshot(t.Context(), now.Add(time.Minute))
	if err != nil || snapshot.GlobalRateUsed != 0 || snapshot.RateWindowStartedAt != "" {
		t.Fatalf("expired rate window snapshot=%+v err=%v", snapshot, err)
	}
}

func TestProviderRunSnapshotAggregatesVerifiedGooglePushInboxWithoutExposingAccounts(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "google-push-monitoring")
	store := NewManagementStore(database, dialect, nil, nil)
	now := time.Date(2026, 9, 27, 12, 30, 0, 0, time.UTC)
	for index, event := range []struct {
		connector, provider, status string
		received, published         time.Time
		bucket                      int
	}{
		{"google_workspace", "google", "routed", now.Add(-2 * time.Minute), now.Add(-3 * time.Minute), 7},
		{"google_workspace", "google", "unmatched", now.Add(-time.Minute), now.Add(-8 * time.Minute), 7},
		{"google_workspace", "google", "routed", now.Add(-50 * time.Minute), time.Time{}, 7},
		{"google_workspace", "google", "routed", now.Add(-10 * time.Minute), now.Add(-16 * time.Minute), 42},
		{"google_workspace", "google", "routed", now.Add(-3 * time.Minute), now.Add(time.Minute), 42},
		{"google_workspace", "google", "routed", now.Add(-80 * time.Minute), now.Add(-81 * time.Minute), 7},
		{"email", "google_workspace", "routed", now.Add(-time.Minute), now.Add(-10 * time.Minute), 7},
		{"google_workspace", "google", "received", now.Add(-time.Minute), now.Add(-10 * time.Minute), 7},
		{"google_workspace", "google", "routed", now.Add(time.Minute), now, 7},
	} {
		published := int64(0)
		if !event.published.IsZero() {
			published = event.published.UnixMilli()
		}
		id := fmt.Sprintf("push-%d", index)
		if _, err := database.ExecContext(t.Context(), `INSERT INTO _integration_provider_inbox (id,workspace_id,connector_key,provider_key,ingress_connection_key,external_id,route_kind,route_hash,route_bucket,payload_json,status,routed_count,received_at,published_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, "platform", event.connector, event.provider, "push-ingress", id, "email", fmt.Sprintf("hash-%d", index), event.bucket, `{}`, event.status, 0, event.received.UnixMilli(), published, event.received.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := store.providerRunSnapshot(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.GooglePushReceivedHour != 5 || snapshot.GooglePushNoWakeHour != 1 || snapshot.GooglePushTimedHour != 3 || snapshot.GooglePushDelayed5mHour != 2 || snapshot.GooglePushMaxDelayMsHour != (7*time.Minute).Milliseconds() {
		t.Fatalf("Google Push latency snapshot=%+v", snapshot)
	}
	if snapshot.GooglePushHotBucket == nil || *snapshot.GooglePushHotBucket != 7 || snapshot.GooglePushHotBucketEventsHour != 3 {
		t.Fatalf("Google Push hotspot snapshot=%+v", snapshot)
	}
	snapshot, err = store.providerRunSnapshot(t.Context(), now.Add(2*time.Hour))
	if err != nil || snapshot.GooglePushReceivedHour != 0 || snapshot.GooglePushTimedHour != 0 || snapshot.GooglePushHotBucket != nil || snapshot.GooglePushMaxDelayMsHour != 0 {
		t.Fatalf("expired Google Push snapshot=%+v err=%v", snapshot, err)
	}
}
