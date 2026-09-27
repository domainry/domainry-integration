package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

// Run explicitly with -bench. This isolates the cost of observing 100k recent
// deduplicated receipts, not webhook ingress throughput or account count.
func BenchmarkGooglePushSnapshotAmong100kRecentReceipts(b *testing.B) {
	b.StopTimer()
	database, dialect := webPushTestDatabase(b, "google-push-100k-receipts")
	now := time.Now().UTC()
	tx, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		b.Fatal(err)
	}
	insert, err := tx.PrepareContext(context.Background(), `INSERT INTO _integration_provider_inbox (id,workspace_id,connector_key,provider_key,ingress_connection_key,external_id,route_kind,route_hash,route_bucket,payload_json,status,routed_count,received_at,published_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	for index := range 100_000 {
		id := fmt.Sprintf("push-%06d", index)
		if _, err := insert.ExecContext(context.Background(), id, "platform", "google_workspace", "google", "shared-push", id, "email", id, index%256, `{}`, "routed", 1, now.Add(-time.Duration(index%3600)*time.Second).UnixMilli(), now.Add(-time.Duration(index%3600+60)*time.Second).UnixMilli(), now.UnixMilli()); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.StartTimer()
	for range b.N {
		var snapshot integrationsdk.ProviderRunSnapshot
		if err := observeGooglePush(context.Background(), database, dialect, now, &snapshot); err != nil || snapshot.GooglePushReceivedHour != 100_000 || snapshot.GooglePushTimedHour != 100_000 {
			b.Fatalf("Google Push snapshot=%+v err=%v", snapshot, err)
		}
	}
}
