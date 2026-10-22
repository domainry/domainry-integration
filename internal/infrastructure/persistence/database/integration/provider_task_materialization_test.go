package integration

import (
	"database/sql"
	"testing"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

func TestConnectionLifecycleMaterializesAndCancelsProviderRuns(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-task-materialization")
	provider := &backgroundOperationsTestProvider{}
	providers := deliveryTestProviders{provider: provider}
	delivery := NewDeliveryStore(database, dialect, providers, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)

	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "primary", "admin", integrationsdk.ConnectionInput{
		ConnectorKey: "crm", ProviderKey: "background", Name: "Primary", Status: "active",
	}); err != nil {
		t.Fatal(err)
	}
	assertProviderRunState(t, database, providerRunKindState, "poll", "ready", 0)
	var initialSuccess int64
	if err := database.QueryRowContext(t.Context(), "SELECT last_success_at FROM _integration_provider_runs WHERE workspace_id=? AND run_kind=? AND run_key=?", "workspace-a", providerRunKindState, "poll").Scan(&initialSuccess); err != nil || initialSuccess != 0 {
		t.Fatalf("new task falsely records success=%d err=%v", initialSuccess, err)
	}
	if _, err := database.ExecContext(t.Context(), "UPDATE _integration_provider_runs SET last_success_at=? WHERE workspace_id=? AND run_kind=? AND run_key=?", time.Now().UTC().UnixMilli(), "workspace-a", providerRunKindState, "poll"); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().UnixMilli()
	insert, args, err := query.NewInsertBuilder(dialect, providerRunTable).
		Columns("workspace_id", "run_kind", "run_key", "connector_key", "provider_key", "connection_key", "task_key", "state_version", "operation_key", "contract_sha256", "payload_json", "status", "last_error_code", "attempt_count", "due_at", "lease_owner", "lease_expires_at", "fencing_token", "created_at", "updated_at").
		Values("workspace-a", providerRunKindCommit, "pending-commit", "crm", "background", "primary", "poll", 0, "ack", "contract", `{}`, "pending", "", 0, now, "", int64(0), 7, now, now).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = database.ExecContext(t.Context(), insert, args...); err != nil {
		t.Fatal(err)
	}

	if _, err = management.SetConnectionStatus(t.Context(), "workspace-a", "primary", "inactive", "admin"); err != nil {
		t.Fatal(err)
	}
	assertProviderRunState(t, database, providerRunKindState, "poll", "cancelled", 1)
	assertProviderRunState(t, database, providerRunKindCommit, "pending-commit", "cancelled", 8)

	if _, err = management.SetConnectionStatus(t.Context(), "workspace-a", "primary", "active", "admin"); err != nil {
		t.Fatal(err)
	}
	assertProviderRunState(t, database, providerRunKindState, "poll", "ready", 2)
	var reactivatedSuccess int64
	if err := database.QueryRowContext(t.Context(), "SELECT last_success_at FROM _integration_provider_runs WHERE workspace_id=? AND run_kind=? AND run_key=?", "workspace-a", providerRunKindState, "poll").Scan(&reactivatedSuccess); err != nil || reactivatedSuccess != 0 {
		t.Fatalf("reactivated task retained stale success=%d err=%v", reactivatedSuccess, err)
	}
	assertProviderRunState(t, database, providerRunKindCommit, "pending-commit", "cancelled", 8)

	if err = management.DeleteConnection(t.Context(), "workspace-a", "primary"); err != nil {
		t.Fatal(err)
	}
	assertProviderRunState(t, database, providerRunKindState, "poll", "cancelled", 3)
	var connections int
	if err = database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_connections WHERE workspace_id=? AND connection_key=?", "workspace-a", "primary").Scan(&connections); err != nil {
		t.Fatal(err)
	}
	if connections != 0 {
		t.Fatalf("deleted connection still exists: %d", connections)
	}
}

func TestProviderWorkerDoesNotScanConnectionsToCreateTasks(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-worker-no-connection-scan")
	provider := &backgroundOperationsTestProvider{}
	providers := deliveryTestProviders{provider: provider}
	delivery := NewDeliveryStore(database, dialect, providers, emptyDeliveryTestSecrets{})
	now := time.Now().UTC().UnixMilli()
	insert, args, err := query.NewInsertBuilder(dialect, "_integration_connections").
		Columns("connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").
		Values("direct", "workspace-a", "crm", "background", "Direct", "active", `{}`, `{}`, "admin", now, now).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = database.ExecContext(t.Context(), insert, args...); err != nil {
		t.Fatal(err)
	}

	workers := NewWorkerStore(database, dialect, delivery, nil, "runtime-a")
	processed, err := workers.ProcessDueProviderTasks(t.Context(), 10)
	if err != nil || processed != 0 || provider.calls != 0 {
		t.Fatalf("worker rediscovered connections: processed=%d calls=%d err=%v", processed, provider.calls, err)
	}
	var runs int
	if err = database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_provider_runs").Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("worker created %d provider runs from a connection scan", runs)
	}
}

func assertProviderRunState(t *testing.T, database *sql.DB, runKind, runKey, wantStatus string, wantFence int64) {
	t.Helper()
	var status string
	var fence int64
	if err := database.QueryRowContext(t.Context(), "SELECT status,fencing_token FROM _integration_provider_runs WHERE workspace_id=? AND run_kind=? AND run_key=?", "workspace-a", runKind, runKey).Scan(&status, &fence); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || fence != wantFence {
		t.Fatalf("provider run %s/%s status=%q fence=%d, want status=%q fence=%d", runKind, runKey, status, fence, wantStatus, wantFence)
	}
}
