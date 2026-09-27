package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

// Run explicitly with -bench: seeding 100k foreign accounts is too expensive
// for the normal unit suite. This measures one current-user list and one due
// batch selection, not concurrent server throughput or a full worker cycle.
func BenchmarkConnectionAccountListAmong100kForeignAccounts(b *testing.B) {
	store, _ := setupConnectionAccountStore(b)
	createAccountConnection(b, store, "personal-a", "token-a", "user-a", integrationsdk.ConnectionAccountScopePersonal)
	now := time.Now().UTC().UnixMilli()
	database, ok := store.database.(*sql.DB)
	if !ok {
		b.Fatal("capacity fixture needs an owned SQL database")
	}
	tx, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		b.Fatal(err)
	}
	connectionInsert, err := tx.PrepareContext(context.Background(), `INSERT INTO _integration_connections (id,connection_key,workspace_id,connector_key,provider_key,status,config_json,secret_refs_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	accountInsert, err := tx.PrepareContext(context.Background(), `INSERT INTO _integration_connection_accounts (id,workspace_id,connection_key,scope,owner_user_id,created_by,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	runInsert, err := tx.PrepareContext(context.Background(), `INSERT INTO _integration_provider_runs (id,workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100_000; i++ {
		key := fmt.Sprintf("foreign-%06d", i)
		if _, err = connectionInsert.ExecContext(context.Background(), "connection-"+key, key, "workspace-a", "crm", "probe", "active", `{}`, `{}`, now, now); err != nil {
			b.Fatal(err)
		}
		if _, err = accountInsert.ExecContext(context.Background(), "account-"+key, "workspace-a", key, "personal", "other-user-"+key, "admin", now, now); err != nil {
			b.Fatal(err)
		}
		if _, err = runInsert.ExecContext(context.Background(), "run-"+key, "workspace-a", providerRunKindState, "poll", "crm", "probe", key, "poll", 1, "", "", `{}`, "ready", "", 0, now, "", int64(0), 0, now, now); err != nil {
			b.Fatal(err)
		}
	}
	if _, err = runInsert.ExecContext(context.Background(), "run-personal-a", "workspace-a", providerRunKindState, "poll", "crm", "probe", "personal-a", "poll", 1, "", "", `{}`, "ready", "", 0, now, "", int64(0), 0, now, now); err != nil {
		b.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		b.Fatal(err)
	}
	subject := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: integrationsdk.ConnectionAccountAccess{Personal: true}}
	statement, args, err := query.NewSelectBuilder(store.dialect, "_integration_connection_accounts").Alias("a").
		Projections(connectionAccountProjection()...).Join(connectionAccountJoin()).Where(connectionAccountAccess(subject)).
		OrderBy(query.AscendingExpression(query.QualifiedColumn("a", "connection_key"))).Build()
	if err != nil {
		b.Fatal(err)
	}
	plan, err := database.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+statement, args...)
	if err != nil {
		b.Fatal(err)
	}
	indexed := false
	for plan.Next() {
		var id, parent, unused int
		var detail string
		if err = plan.Scan(&id, &parent, &unused, &detail); err != nil {
			b.Fatal(err)
		}
		if strings.Contains(detail, "SEARCH a USING") && strings.Contains(detail, "idx_integration_connection_account_owner") {
			indexed = true
		}
	}
	if err = plan.Err(); err != nil {
		b.Fatal(err)
	}
	if err = plan.Close(); err != nil {
		b.Fatal(err)
	}
	if !indexed {
		b.Fatal("100k-account list did not use the current-owner index")
	}
	ctx := accountScope(context.Background(), "user-a", false)
	b.Run("list", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			accounts, err := store.ListConnectionAccounts(ctx, subject)
			if err != nil || len(accounts) != 1 || accounts[0].Key != "personal-a" || len(accounts[0].Background) != 1 {
				b.Fatalf("visible accounts=%d err=%v", len(accounts), err)
			}
		}
	})
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			account, err := store.GetConnectionAccount(ctx, subject, "personal-a")
			if err != nil || account.Key != "personal-a" || len(account.Background) != 1 {
				b.Fatalf("account=%q err=%v", account.Key, err)
			}
		}
	})
	worker := NewWorkerStore(database, store.dialect, nil, nil, "capacity-benchmark")
	availabilityStatement, availabilityArgs, err := query.NewSelectBuilder(store.dialect, providerRunTable).
		Columns("id").Where(dueProviderTaskPredicate(time.Now().UTC().Add(time.Second))).Limit(1).Build()
	if err != nil {
		b.Fatal(err)
	}
	availabilityPlan, err := database.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+availabilityStatement, availabilityArgs...)
	if err != nil {
		b.Fatal(err)
	}
	availabilityIndexed := false
	for availabilityPlan.Next() {
		var id, parent, unused int
		var detail string
		if err := availabilityPlan.Scan(&id, &parent, &unused, &detail); err != nil {
			b.Fatal(err)
		}
		availabilityIndexed = availabilityIndexed || strings.Contains(detail, "idx_integration_provider_run_due")
	}
	if err := availabilityPlan.Err(); err != nil {
		b.Fatal(err)
	}
	_ = availabilityPlan.Close()
	if !availabilityIndexed {
		b.Fatal("due-task existence check missed the status/due index")
	}
	for _, after := range []string{"", "workspace-a"} {
		predicate := dueProviderTaskPredicate(time.Now().UTC().Add(time.Second))
		if after != "" {
			predicate = query.And(predicate, query.GreaterThan("workspace_id", after))
		}
		statement, values, err := query.NewSelectBuilder(store.dialect, providerRunTable).Columns("workspace_id").Where(predicate).OrderBy(query.Ascending("workspace_id")).Limit(1).Build()
		if err != nil {
			b.Fatal(err)
		}
		rows, err := database.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+statement, values...)
		if err != nil {
			b.Fatal(err)
		}
		seekIndexed := false
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				b.Fatal(err)
			}
			seekIndexed = seekIndexed || strings.Contains(detail, "idx_integration_provider_run_workspace_due")
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		_ = rows.Close()
		if !seekIndexed {
			b.Fatalf("due workspace seek after %q missed its index", after)
		}
	}
	statement, values, err := query.NewSelectBuilder(store.dialect, providerRunTable).
		Columns("id", "workspace_id", "connector_key", "provider_key", "connection_key", "task_key", "state_version", "payload_json", "status", "attempt_count", "fencing_token", "wake_sequence", "due_at", "lease_expires_at").
		Where(query.And(dueProviderTaskPredicate(time.Now().UTC().Add(time.Second)), query.Equal("workspace_id", "workspace-a"))).
		OrderBy(query.Ascending("due_at"), query.Ascending("connection_key"), query.Ascending("task_key")).Limit(25).Build()
	if err != nil {
		b.Fatal(err)
	}
	planRows, err := database.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+statement, values...)
	if err != nil {
		b.Fatal(err)
	}
	candidateIndexed := false
	for planRows.Next() {
		var id, parent, unused int
		var detail string
		if err := planRows.Scan(&id, &parent, &unused, &detail); err != nil {
			b.Fatal(err)
		}
		candidateIndexed = candidateIndexed || strings.Contains(detail, "idx_integration_provider_run_workspace_due")
		if strings.Contains(detail, "USE TEMP B-TREE FOR ORDER BY") {
			b.Fatalf("due workspace candidates require a full sort: %s", detail)
		}
	}
	if err := planRows.Err(); err != nil {
		b.Fatal(err)
	}
	_ = planRows.Close()
	if !candidateIndexed {
		b.Fatal("due workspace candidates missed their index")
	}
	b.Run("select_due_25", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			candidates, err := worker.listFairProviderTaskCandidates(context.Background(), time.Now().UTC().Add(time.Second), 25)
			if err != nil || len(candidates) != 25 {
				b.Fatalf("due candidates=%d err=%v", len(candidates), err)
			}
		}
	})
	monitor := NewManagementStore(database, store.dialect, nil, nil)
	b.Run("global_monitor_snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			snapshot, err := monitor.ProviderRunSnapshot(context.Background())
			if err != nil || snapshot.ReadyDue != 100_001 {
				b.Fatalf("provider-run snapshot=%+v err=%v", snapshot, err)
			}
		}
	})
	if _, err := database.ExecContext(context.Background(), `INSERT INTO _integration_provider_runs (id,workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"run-cold", "workspace-z", providerRunKindState, "poll", "crm", "probe", "cold", "poll", 1, "", "", `{}`, "ready", "", 0, now, "", int64(0), 0, now, now); err != nil {
		b.Fatal(err)
	}
	b.Run("select_hot_and_cold", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			candidates, err := worker.listFairProviderTaskCandidates(context.Background(), time.Now().UTC().Add(time.Second), 2)
			if err != nil || len(candidates) != 2 || candidates[0].workspaceID == candidates[1].workspaceID {
				b.Fatalf("hot/cold candidates=%#v err=%v", candidates, err)
			}
		}
	})
	workspaceTx, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		b.Fatal(err)
	}
	workspaceInsert, err := workspaceTx.PrepareContext(context.Background(), `INSERT INTO _integration_provider_runs (id,workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		workspaceID := fmt.Sprintf("workspace-%04d", i)
		if _, err := workspaceInsert.ExecContext(context.Background(), "run-"+workspaceID, workspaceID, providerRunKindState, "poll", "crm", "probe", "connection", "poll", 1, "", "", `{}`, "ready", "", 0, now, "", int64(0), 0, now, now); err != nil {
			b.Fatal(err)
		}
	}
	if err := workspaceTx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.Run("select_across_1000_workspaces", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			candidates, err := worker.listFairProviderTaskCandidates(context.Background(), time.Now().UTC().Add(time.Second), 25)
			if err != nil || len(candidates) != 25 {
				b.Fatalf("distributed candidates=%d err=%v", len(candidates), err)
			}
			seen := make(map[string]bool, len(candidates))
			for _, candidate := range candidates {
				if seen[candidate.workspaceID] {
					b.Fatalf("duplicate workspace in distributed batch: %q", candidate.workspaceID)
				}
				seen[candidate.workspaceID] = true
			}
		}
	})
	if _, err := database.ExecContext(context.Background(), `UPDATE _integration_provider_runs SET status='dead_letter' WHERE run_kind=?`, providerRunKindState); err != nil {
		b.Fatal(err)
	}
	b.Run("select_none_after_dead_letters", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			candidates, err := worker.listFairProviderTaskCandidates(context.Background(), time.Now().UTC().Add(time.Second), 25)
			if err != nil || len(candidates) != 0 {
				b.Fatalf("dead-letter candidates=%d err=%v", len(candidates), err)
			}
		}
	})
}
