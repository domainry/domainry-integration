package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	ormdialect "github.com/domainry/domainry-orm/dialect"
)

const (
	providerCapacityAcceptanceEnv = "INTEGRATION_PROVIDER_CAPACITY_ACCEPTANCE"
	providerCapacityWorkspaces    = 1000
	providerCapacityDefaultBatch  = 25
	providerCapacityWorkerBatch   = 500
	providerCapacityReadWorkers   = 16
	providerCapacityReadOps       = 8
	providerCapacitySeekWorkers   = 4
	providerCapacitySeekOps       = 6
)

type providerCapacityModel struct {
	accounts                    int64
	executionsPerAccountPerHour float64
	targetUtilization           float64
	providerLatencyP95          time.Duration
}

type providerCapacityPlan struct {
	tasksPerSecond        float64
	requiredRatePerMinute int64
	requiredConcurrency   int
	requiredProcesses     int
	accountsPerWorkspace  int64
	requiredWorkspaces    int64
}

func (m providerCapacityModel) plan() providerCapacityPlan {
	tasksPerSecond := float64(m.accounts) * m.executionsPerAccountPerHour / float64(time.Hour/time.Second)
	requiredRatePerMinute := int64(math.Ceil(tasksPerSecond * 60 / m.targetUtilization))
	requiredConcurrency := int(math.Ceil(tasksPerSecond * m.providerLatencyP95.Seconds() / m.targetUtilization))
	processConcurrency := min(providerTaskExecutionConcurrency, providerCapacityDefaultBatch)
	requiredProcesses := int(math.Ceil(float64(requiredConcurrency) / float64(processConcurrency)))
	workspaceRate := float64(providerWorkspaceRatePerMinute) * m.targetUtilization
	accountsPerWorkspace := int64(math.Floor(workspaceRate * 60 / m.executionsPerAccountPerHour))
	return providerCapacityPlan{
		tasksPerSecond:        tasksPerSecond,
		requiredRatePerMinute: requiredRatePerMinute,
		requiredConcurrency:   requiredConcurrency,
		requiredProcesses:     requiredProcesses,
		accountsPerWorkspace:  accountsPerWorkspace,
		requiredWorkspaces:    int64(math.Ceil(float64(m.accounts) / float64(accountsPerWorkspace))),
	}
}

func TestProviderAccountCapacityModelCovers10kAnd100kBaseline(t *testing.T) {
	// Outlook performs one Inbox and one Sent execution per 15-minute default
	// reconciliation cycle, so eight task executions per account-hour is the
	// stricter current Google/Microsoft steady-state baseline. Bootstrap pages,
	// retries and vendor API-call quotas are intentionally outside this model.
	for _, accounts := range []int64{10_000, 100_000} {
		plan := (providerCapacityModel{
			accounts: accounts, executionsPerAccountPerHour: 8,
			targetUtilization: 0.70, providerLatencyP95: 2 * time.Second,
		}).plan()
		if plan.requiredRatePerMinute > providerGlobalRatePerMinute || plan.requiredRatePerMinute > providerKindRatePerMinute {
			t.Fatalf("%d-account baseline needs %d task admissions/minute, global=%d provider=%d", accounts, plan.requiredRatePerMinute, providerGlobalRatePerMinute, providerKindRatePerMinute)
		}
		if plan.requiredConcurrency <= 0 || plan.requiredProcesses <= 0 || plan.accountsPerWorkspace <= 0 || plan.requiredWorkspaces <= 0 {
			t.Fatalf("%d-account plan is invalid: %+v", accounts, plan)
		}
		t.Logf("accounts=%d tasks/s=%.2f rate/min=%d concurrency=%d processes=%d accounts/workspace=%d workspaces=%d", accounts, plan.tasksPerSecond, plan.requiredRatePerMinute, plan.requiredConcurrency, plan.requiredProcesses, plan.accountsPerWorkspace, plan.requiredWorkspaces)
	}
}

type capacityAcceptanceProvider struct {
	delay   time.Duration
	active  atomic.Int32
	maximum atomic.Int32
}

func (*capacityAcceptanceProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "email", ProviderKey: "capacity_acceptance", ProviderRevision: "1.0.0"}
}

func (*capacityAcceptanceProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, nil
}

func (*capacityAcceptanceProvider) BackgroundTasks(connector.Connection) []connector.BackgroundTaskDescriptor {
	return []connector.BackgroundTaskDescriptor{{Key: "mail_sync", StateVersion: 1}}
}

func (p *capacityAcceptanceProvider) ProcessBackground(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	active := p.active.Add(1)
	defer p.active.Add(-1)
	for {
		maximum := p.maximum.Load()
		if active <= maximum || p.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return connector.BackgroundResult{}, ctx.Err()
	case <-time.After(p.delay):
		return connector.BackgroundResult{State: json.RawMessage(`{}`), NextDueAt: request.Now.Add(time.Hour)}, nil
	}
}

type capacityLatencies struct {
	mu     sync.Mutex
	values []time.Duration
}

func (l *capacityLatencies) observe(started time.Time) {
	l.mu.Lock()
	l.values = append(l.values, time.Since(started))
	l.mu.Unlock()
}

func (l *capacityLatencies) p95() time.Duration {
	l.mu.Lock()
	values := append([]time.Duration(nil), l.values...)
	l.mu.Unlock()
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[int(math.Ceil(float64(len(values))*0.95))-1]
}

func TestProviderAccountCapacityAcceptance(t *testing.T) {
	if os.Getenv(providerCapacityAcceptanceEnv) != "1" {
		t.Skip("set " + providerCapacityAcceptanceEnv + "=1 to run the 10k/100k account load acceptance")
	}
	for _, accounts := range []int{10_000, 100_000} {
		t.Run(fmt.Sprintf("accounts_%d", accounts), func(t *testing.T) {
			database, dialect, store, worker, provider := setupProviderCapacityAcceptance(t, accounts)
			readP95, seekP95 := runProviderCapacityReadPressure(t, store, worker, accounts)
			if readP95 > 100*time.Millisecond {
				t.Fatalf("current-owner account list p95=%s exceeds 100ms", readP95)
			}
			if seekP95 > 250*time.Millisecond {
				t.Fatalf("fair due-task selection p95=%s exceeds 250ms", seekP95)
			}

			started := time.Now()
			processed, err := worker.ProcessDueProviderTasks(t.Context(), providerCapacityWorkerBatch)
			executionDuration := time.Since(started)
			if err != nil || processed != providerCapacityWorkerBatch {
				t.Fatalf("provider burst processed=%d duration=%s err=%v", processed, executionDuration, err)
			}
			throughput := float64(processed) / executionDuration.Seconds()
			if throughput < 50 {
				t.Fatalf("provider burst throughput=%.2f tasks/s below 50", throughput)
			}
			if maximum := provider.maximum.Load(); maximum < 16 {
				t.Fatalf("provider burst used only %d concurrent calls", maximum)
			}

			monitor := NewManagementStore(database, dialect, nil, nil)
			started = time.Now()
			snapshot, err := monitor.ProviderRunSnapshot(t.Context())
			monitorDuration := time.Since(started)
			if err != nil || snapshot.ReadyDue != int64(accounts-providerCapacityWorkerBatch) {
				t.Fatalf("provider snapshot ready_due=%d duration=%s err=%v", snapshot.ReadyDue, monitorDuration, err)
			}
			if monitorDuration > 500*time.Millisecond {
				t.Fatalf("provider snapshot duration=%s exceeds 500ms", monitorDuration)
			}
			t.Logf("accounts=%d owner_list_p95=%s select_500_p95=%s provider_burst=%.2f_tasks/s provider_concurrency=%d snapshot=%s", accounts, readP95, seekP95, throughput, provider.maximum.Load(), monitorDuration)
		})
	}
}

func setupProviderCapacityAcceptance(t *testing.T, accounts int) (*sql.DB, modulehost.Dialect, *ManagementStore, *WorkerStore, *capacityAcceptanceProvider) {
	t.Helper()
	database, dialect := capacityAcceptanceDatabase(t, accounts)
	seedProviderCapacityAcceptance(t, database, accounts)
	provider := &capacityAcceptanceProvider{delay: 25 * time.Millisecond}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	store := NewManagementStore(database, dialect, nil, delivery)
	worker := NewWorkerStore(database, dialect, delivery, nil, "capacity-execution")
	worker.clock = func() time.Time { return time.Now().UTC().Add(time.Second) }
	return database, dialect, store, worker, provider
}

func runProviderCapacityReadPressure(t *testing.T, store *ManagementStore, worker *WorkerStore, accounts int) (time.Duration, time.Duration) {
	t.Helper()
	var reads, seeks capacityLatencies
	errors := make(chan error, providerCapacityReadWorkers+providerCapacitySeekWorkers)
	start := make(chan struct{})
	var group sync.WaitGroup
	for reader := range providerCapacityReadWorkers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for operation := range providerCapacityReadOps {
				index := (reader*providerCapacityReadOps + operation) * 7919 % accounts
				workspaceID := fmt.Sprintf("workspace-%04d", index%providerCapacityWorkspaces)
				userID := fmt.Sprintf("user-%06d", index)
				subject := integrationsdk.ConnectionAccountSubject{WorkspaceID: workspaceID, UserID: userID, Access: integrationsdk.ConnectionAccountAccess{Personal: true}}
				started := time.Now()
				access := integrationmodel.WithAccessScope(t.Context(), integrationmodel.AccessScope{
					WorkspaceID: workspaceID, PermissionKey: "integration.connection_accounts.capacity", ActorID: userID, AllowedUserIDs: []string{userID},
				})
				values, err := store.ListConnectionAccounts(access, subject)
				reads.observe(started)
				if err != nil || len(values) != 1 || values[0].OwnerUserID != userID || len(values[0].Background) != 1 {
					errors <- fmt.Errorf("owner list workspace=%s user=%s values=%d err=%v", workspaceID, userID, len(values), err)
					return
				}
			}
		}()
	}
	for seeker := range providerCapacitySeekWorkers {
		group.Add(1)
		go func() {
			defer group.Done()
			current := NewWorkerStore(worker.database, worker.dialect, worker.delivery, nil, fmt.Sprintf("capacity-seek-%d", seeker))
			<-start
			for range providerCapacitySeekOps {
				started := time.Now()
				values, err := current.listFairProviderTaskCandidates(t.Context(), time.Now().UTC().Add(time.Second), providerCapacityWorkerBatch)
				seeks.observe(started)
				if err != nil || len(values) != providerCapacityWorkerBatch {
					errors <- fmt.Errorf("fair selection candidates=%d err=%v", len(values), err)
					return
				}
				seen := make(map[string]struct{}, len(values))
				for _, value := range values {
					if _, duplicate := seen[value.workspaceID]; duplicate {
						errors <- fmt.Errorf("fair selection repeated workspace %q", value.workspaceID)
						return
					}
					seen[value.workspaceID] = struct{}{}
				}
			}
		}()
	}
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	return reads.p95(), seeks.p95()
}

func seedProviderCapacityAcceptance(t *testing.T, database *sql.DB, accounts int) {
	t.Helper()
	now := time.Now().UTC().Add(-time.Minute).UnixMilli()
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	connectionInsert, err := tx.PrepareContext(t.Context(), `INSERT INTO _integration_connections (id,connection_key,workspace_id,connector_key,provider_key,status,config_json,secret_refs_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer connectionInsert.Close()
	accountInsert, err := tx.PrepareContext(t.Context(), `INSERT INTO _integration_connection_accounts (id,workspace_id,connection_key,scope,owner_user_id,created_by,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer accountInsert.Close()
	runInsert, err := tx.PrepareContext(t.Context(), `INSERT INTO _integration_provider_runs (id,workspace_id,run_kind,run_key,connector_key,provider_key,connection_key,task_key,state_version,operation_key,contract_sha256,payload_json,status,last_error_code,attempt_count,due_at,lease_owner,lease_expires_at,fencing_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer runInsert.Close()
	for index := range accounts {
		workspaceID := fmt.Sprintf("workspace-%04d", index%providerCapacityWorkspaces)
		connectionKey := fmt.Sprintf("connection-%06d", index)
		userID := fmt.Sprintf("user-%06d", index)
		if _, err = connectionInsert.ExecContext(t.Context(), "connection-row-"+connectionKey, connectionKey, workspaceID, "email", "capacity_acceptance", "active", `{}`, `{}`, now, now); err != nil {
			t.Fatal(err)
		}
		if _, err = accountInsert.ExecContext(t.Context(), "account-row-"+connectionKey, workspaceID, connectionKey, "personal", userID, "capacity", now, now); err != nil {
			t.Fatal(err)
		}
		if _, err = runInsert.ExecContext(t.Context(), "run-row-"+connectionKey, workspaceID, providerRunKindState, "mail_sync", "email", "capacity_acceptance", connectionKey, "mail_sync", 1, "", "", `{}`, "ready", "", 0, now, "", int64(0), 0, now, now); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = database.ExecContext(t.Context(), "ANALYZE"); err != nil {
		t.Fatal(err)
	}
}

func capacityAcceptanceDatabase(t *testing.T, accounts int) (*sql.DB, modulehost.Dialect) {
	t.Helper()
	path := filepath.Join(t.TempDir(), fmt.Sprintf("provider-capacity-%d.db", accounts))
	database, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	database.SetMaxOpenConns(64)
	rawDialect, err := ormdialect.New(ormdialect.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	dialect := rawDialect.WithSchema("")
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err = database.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	return database, dialect
}
