package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

type providerCapacityTestProvider struct{}

func (*providerCapacityTestProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "capacity", ProviderRevision: "1.0.0"}
}
func (*providerCapacityTestProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, nil
}
func (*providerCapacityTestProvider) BackgroundTasks(connector.Connection) []connector.BackgroundTaskDescriptor {
	return []connector.BackgroundTaskDescriptor{{Key: "sync", StateVersion: 1}, {Key: "watch", StateVersion: 1}}
}
func (*providerCapacityTestProvider) ProcessBackground(context.Context, connector.BackgroundRequest) (connector.BackgroundResult, error) {
	return connector.BackgroundResult{State: json.RawMessage(`{}`), NextDueAt: time.Now().UTC().Add(time.Hour)}, nil
}

type concurrentProviderCapacityTestProvider struct {
	active   atomic.Int32
	maximum  atomic.Int32
	started  chan string
	release  chan struct{}
	finished chan string
}

func (*concurrentProviderCapacityTestProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "capacity_parallel", ProviderRevision: "1.0.0"}
}

func (*concurrentProviderCapacityTestProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, nil
}

func (*concurrentProviderCapacityTestProvider) BackgroundTasks(connector.Connection) []connector.BackgroundTaskDescriptor {
	return []connector.BackgroundTaskDescriptor{{Key: "sync", StateVersion: 1}}
}

func (p *concurrentProviderCapacityTestProvider) ProcessBackground(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	active := p.active.Add(1)
	defer p.active.Add(-1)
	for {
		maximum := p.maximum.Load()
		if active <= maximum || p.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	p.started <- request.Connection.Key
	select {
	case <-ctx.Done():
		return connector.BackgroundResult{}, ctx.Err()
	case <-p.release:
	}
	p.finished <- request.Connection.Key
	return connector.BackgroundResult{State: json.RawMessage(`{}`), NextDueAt: request.Now.Add(time.Hour)}, nil
}

func TestProviderTasksExecuteConcurrentlyAcrossConnections(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-capacity-parallel")
	provider := &concurrentProviderCapacityTestProvider{
		started: make(chan string, 4), release: make(chan struct{}), finished: make(chan string, 4),
	}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)
	for index := range 4 {
		workspaceID := fmt.Sprintf("workspace-%d", index)
		if _, err := management.UpsertConnection(t.Context(), workspaceID, "connection", "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "capacity_parallel", Status: "active"}); err != nil {
			t.Fatal(err)
		}
	}
	worker := NewWorkerStore(database, dialect, delivery, nil, "parallel-worker")
	worker.clock = func() time.Time { return time.Now().UTC().Add(time.Second) }
	done := make(chan error, 1)
	go func() {
		processed, err := worker.ProcessDueProviderTasks(t.Context(), 4)
		if err == nil && processed != 4 {
			err = fmt.Errorf("processed %d tasks, want 4", processed)
		}
		done <- err
	}()
	for index := range 4 {
		select {
		case <-provider.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d provider calls started before release", index)
		}
	}
	if maximum := provider.maximum.Load(); maximum != 4 {
		t.Fatalf("maximum concurrent provider calls=%d, want 4", maximum)
	}
	close(provider.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parallel provider batch did not finish")
	}
}

func TestProviderTaskSelectionIsFairAcrossWorkspaces(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-capacity-fairness")
	provider := &providerCapacityTestProvider{}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)
	for index := 0; index < 4; index++ {
		if _, err := management.UpsertConnection(t.Context(), "workspace-a", "connection-a-"+string(rune('0'+index)), "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "capacity", Status: "active"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := management.UpsertConnection(t.Context(), "workspace-b", "connection-b", "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "capacity", Status: "active"}); err != nil {
		t.Fatal(err)
	}
	worker := NewWorkerStore(database, dialect, delivery, nil, "worker-a")
	now := time.Now().UTC().Add(time.Second)
	candidates, err := worker.listFairProviderTaskCandidates(t.Context(), now, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) < 2 || candidates[0].workspaceID == candidates[1].workspaceID {
		t.Fatalf("workspace round-robin candidates=%#v", candidates)
	}
	foundB := false
	for _, candidate := range candidates {
		foundB = foundB || candidate.workspaceID == "workspace-b"
	}
	if !foundB {
		t.Fatalf("workspace-b was starved: %#v", candidates)
	}
}

func TestProviderTaskSelectionRotatesAcrossBatches(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-capacity-rotation")
	provider := &providerCapacityTestProvider{}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)
	for _, workspaceID := range []string{"workspace-a", "workspace-b", "workspace-c", "workspace-d"} {
		if _, err := management.UpsertConnection(t.Context(), workspaceID, "connection", "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "capacity", Status: "active"}); err != nil {
			t.Fatal(err)
		}
	}
	worker := NewWorkerStore(database, dialect, delivery, nil, "worker-a")
	now := time.Now().UTC().Add(time.Second)
	want := []string{"workspace-a", "workspace-b", "workspace-c", "workspace-d", "workspace-a", "workspace-b"}
	for batch, workspaceID := range want {
		candidates, err := worker.listFairProviderTaskCandidates(t.Context(), now, 1)
		if err != nil || len(candidates) != 1 || candidates[0].workspaceID != workspaceID {
			t.Fatalf("batch %d: candidates=%#v err=%v, want workspace %q", batch, candidates, err, workspaceID)
		}
	}
}

func TestOwnedFailedProviderRunRetryIsClaimedByWorker(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-retry-worker")
	provider := &providerCapacityTestProvider{}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)
	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "personal-a", "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "capacity", Status: "active"}); err != nil {
		t.Fatal(err)
	}
	if _, err := management.RegisterConnectionAccount(accountScope(t.Context(), "admin", true), "workspace-a", "personal-a", "admin", integrationsdk.ConnectionAccountRegistration{Scope: integrationsdk.ConnectionAccountScopePersonal, OwnerUserID: "user-a"}); err != nil {
		t.Fatal(err)
	}
	var updatedAt int64
	if err := database.QueryRowContext(t.Context(), "SELECT updated_at FROM _integration_provider_runs WHERE workspace_id=? AND run_kind=? AND connection_key=? AND run_key=?", "workspace-a", providerRunKindState, "personal-a", "sync").Scan(&updatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), "UPDATE _integration_provider_runs SET status='failed', attempt_count=2, last_error_code='provider.failed' WHERE workspace_id=? AND run_kind=? AND connection_key=? AND run_key=?", "workspace-a", providerRunKindState, "personal-a", "sync"); err != nil {
		t.Fatal(err)
	}
	subject := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: integrationsdk.ConnectionAccountAccess{Personal: true}}
	task, err := management.RetryConnectionAccountBackgroundTask(accountScope(t.Context(), "user-a", false), subject, "personal-a", "sync", integrationsdk.ConnectionAccountBackgroundRetryRequest{ExpectedUpdatedAt: timestampString(updatedAt)})
	if err != nil || task.Status != "ready" || task.LastSuccessAt != "" {
		t.Fatalf("retry task=%#v err=%v", task, err)
	}
	worker := NewWorkerStore(database, dialect, delivery, nil, "worker-a")
	worker.clock = func() time.Time { return time.Now().UTC().Add(time.Second) }
	if processed, err := worker.ProcessDueProviderTasks(t.Context(), 10); err != nil || processed < 1 {
		t.Fatalf("retried run not processed: processed=%d err=%v", processed, err)
	}
	var status string
	var lastSuccessAt int64
	if err := database.QueryRowContext(t.Context(), "SELECT status,last_success_at FROM _integration_provider_runs WHERE workspace_id=? AND run_kind=? AND connection_key=? AND run_key=?", "workspace-a", providerRunKindState, "personal-a", "sync").Scan(&status, &lastSuccessAt); err != nil {
		t.Fatal(err)
	}
	if status != "ready" || lastSuccessAt == 0 {
		t.Fatalf("worker did not commit retried run: status=%q last_success_at=%d", status, lastSuccessAt)
	}
}

func TestExpiredProcessingProviderRunIsRecoveredWithoutStaleWorkerCommit(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-expired-processing")
	provider := &providerCapacityTestProvider{}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)
	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "personal-a", "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "capacity", Status: "active"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(time.Second)
	if _, err := database.ExecContext(t.Context(), "UPDATE _integration_provider_runs SET due_at=? WHERE workspace_id=? AND connection_key=? AND run_key=?", now.Add(time.Hour).UnixMilli(), "workspace-a", "personal-a", "watch"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), "UPDATE _integration_provider_runs SET status='processing',due_at=?,lease_owner=?,lease_expires_at=?,fencing_token=7 WHERE workspace_id=? AND connection_key=? AND run_key=?", now.Add(-time.Minute).UnixMilli(), "worker-old:integration", now.Add(time.Minute).UnixMilli(), "workspace-a", "personal-a", "sync"); err != nil {
		t.Fatal(err)
	}
	worker := NewWorkerStore(database, dialect, delivery, nil, "worker-new")
	worker.clock = func() time.Time { return now }
	if candidates, err := worker.listFairProviderTaskCandidates(t.Context(), now, 10); err != nil || len(candidates) != 0 {
		t.Fatalf("live processing lease was selected: candidates=%#v err=%v", candidates, err)
	}
	if _, err := database.ExecContext(t.Context(), "UPDATE _integration_provider_runs SET lease_expires_at=? WHERE workspace_id=? AND connection_key=? AND run_key=?", now.Add(-time.Millisecond).UnixMilli(), "workspace-a", "personal-a", "sync"); err != nil {
		t.Fatal(err)
	}
	candidates, err := worker.listFairProviderTaskCandidates(t.Context(), now, 10)
	if err != nil || len(candidates) != 1 || candidates[0].taskKey != "sync" || candidates[0].status != "processing" {
		t.Fatalf("expired processing lease was not selected: candidates=%#v err=%v", candidates, err)
	}
	if processed, err := worker.ProcessDueProviderTasks(t.Context(), 10); err != nil || processed != 1 {
		t.Fatalf("expired processing task was not recovered: processed=%d err=%v", processed, err)
	}
	var status, payload, lastError string
	var lastSuccessAt, fencingToken int64
	if err := database.QueryRowContext(t.Context(), "SELECT status,payload_json,last_error_code,last_success_at,fencing_token FROM _integration_provider_runs WHERE workspace_id=? AND connection_key=? AND run_key=?", "workspace-a", "personal-a", "sync").Scan(&status, &payload, &lastError, &lastSuccessAt, &fencingToken); err != nil {
		t.Fatal(err)
	}
	if status != "ready" || payload != `{}` || lastError != "" || lastSuccessAt != now.UnixMilli() || fencingToken != 8 {
		t.Fatalf("recovered task status=%q payload=%s error=%q success=%d fence=%d", status, payload, lastError, lastSuccessAt, fencingToken)
	}
	oldWorker := NewWorkerStore(database, dialect, delivery, nil, "worker-old")
	oldCandidate := candidates[0]
	oldCandidate.fencingToken--
	if err := oldWorker.commitProviderTaskResult(t.Context(), oldCandidate, connector.BackgroundResult{State: json.RawMessage(`{"stale":true}`), NextDueAt: now.Add(time.Hour)}, now); err == nil {
		t.Fatal("stale worker committed after a new worker recovered its lease")
	}
	if err := database.QueryRowContext(t.Context(), "SELECT payload_json,last_success_at FROM _integration_provider_runs WHERE workspace_id=? AND connection_key=? AND run_key=?", "workspace-a", "personal-a", "sync").Scan(&payload, &lastSuccessAt); err != nil || payload != `{}` || lastSuccessAt != now.UnixMilli() {
		t.Fatalf("stale worker changed committed state: payload=%s success=%d err=%v", payload, lastSuccessAt, err)
	}
}

func TestProviderCapacityEnforcesConnectionSingleFlightAndFourRateScopes(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-capacity-single-flight")
	provider := &providerCapacityTestProvider{}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)
	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "connection-a", "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "capacity", Status: "active"}); err != nil {
		t.Fatal(err)
	}
	worker := NewWorkerStore(database, dialect, delivery, nil, "worker-a")
	now := time.Now().UTC().Add(time.Second)
	candidates, err := worker.listFairProviderTaskCandidates(t.Context(), now, 2)
	if err != nil || len(candidates) != 2 {
		t.Fatalf("candidates=%#v err=%v", candidates, err)
	}
	owner, claimed, err := worker.acquireProviderTask(t.Context(), candidates[0], now)
	if err != nil || !claimed || owner == "" {
		t.Fatalf("first claim owner=%q claimed=%v err=%v", owner, claimed, err)
	}
	if _, claimed, err = worker.acquireProviderTask(t.Context(), candidates[1], now); err != nil || claimed {
		t.Fatalf("concurrent connection claim claimed=%v err=%v", claimed, err)
	}
	var scopes int
	if err = database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _integration_provider_rate_limits WHERE used_count=1`).Scan(&scopes); err != nil || scopes != 4 {
		t.Fatalf("rate scopes=%d err=%v", scopes, err)
	}
	if err = worker.releaseProviderConnectionLease(t.Context(), candidates[0], owner, now); err != nil {
		t.Fatal(err)
	}
}

func TestProviderRateWindowRejectsExhaustedScopeAndResetsNextMinute(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-capacity-rate-window")
	now := time.Date(2026, 9, 26, 11, 22, 30, 0, time.UTC)
	scope := providerRateScope{workspaceID: "workspace-a", kind: "test", key: "scope-a", limit: 1}
	allowed, err := consumeProviderRate(t.Context(), database, dialect, scope, now)
	if err != nil || !allowed {
		t.Fatalf("first rate token allowed=%v err=%v", allowed, err)
	}
	allowed, err = consumeProviderRate(t.Context(), database, dialect, scope, now.Add(10*time.Second))
	if err != nil || allowed {
		t.Fatalf("exhausted rate token allowed=%v err=%v", allowed, err)
	}
	allowed, err = consumeProviderRate(t.Context(), database, dialect, scope, now.Add(time.Minute))
	if err != nil || !allowed {
		t.Fatalf("next-window rate token allowed=%v err=%v", allowed, err)
	}
}
