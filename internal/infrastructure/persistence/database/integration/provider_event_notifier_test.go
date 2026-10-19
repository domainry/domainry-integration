package integration

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

type eventNotifierFunc func()

func (f eventNotifierFunc) EventsCommitted() { f() }

type commitHintProvider struct {
	slowStarted chan struct{}
	releaseSlow chan struct{}
	failCommit  bool
}

func (*commitHintProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "commit_hint", ProviderRevision: "1.0.0"}
}
func (*commitHintProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, nil
}
func (*commitHintProvider) BackgroundTasks(connector.Connection) []connector.BackgroundTaskDescriptor {
	return []connector.BackgroundTaskDescriptor{{Key: "sync", StateVersion: 1}}
}
func (p *commitHintProvider) ProcessBackground(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	result := connector.BackgroundResult{State: json.RawMessage(`{}`), NextDueAt: request.Now.Add(time.Hour)}
	if request.Connection.Key == "slow" {
		close(p.slowStarted)
		select {
		case <-ctx.Done():
			return connector.BackgroundResult{}, ctx.Err()
		case <-p.releaseSlow:
		}
		return result, nil
	}
	if p.slowStarted != nil {
		select {
		case <-p.slowStarted:
		case <-ctx.Done():
			return connector.BackgroundResult{}, ctx.Err()
		}
	}
	result.Events = []connector.BackgroundEvent{{ExternalID: "message-one", EventType: "mail.received", Payload: json.RawMessage(`{"test":true}`)}}
	if p.failCommit {
		// Valid result; fail inside the transaction after inserting the event.
		result.Commit = []connector.BackgroundCommit{{OperationKey: "missing", ContractSHA256: strings.Repeat("a", 64), Payload: json.RawMessage(`{}`)}}
	}
	return result, nil
}

func TestProviderEventNotifiesAfterCommitWithoutWaitingForSlowBatchPeer(t *testing.T) {
	db, dialect := webPushTestDatabase(t, "commit-hint-fast-peer")
	provider := &commitHintProvider{slowStarted: make(chan struct{}), releaseSlow: make(chan struct{})}
	delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(db, dialect, nil, delivery)
	for _, key := range []string{"fast", "slow"} {
		if _, err := management.UpsertConnection(t.Context(), "workspace-a", key, "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "commit_hint", Status: "active"}); err != nil {
			t.Fatal(err)
		}
	}
	worker := NewWorkerStore(db, dialect, delivery, nil, "commit-hint-worker")
	worker.clock = func() time.Time { return time.Now().UTC().Add(time.Second) }
	notified := make(chan struct{}, 1)
	worker.SetEventWorkNotifier(eventNotifierFunc(func() { notified <- struct{}{} }))
	done := make(chan error, 1)
	go func() {
		_, err := worker.ProcessDueProviderTasks(t.Context(), 2)
		done <- err
	}()
	select {
	case <-notified:
	case <-time.After(2 * time.Second):
		close(provider.releaseSlow)
		t.Fatal("fast committed event waited for the slow batch peer")
	}
	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_events WHERE event_type='mail.received'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("notification preceded durable event commit: count=%d err=%v", count, err)
	}
	select {
	case err := <-done:
		t.Fatalf("slow peer was not still running: %v", err)
	default:
	}
	close(provider.releaseSlow)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch did not finish after peer release")
	}
}

func TestProviderEventRollbackDoesNotNotifyOrPersistEvent(t *testing.T) {
	db, dialect := webPushTestDatabase(t, "commit-hint-rollback")
	provider := &commitHintProvider{failCommit: true}
	delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(db, dialect, nil, delivery)
	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "fast", "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "commit_hint", Status: "active"}); err != nil {
		t.Fatal(err)
	}
	worker := NewWorkerStore(db, dialect, delivery, nil, "commit-hint-worker")
	worker.clock = func() time.Time { return time.Now().UTC().Add(time.Second) }
	var notifications atomic.Int32
	worker.SetEventWorkNotifier(eventNotifierFunc(func() { notifications.Add(1) }))
	if _, err := worker.ProcessDueProviderTasks(t.Context(), 1); err == nil {
		t.Fatal("expected transactional commit failure")
	}
	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_events").Scan(&count); err != nil || count != 0 || notifications.Load() != 0 {
		t.Fatalf("rolled back events leaked: count=%d notifications=%d err=%v", count, notifications.Load(), err)
	}
}

func TestConcurrentProviderBatchesKeepExistingConnectionLeases(t *testing.T) {
	db, dialect := webPushTestDatabase(t, "commit-hint-concurrent-batches")
	provider := &concurrentProviderCapacityTestProvider{started: make(chan string, 4), release: make(chan struct{}), finished: make(chan string, 4)}
	delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(db, dialect, nil, delivery)
	for _, key := range []string{"one", "two"} {
		if _, err := management.UpsertConnection(t.Context(), "workspace-a", key, "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "capacity_parallel", Status: "active"}); err != nil {
			t.Fatal(err)
		}
	}
	worker := NewWorkerStore(db, dialect, delivery, nil, "shared-worker")
	worker.clock = func() time.Time { return time.Now().UTC().Add(time.Second) }
	done := make(chan error, 1)
	go func() {
		_, err := worker.ProcessDueProviderTasks(t.Context(), 2)
		done <- err
	}()
	for range 2 {
		select {
		case <-provider.started:
		case <-time.After(2 * time.Second):
			close(provider.release)
			t.Fatal("first batch did not acquire both connections")
		}
	}
	processed, err := worker.ProcessDueProviderTasks(t.Context(), 2)
	if err != nil || processed != 0 || provider.maximum.Load() != 2 {
		t.Fatalf("second batch duplicated active mailboxes: processed=%d concurrency=%d err=%v", processed, provider.maximum.Load(), err)
	}
	close(provider.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
