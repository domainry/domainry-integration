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

type providerStreamTestProvider struct {
	generation atomic.Int32
	active     atomic.Int32
	maximum    atomic.Int32
	started    chan int
	stopped    chan int
}

func (*providerStreamTestProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "chat", ProviderKey: "stream", ProviderRevision: "1.0.0", SecretFields: []connector.SecretField{{Key: "token", Name: "Token", CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestOptional}}}
}

func (*providerStreamTestProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, nil
}

func (*providerStreamTestProvider) Streams(connection connector.Connection) []connector.StreamDescriptor {
	if connection.Status != "active" {
		return nil
	}
	return []connector.StreamDescriptor{{Key: "messages", StateVersion: 1}}
}

func (p *providerStreamTestProvider) RunStream(ctx context.Context, request connector.StreamRequest, sink connector.StreamSink) error {
	generation := int(p.generation.Add(1))
	active := p.active.Add(1)
	defer p.active.Add(-1)
	for {
		maximum := p.maximum.Load()
		if active <= maximum || p.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	state, _ := json.Marshal(map[string]any{"generation": generation})
	payload, _ := json.Marshal(map[string]any{"generation": generation})
	if err := sink.Commit(ctx, connector.StreamCheckpoint{
		State:    state,
		Progress: &connector.BackgroundProgress{Phase: "connected", Unit: "message", CompletedUnits: int64(generation), InitializationComplete: true},
		Events:   []connector.BackgroundEvent{{ExternalID: fmt.Sprintf("stream-event-%d", generation), EventType: "chat.message", Payload: payload}},
	}); err != nil {
		return err
	}
	p.started <- generation
	<-ctx.Done()
	p.stopped <- generation
	return ctx.Err()
}

func TestProviderStreamHasOneOwnerAndRestartsImmediatelyOnConnectionChange(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-stream-owner")
	provider := &providerStreamTestProvider{started: make(chan int, 4), stopped: make(chan int, 4)}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	worker := NewWorkerStore(database, dialect, delivery, nil, "runtime-a")
	management := NewManagementStore(database, dialect, nil, delivery)
	management.ConfigureProviderStreamChangeNotifier(worker)
	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "primary", "admin", integrationsdk.ConnectionInput{ConnectorKey: "chat", ProviderKey: "stream", Name: "Primary one", Status: "active"}); err != nil {
		t.Fatal(err)
	}
	assertProviderRunState(t, database, providerRunKindStream, "messages", "ready", 0)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- worker.RunProviderStreams(ctx) }()
	awaitStreamGeneration(t, provider.started, 1)
	if maximum := provider.maximum.Load(); maximum != 1 {
		t.Fatalf("maximum active streams=%d, want 1", maximum)
	}
	var status, state string
	if err := database.QueryRowContext(t.Context(), "SELECT status,payload_json FROM _integration_provider_runs WHERE workspace_id=? AND run_kind=? AND run_key=?", "workspace-a", providerRunKindStream, "messages").Scan(&status, &state); err != nil || status != "running" || state != `{"generation":1}` {
		t.Fatalf("status=%q state=%s err=%v", status, state, err)
	}
	var events int
	if err := database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_events WHERE workspace_id=? AND provider=?", "workspace-a", "stream").Scan(&events); err != nil || events != 1 {
		t.Fatalf("events=%d err=%v", events, err)
	}

	changedAt := time.Now()
	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "primary", "admin", integrationsdk.ConnectionInput{ConnectorKey: "chat", ProviderKey: "stream", Name: "Primary two", Status: "active"}); err != nil {
		t.Fatal(err)
	}
	awaitStreamGeneration(t, provider.stopped, 1)
	awaitStreamGeneration(t, provider.started, 2)
	if time.Since(changedAt) > time.Second {
		t.Fatalf("connection generation took %s to replace", time.Since(changedAt))
	}
	if maximum := provider.maximum.Load(); maximum != 1 {
		t.Fatalf("overlapping stream owners=%d", maximum)
	}

	cancel()
	awaitStreamGeneration(t, provider.stopped, 2)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream supervisor did not stop")
	}
}

func TestProviderStreamSecretChangeFencesCurrentGeneration(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-stream-secret-fence")
	provider := &providerStreamTestProvider{started: make(chan int, 1), stopped: make(chan int, 1)}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, deliveryTestSecrets{})
	management := NewManagementStore(database, dialect, transactionTestCipher{}, delivery)
	if _, err := management.UpsertSecret(t.Context(), "workspace-a", "token", "admin", integrationsdk.SecretInput{Kind: "bearer_token", Value: "first"}); err != nil {
		t.Fatal(err)
	}
	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "primary", "admin", integrationsdk.ConnectionInput{ConnectorKey: "chat", ProviderKey: "stream", Status: "active", SecretRefs: map[string]string{"token": "secret:token"}}); err != nil {
		t.Fatal(err)
	}
	assertProviderRunState(t, database, providerRunKindStream, "messages", "ready", 0)
	if _, err := management.UpsertSecret(t.Context(), "workspace-a", "token", "admin", integrationsdk.SecretInput{Kind: "bearer_token", Value: "second"}); err != nil {
		t.Fatal(err)
	}
	assertProviderRunState(t, database, providerRunKindStream, "messages", "ready", 1)
}

func awaitStreamGeneration(t *testing.T, values <-chan int, want int) {
	t.Helper()
	select {
	case got := <-values:
		if got != want {
			t.Fatalf("stream generation=%d, want %d", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for stream generation %d", want)
	}
}
