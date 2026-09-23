package integration

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

var refreshFollowupFailure = errors.New("synthetic provider request failed after token rotation")
var refreshStorageFailure = errors.New("synthetic credential storage unavailable")

type refreshResultProvider struct {
	operationsTestProvider
	succeed bool
}

func (p *refreshResultProvider) Descriptor() connector.ProviderDescriptor {
	d := p.operationsTestProvider.Descriptor()
	d.SecretFields = []connector.SecretField{{Key: "token", Name: "Token", Required: true}}
	return d
}
func (p *refreshResultProvider) Call(_ context.Context, request connector.CallRequest) (connector.CallResult, error) {
	if request.Secrets["token"] != "original-token" {
		return connector.CallResult{}, errors.New("unexpected resolved credential")
	}
	p.calls++
	if p.duringCall != nil {
		p.duringCall()
	}
	result := connector.CallResult{SecretUpdates: map[string]string{"token": "rotated-token"}}
	if p.succeed {
		return result, nil
	}
	return result, refreshFollowupFailure
}
func (p *refreshResultProvider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.Call(ctx, connector.CallRequest{Secrets: request.Secrets})
	return connector.TestConnectionResult{Connected: err == nil, SecretUpdates: result.SecretUpdates}, err
}

// The real Integration material store uses its host cipher; the test supplies
// AES-GCM and keeps all credentials synthetic and scoped to a temporary DB.
type refreshTestCipher struct{ rejectRotation bool }

func (c *refreshTestCipher) EncryptSecretMaterial(_ context.Context, workspace, key, value string) (string, error) {
	if c.rejectRotation && value == "rotated-token" {
		return "", refreshStorageFailure
	}
	block, _ := aes.NewCipher([]byte(strings.Repeat("s", 32)))
	gcm, _ := cipher.NewGCM(block)
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, []byte(value), []byte(workspace+"\x00"+key))), nil
}
func (*refreshTestCipher) DecryptSecretMaterial(_ context.Context, workspace, key, value string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	block, _ := aes.NewCipher([]byte(strings.Repeat("s", 32)))
	gcm, _ := cipher.NewGCM(block)
	if len(raw) < gcm.NonceSize() {
		return "", errors.New("invalid test ciphertext")
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], []byte(workspace+"\x00"+key))
	return string(plain), err
}

func TestCredentialRotationPersistsIndependentlyOfProviderOutcome(t *testing.T) {
	for _, operation := range []string{"call", "delivery", "connection-test"} {
		for _, outcome := range []string{"succeeded", "failed", "storage-failed", "successful-provider-storage-failed", "writer-unavailable"} {
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				db, dialect := webPushTestDatabase(t, "credential-rotation")
				storageFailed := outcome == "storage-failed" || outcome == "successful-provider-storage-failed"
				providerSucceeded := outcome == "succeeded" || outcome == "successful-provider-storage-failed"
				crypt := &refreshTestCipher{rejectRotation: storageFailed}
				provider := &refreshResultProvider{succeed: providerSucceeded}
				resolver := NewSecretResolver(db, dialect, crypt)
				delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider}, resolver)
				if outcome == "writer-unavailable" {
					delivery.secrets = struct{ SecretReferenceResolver }{resolver}
				}
				management := NewManagementStore(db, dialect, crypt, delivery)
				scope := func(permission string) context.Context {
					return integrationmodel.WithAccessScope(t.Context(), integrationmodel.AccessScope{WorkspaceID: "workspace-a", PermissionKey: permission, ActorID: "user-a", Unrestricted: true})
				}
				ctx := scope("integration.secrets.upsert")
				if _, err := management.UpsertSecret(ctx, "workspace-a", "owned-token", "user-a", integrationsdk.SecretInput{Kind: "token", Value: "original-token"}); err != nil {
					t.Fatal(err)
				}
				refs := map[string]string{"token": "secret:owned-token"}
				ctx = scope("integration.connections.upsert")
				if _, err := management.UpsertConnection(ctx, "workspace-a", "owned-connection", "user-a", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Status: "active", SecretRefs: refs}); err != nil {
					t.Fatal(err)
				}
				var result any
				var err error
				switch operation {
				case "call":
					ctx = scope("integration.providers.call")
					result, err = NewOperationsStore(db, dialect, delivery, nil, newTestDefinitionStore()).Call(ctx, integrationmodel.ProviderCallRequest{RequestID: "rotation", WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "owned-connection", Operation: "lookup", Payload: json.RawMessage(`{}`), ActorID: "user-a"})
				case "delivery":
					ctx = scope("integration.deliveries.accept")
					result, err = delivery.Accept(ctx, integrationmodel.DeliveryRequest{MessageID: "rotation", WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "owned-connection", Operation: "lookup", Payload: json.RawMessage(`{}`)})
				case "connection-test":
					ctx = scope("integration.connections.test")
					result, err = management.TestConnection(ctx, "workspace-a", "owned-connection", integrationsdk.ConnectionTestRequest{})
				}
				if outcome == "succeeded" && err != nil || !providerSucceeded && !errors.Is(err, refreshFollowupFailure) {
					t.Fatal("provider outcome was lost", err)
				}
				if storageFailed && !errors.Is(err, refreshStorageFailure) {
					t.Fatal("credential persistence failure was hidden", err)
				}
				if outcome == "writer-unavailable" && (err == nil || !strings.Contains(err.Error(), "writer is unavailable")) {
					t.Fatal("missing credential writer was hidden", err)
				}
				wantStatus := "succeeded"
				if outcome != "succeeded" {
					wantStatus = "failed"
				}
				var status string
				switch r := result.(type) {
				case integrationmodel.ProviderCallResult:
					status = r.Invocation.Status
				case integrationmodel.DeliveryReceipt:
					status = r.Status
				case integrationsdk.ConnectionTestResult:
					status = string(r.Receipt.Status)
				}
				if status != wantStatus {
					t.Fatal("receipt status does not reflect outcome", status, wantStatus)
				}
				values, err := resolver.ResolveSecretReferences(ctx, "workspace-a", refs)
				want := "rotated-token"
				if storageFailed || outcome == "writer-unavailable" {
					want = "original-token"
				}
				if err != nil || values["token"] != want || provider.calls != 1 {
					t.Fatal("rotation was discarded or provider was called again", outcome, err, provider.calls)
				}
				if _, err = resolver.ResolveSecretReferences(ctx, "workspace-b", refs); err == nil {
					t.Fatal("credential leaked to another workspace")
				}
				var ciphertext string
				if err = db.QueryRowContext(ctx, "SELECT ciphertext FROM _integration_secret_materials WHERE workspace_id=? AND secret_key=?", "workspace-a", "owned-token").Scan(&ciphertext); err != nil || strings.Contains(ciphertext, "rotated-token") || strings.Contains(ciphertext, "original-token") {
					t.Fatal("credential material was not encrypted", err)
				}
				raw, _ := json.Marshal(result)
				if strings.Contains(string(raw), "rotated-token") || strings.Contains(string(raw), "original-token") {
					t.Fatal("public result contains credential material")
				}
			})
		}
	}
}

func TestProviderCredentialUpdateRejectsStaleAndRevokedSnapshotsAtomically(t *testing.T) {
	t.Run("newer refresh wins", func(t *testing.T) {
		db, dialect := webPushTestDatabase(t, "credential-refresh-cas")
		crypt := &refreshTestCipher{}
		resolver := NewSecretResolver(db, dialect, crypt)
		management := NewManagementStore(db, dialect, crypt, NewDeliveryStore(db, dialect, deliveryTestProviders{&refreshResultProvider{}}, resolver))
		if _, err := management.UpsertSecret(t.Context(), "workspace-a", "token", "user-a", integrationsdk.SecretInput{Kind: "token", Value: "original-token"}); err != nil {
			t.Fatal(err)
		}
		refs := map[string]string{"token": "secret:token"}
		first, err := resolver.ResolveSecretReferencesSnapshot(t.Context(), "workspace-a", refs)
		if err != nil {
			t.Fatal(err)
		}
		second, err := resolver.ResolveSecretReferencesSnapshot(t.Context(), "workspace-a", refs)
		if err != nil {
			t.Fatal(err)
		}
		if err := resolver.ApplySecretUpdatesIfCurrent(t.Context(), "workspace-a", refs, second.Versions, map[string]string{"token": "winner-token"}); err != nil {
			t.Fatal(err)
		}
		if err := resolver.ApplySecretUpdatesIfCurrent(t.Context(), "workspace-a", refs, first.Versions, map[string]string{"token": "late-token"}); !errors.Is(err, errProviderSecretChanged) {
			t.Fatal("late refresh was not rejected", err)
		}
		values, err := resolver.ResolveSecretReferences(t.Context(), "workspace-a", refs)
		if err != nil || values["token"] != "winner-token" {
			t.Fatal("late refresh overwrote the winner", values, err)
		}
	})

	t.Run("revocation rolls back the whole returned set", func(t *testing.T) {
		db, dialect := webPushTestDatabase(t, "credential-refresh-revoke")
		crypt := &refreshTestCipher{}
		resolver := NewSecretResolver(db, dialect, crypt)
		management := NewManagementStore(db, dialect, crypt, NewDeliveryStore(db, dialect, deliveryTestProviders{&refreshResultProvider{}}, resolver))
		for key, value := range map[string]string{"access": "original-access", "refresh": "original-refresh"} {
			if _, err := management.UpsertSecret(t.Context(), "workspace-a", key, "user-a", integrationsdk.SecretInput{Kind: "token", Value: value}); err != nil {
				t.Fatal(err)
			}
		}
		refs := map[string]string{"access_token": "secret:access", "refresh_token": "secret:refresh"}
		snapshot, err := resolver.ResolveSecretReferencesSnapshot(t.Context(), "workspace-a", refs)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := management.TransitionSecret(t.Context(), "workspace-a", "refresh", "revoke", "user-a"); err != nil {
			t.Fatal(err)
		}
		err = resolver.ApplySecretUpdatesIfCurrent(t.Context(), "workspace-a", refs, snapshot.Versions, map[string]string{"access_token": "late-access", "refresh_token": "late-refresh"})
		if !errors.Is(err, errProviderSecretChanged) {
			t.Fatal("revoked snapshot was not rejected", err)
		}
		access, err := resolver.ResolveSecretReferences(t.Context(), "workspace-a", map[string]string{"access_token": refs["access_token"]})
		if err != nil || access["access_token"] != "original-access" {
			t.Fatal("partial access-token update escaped rollback", access, err)
		}
		if _, err := resolver.ResolveSecretReferences(t.Context(), "workspace-a", map[string]string{"refresh_token": refs["refresh_token"]}); err == nil {
			t.Fatal("revoked refresh token became active again")
		}
		var ciphertext string
		if err := db.QueryRowContext(t.Context(), "SELECT ciphertext FROM _integration_secret_materials WHERE workspace_id=? AND secret_key=?", "workspace-a", "refresh").Scan(&ciphertext); err != nil {
			t.Fatal(err)
		}
		plain, err := crypt.DecryptSecretMaterial(t.Context(), "workspace-a", "refresh", ciphertext)
		if err != nil || plain != "original-refresh" {
			t.Fatal("revoked material was overwritten", err)
		}
	})
}

func TestProviderCredentialUpdateCompletesAfterCallerCancellation(t *testing.T) {
	db, dialect := webPushTestDatabase(t, "credential-refresh-cancel")
	crypt := &refreshTestCipher{}
	resolver := NewSecretResolver(db, dialect, crypt)
	delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{&refreshResultProvider{}}, resolver)
	management := NewManagementStore(db, dialect, crypt, delivery)
	if _, err := management.UpsertSecret(t.Context(), "workspace-a", "token", "user-a", integrationsdk.SecretInput{Kind: "token", Value: "original-token"}); err != nil {
		t.Fatal(err)
	}
	refs := map[string]string{"token": "secret:token"}
	snapshot, err := resolver.ResolveSecretReferencesSnapshot(t.Context(), "workspace-a", refs)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	err = delivery.persistProviderSecretUpdates(canceled, "workspace-a", refs, snapshot.Versions, map[string]string{"token": "rotated-after-cancel"}, context.Canceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("caller cancellation was lost", err)
	}
	values, err := resolver.ResolveSecretReferences(t.Context(), "workspace-a", refs)
	if err != nil || values["token"] != "rotated-after-cancel" {
		t.Fatal("completed provider rotation was canceled with the caller", values, err)
	}
}

func TestProviderCallCannotReactivateCredentialRevokedDuringRequest(t *testing.T) {
	db, dialect := webPushTestDatabase(t, "credential-refresh-request-revoke")
	crypt := &refreshTestCipher{}
	resolver := NewSecretResolver(db, dialect, crypt)
	provider := &refreshResultProvider{succeed: true}
	delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider}, resolver)
	management := NewManagementStore(db, dialect, crypt, delivery)
	if _, err := management.UpsertSecret(t.Context(), "workspace-a", "token", "user-a", integrationsdk.SecretInput{Kind: "token", Value: "original-token"}); err != nil {
		t.Fatal(err)
	}
	refs := map[string]string{"token": "secret:token"}
	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "account", "user-a", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Status: "active", SecretRefs: refs}); err != nil {
		t.Fatal(err)
	}
	provider.duringCall = func() {
		if _, err := management.TransitionSecret(t.Context(), "workspace-a", "token", "revoke", "user-a"); err != nil {
			t.Fatal(err)
		}
	}
	result, err := NewOperationsStore(db, dialect, delivery, nil, newTestDefinitionStore()).Call(t.Context(), integrationmodel.ProviderCallRequest{RequestID: "revoke-during-call", WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "account", Operation: "lookup", Payload: json.RawMessage(`{}`), ActorID: "user-a"})
	if !errors.Is(err, errProviderSecretChanged) || result.Invocation.Status != "failed" || provider.calls != 1 {
		t.Fatal("late provider result did not retain revocation", result, provider.calls, err)
	}
	secret, err := management.getSecret(t.Context(), "workspace-a", "token")
	if err != nil || secret.Status != "revoked" {
		t.Fatal("late refresh reactivated revoked secret", secret, err)
	}
	if _, err := resolver.ResolveSecretReferences(t.Context(), "workspace-a", refs); err == nil {
		t.Fatal("revoked secret resolved after late refresh")
	}
}

type concurrentRefreshProvider struct {
	mu      sync.Mutex
	calls   int
	entered chan int
	release [2]chan struct{}
}

func (*concurrentRefreshProvider) Descriptor() connector.ProviderDescriptor {
	return (&refreshResultProvider{}).Descriptor()
}
func (p *concurrentRefreshProvider) Call(_ context.Context, request connector.CallRequest) (connector.CallResult, error) {
	if request.Secrets["token"] != "original-token" {
		return connector.CallResult{}, errors.New("unexpected concurrent credential")
	}
	p.mu.Lock()
	index := p.calls
	p.calls++
	p.mu.Unlock()
	p.entered <- index
	<-p.release[index]
	value := "late-token"
	if index == 1 {
		value = "winner-token"
	}
	return connector.CallResult{Payload: json.RawMessage(`{}`), SecretUpdates: map[string]string{"token": value}}, nil
}

func TestConcurrentProviderRefreshCannotOverwriteNewerResult(t *testing.T) {
	db, dialect := webPushTestDatabase(t, "credential-refresh-concurrent")
	crypt := &refreshTestCipher{}
	resolver := NewSecretResolver(db, dialect, crypt)
	provider := &concurrentRefreshProvider{entered: make(chan int, 2), release: [2]chan struct{}{make(chan struct{}), make(chan struct{})}}
	delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider}, resolver)
	management := NewManagementStore(db, dialect, crypt, delivery)
	if _, err := management.UpsertSecret(t.Context(), "workspace-a", "token", "user-a", integrationsdk.SecretInput{Kind: "token", Value: "original-token"}); err != nil {
		t.Fatal(err)
	}
	refs := map[string]string{"token": "secret:token"}
	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "account", "user-a", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Status: "active", SecretRefs: refs}); err != nil {
		t.Fatal(err)
	}
	store := NewOperationsStore(db, dialect, delivery, nil, newTestDefinitionStore())
	type outcome struct {
		result integrationmodel.ProviderCallResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	call := func(id string) {
		result, err := store.Call(t.Context(), integrationmodel.ProviderCallRequest{RequestID: id, WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "account", Operation: "lookup", Payload: json.RawMessage(`{}`), ActorID: "user-a"})
		outcomes <- outcome{result: result, err: err}
	}
	go call("concurrent-first")
	if index := <-provider.entered; index != 0 {
		t.Fatal("first provider call order changed", index)
	}
	go call("concurrent-second")
	if index := <-provider.entered; index != 1 {
		t.Fatal("second provider call order changed", index)
	}
	close(provider.release[1])
	winner := <-outcomes
	if winner.err != nil || winner.result.Invocation.Status != "succeeded" {
		t.Fatal("newer refresh failed", winner)
	}
	close(provider.release[0])
	late := <-outcomes
	if !errors.Is(late.err, errProviderSecretChanged) || late.result.Invocation.Status != "failed" {
		t.Fatal("late refresh was not rejected", late)
	}
	values, err := resolver.ResolveSecretReferences(t.Context(), "workspace-a", refs)
	if err != nil || values["token"] != "winner-token" {
		t.Fatal("late refresh overwrote newer result", values, err)
	}
}
