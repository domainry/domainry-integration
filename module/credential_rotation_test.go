package module_test

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationmigration "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/migration"
	"github.com/domainry/domainry-integration/module"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	_ "modernc.org/sqlite"
)

var followupUnavailable = errors.New("synthetic business request unavailable after refresh")

// This exercises the trusted host SDK, not end-user authorization. Only public
// module/SDK contracts are available here; the provider is a protocol fixture.
func TestPublicModuleRetainsRotatedCredentialsAcrossRestart(t *testing.T) {
	for _, entry := range []string{"call", "delivery", "connection-test"} {
		t.Run(entry, func(t *testing.T) {
			path := t.TempDir() + "/host.db"
			var key [32]byte
			if _, err := rand.Read(key[:]); err != nil {
				t.Fatal(err)
			}
			open := func() (*rotationHost, integrationsdk.Binding) {
				t.Helper()
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				db.SetMaxOpenConns(1)
				t.Cleanup(func() { _ = db.Close() })
				d, err := ormdialect.New(ormdialect.SQLite)
				if err != nil {
					t.Fatal(err)
				}
				host := &rotationHost{db: db, dialect: d.WithSchema(""), provider: &rotationProvider{}, key: key}
				binding, err := module.NewFactory().OpenModule(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "rotation-runtime"}, host)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = binding.Close(context.Background()) })
				return host, binding
			}
			host, binding := open()
			management := binding.(integrationsdk.ManagementBinding).Management()
			if _, err := management.UpsertSecret(t.Context(), "workspace-a", "account-token", "user-a", integrationsdk.SecretInput{Kind: "token", Value: "original-credential"}); err != nil {
				t.Fatal(err)
			}
			connection := integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "rotation_probe", Status: "active", SecretRefs: map[string]string{"token": "secret:account-token"}}
			if _, err := management.UpsertConnection(t.Context(), "workspace-a", "account", "user-a", connection); err != nil {
				t.Fatal(err)
			}
			call := func(binding integrationsdk.Binding, id, workspace string) (any, error) {
				switch entry {
				case "delivery":
					return binding.Delivery().Accept(t.Context(), integrationsdk.DeliveryRequest{MessageID: id, DeduplicationKey: id, WorkspaceID: workspace, ConnectorKey: "crm", ConnectionKey: "account", Operation: "lookup", Payload: json.RawMessage(`{}`)})
				case "connection-test":
					return binding.(integrationsdk.ManagementBinding).Management().TestConnection(t.Context(), workspace, "account", integrationsdk.ConnectionTestRequest{})
				default:
					return binding.(integrationsdk.OperationsBinding).Operations().Call(t.Context(), integrationsdk.ProviderCallRequest{RequestID: id, WorkspaceID: workspace, ConnectorKey: "crm", ConnectionKey: "account", Operation: "lookup", Payload: json.RawMessage(`{}`), ActorID: "user-a"})
				}
			}
			result, err := call(binding, "before-restart", "workspace-a")
			if !errors.Is(err, followupUnavailable) || host.provider.calls != 1 {
				t.Fatal("first business failure was lost", err, host.provider.calls)
			}
			assertPublicRotationResult(t, result)
			var encrypted string
			if err := host.db.QueryRowContext(t.Context(), "SELECT ciphertext FROM _integration_secret_materials WHERE workspace_id=? AND secret_key=?", "workspace-a", "account-token").Scan(&encrypted); err != nil {
				t.Fatal(err)
			}
			plain, err := host.DecryptSecretMaterial(t.Context(), "workspace-a", "account-token", encrypted)
			if err != nil || plain != "rotated-credential" || strings.Contains(encrypted, "credential") {
				t.Fatal("completed rotation was not encrypted and stored", err)
			}
			if err := binding.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := host.db.Close(); err != nil {
				t.Fatal(err)
			}
			host, binding = open()
			result, err = call(binding, "after-restart", "workspace-a")
			if err != nil || host.provider.calls != 1 || host.provider.rotations != 0 {
				t.Fatal("restart did not reuse persisted credential", err, host.provider.calls, host.provider.rotations)
			}
			assertPublicRotationResult(t, result)
			if _, err := call(binding, "wrong-workspace", "workspace-b"); err == nil || host.provider.calls != 1 {
				t.Fatal("another workspace accessed the connection", err)
			}
			management = binding.(integrationsdk.ManagementBinding).Management()
			if _, err := management.TransitionSecret(t.Context(), "workspace-a", "account-token", "revoke", "user-a"); err != nil {
				t.Fatal(err)
			}
			if _, err := call(binding, "after-revoke", "workspace-a"); err == nil || host.provider.calls != 1 {
				t.Fatal("revoked credential reached provider", err)
			}
			var integrationMigrations, metadataMigrations int
			migrations, err := module.SchemaMigrations("sqlite", "")
			if err != nil {
				t.Fatal(err)
			}
			if err := host.db.QueryRowContext(t.Context(), "SELECT SUM(CASE WHEN owner='integration' THEN 1 ELSE 0 END), SUM(CASE WHEN owner='metadata' THEN 1 ELSE 0 END) FROM _schema_migrations").Scan(&integrationMigrations, &metadataMigrations); err != nil || integrationMigrations != len(migrations) || metadataMigrations == 0 {
				t.Fatal("host migration ledger was not reused", err, integrationMigrations, metadataMigrations)
			}
		})
	}
}

func assertPublicRotationResult(t *testing.T, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil || strings.Contains(string(raw), "original-credential") || strings.Contains(string(raw), "rotated-credential") || strings.Contains(string(raw), "secret_updates") {
		t.Fatal("credentials entered the public SDK response", err)
	}
}

type rotationProvider struct{ calls, rotations int }

func (*rotationProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "rotation_probe", ProviderRevision: "1.0.0", SecretFields: []connector.SecretField{{Key: "token", Name: "Token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationOAuthRefresh, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}, Operations: []connector.OperationDescriptor{{ConnectorKey: "crm", ProviderKey: "rotation_probe", Key: "lookup", ContractSHA256: strings.Repeat("e", 64), Mode: connector.ModeCall, Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}}}
}
func (p *rotationProvider) Call(_ context.Context, request connector.CallRequest) (connector.CallResult, error) {
	p.calls++
	switch request.Secrets["token"] {
	case "original-credential":
		p.rotations++
		return connector.CallResult{SecretUpdates: map[string]string{"token": "rotated-credential"}}, followupUnavailable
	case "rotated-credential":
		return connector.CallResult{Payload: json.RawMessage(`{"connected":true}`)}, nil
	default:
		return connector.CallResult{}, errors.New("unexpected credential")
	}
}
func (p *rotationProvider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.Call(ctx, connector.CallRequest{Secrets: request.Secrets})
	return connector.TestConnectionResult{Connected: err == nil, Details: result.Payload, SecretUpdates: result.SecretUpdates}, err
}

type rotationHost struct {
	db       *sql.DB
	dialect  modulehost.Dialect
	provider *rotationProvider
	key      [32]byte
}

func (h *rotationHost) Database() modulehost.Database                 { return h.db }
func (h *rotationHost) Dialect() modulehost.Dialect                   { return h.dialect }
func (h *rotationHost) Migrations() modulehost.MigrationRegistrar     { return h }
func (h *rotationHost) Providers() modulehost.ProviderRegistry        { return h }
func (h *rotationHost) SecretCipher() modulehost.SecretMaterialCipher { return h }
func (h *rotationHost) RuntimeTriggers() integrationsdk.TriggerSink   { return h }
func (*rotationHost) Driver() string                                  { return "sqlite" }
func (*rotationHost) Schema() string                                  { return "" }
func (h *rotationHost) ApplyOwnedMigrations(ctx context.Context, owner string, migrations []modulehost.SchemaMigration) error {
	if owner != "integration" && owner != "metadata" {
		return errors.New("unexpected migration owner")
	}
	return integrationmigration.ApplyOwnedMigrations(ctx, h.db, h.dialect, owner, migrations)
}
func (h *rotationHost) Provider(connectorKey, providerKey string) (connector.Adapter, bool) {
	return h.provider, connectorKey == "crm" && providerKey == "rotation_probe"
}
func (h *rotationHost) Descriptors() []connector.ProviderDescriptor {
	return []connector.ProviderDescriptor{h.provider.Descriptor()}
}
func (*rotationHost) Trigger(context.Context, integrationsdk.TriggerRequest) (integrationsdk.RuntimeExecutionReceipt, error) {
	return integrationsdk.RuntimeExecutionReceipt{}, errors.New("unexpected runtime trigger")
}
func (h *rotationHost) EncryptSecretMaterial(_ context.Context, workspace, key, plain string) (string, error) {
	block, _ := aes.NewCipher(h.key[:])
	gcm, _ := cipher.NewGCM(block)
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, []byte(plain), []byte(workspace+"\x00"+key))), nil
}
func (h *rotationHost) DecryptSecretMaterial(_ context.Context, workspace, key, encrypted string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", err
	}
	block, _ := aes.NewCipher(h.key[:])
	gcm, _ := cipher.NewGCM(block)
	if len(raw) < gcm.NonceSize() {
		return "", errors.New("invalid test ciphertext")
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], []byte(workspace+"\x00"+key))
	return string(plain), err
}
