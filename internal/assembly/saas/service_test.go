package saas

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-foundation/modulehttp"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-integration-sdk/remote"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	_ "modernc.org/sqlite"
)

type testHost struct {
	database *sql.DB
	dialect  modulehost.Dialect
}

func (h testHost) Database() modulehost.Database               { return h.database }
func (h testHost) Dialect() modulehost.Dialect                 { return h.dialect }
func (h testHost) Migrations() modulehost.MigrationRegistrar   { return testRegistrar{h} }
func (testHost) Providers() modulehost.ProviderRegistry        { return testProviders{} }
func (testHost) SecretCipher() modulehost.SecretMaterialCipher { return testCipher{} }
func (testHost) RuntimeTriggers() integrationsdk.TriggerSink   { return testTrigger{} }

type testTrigger struct{}

func (testTrigger) Trigger(context.Context, integrationsdk.TriggerRequest) (integrationsdk.RuntimeExecutionReceipt, error) {
	return integrationsdk.RuntimeExecutionReceipt{Status: "succeeded"}, nil
}

type testRegistrar struct{ host testHost }

func (testRegistrar) Driver() string { return "sqlite" }
func (testRegistrar) Schema() string { return "" }
func (r testRegistrar) ApplyOwnedMigrations(ctx context.Context, _ string, migrations []modulehost.SchemaMigration) error {
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := r.host.database.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	}
	return nil
}

type testProviders struct{}

func (testProviders) Provider(connectorKey, providerKey string) (connector.Adapter, bool) {
	provider := testProvider{}
	return provider, connectorKey == "crm" && providerKey == "probe"
}
func (testProviders) Descriptors() []connector.ProviderDescriptor {
	return []connector.ProviderDescriptor{testProvider{}.Descriptor()}
}

type testProvider struct{}

func (testProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "probe", ProviderRevision: "1.0.0", Operations: []connector.OperationDescriptor{{
		ConnectorKey: "crm", ProviderKey: "probe", Key: "lookup", Mode: connector.ModeCall, ContractSHA256: strings.Repeat("a", 64),
		Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}},
	}}}
}
func (testProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{Payload: []byte(`{}`)}, nil
}

type testCipher struct{}

func (testCipher) EncryptSecretMaterial(_ context.Context, _, _ string, plaintext string) (string, error) {
	return plaintext, nil
}
func (testCipher) DecryptSecretMaterial(context.Context, string, string, string) (string, error) {
	return "", errors.New("not configured")
}

func TestServiceMatchesIntegrationSDKRemoteContract(t *testing.T) {
	database, err := sql.Open("sqlite", t.TempDir()+"/integration.db")
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	defer database.Close()
	dialect, _ := ormdialect.New(ormdialect.SQLite)
	service, err := Open(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "runtime-a"}, testHost{database: database, dialect: dialect.WithSchema("")}, "service-token")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(t.Context())
	server := httptest.NewServer(service.Handler)
	defer server.Close()
	binding, err := NewFactory(remote.NewFactory(remote.Options{BaseURL: server.URL, Token: "service-token", HTTPClient: server.Client()})).OpenSaaS(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "runtime-a"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if binding.Descriptor().Mode != integrationsdk.DeploymentModeSaaS {
		t.Fatalf("mode=%q", binding.Descriptor().Mode)
	}
	provider, ok := binding.(modulehttp.Provider)
	if !ok || len(provider.HTTPSurfaces()) != 1 || len(provider.HTTPSurfaces()[0].Routes()) != 38 {
		t.Fatalf("SaaS Integration HTTP surfaces=%v", provider)
	}
	if values, err := binding.Catalog().ListConnectorDefinitions(t.Context()); err != nil || len(values) < 50 {
		t.Fatalf("built-in catalog count=%d err=%v", len(values), err)
	}
	if err := binding.Requirements().SynchronizeConnections(t.Context(), []integrationsdk.ConnectionRequirement{{Key: "primary", WorkspaceID: "workspace-a", ConnectorKey: "crm", ProviderKey: "probe", Config: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	management, ok := binding.(integrationsdk.ManagementBinding)
	if !ok || management.Management() == nil {
		t.Fatal("SaaS binding does not expose Integration Management")
	}
	connection, err := management.Management().GetConnection(t.Context(), "workspace-a", "primary")
	if err != nil || connection.ConnectorKey != "crm" {
		t.Fatalf("remote connection=%#v err=%v", connection, err)
	}
	connection, err = management.Management().UpsertConnection(t.Context(), "workspace-a", "primary", "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Name: "Remote"})
	if err != nil || connection.Name != "Remote" {
		t.Fatalf("remote updated connection=%#v err=%v", connection, err)
	}
	readiness, err := binding.(integrationsdk.WebPushBinding).WebPushSubscriptions().Readiness(t.Context(), "workspace-a")
	if err != nil || readiness.Status != "unconfigured" {
		t.Fatalf("readiness=%#v err=%v", readiness, err)
	}
}
