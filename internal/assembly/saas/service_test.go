package saas

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
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

func (testProviders) Provider(string, string) (connector.Adapter, bool) { return nil, false }
func (testProviders) Descriptors() []connector.ProviderDescriptor       { return nil }

type testCipher struct{}

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
	if !ok || len(provider.HTTPSurfaces()) != 1 || len(provider.HTTPSurfaces()[0].Routes()) != 5 {
		t.Fatalf("SaaS Integration HTTP surfaces=%v", provider)
	}
	if values, err := binding.Catalog().ListConnectorDefinitions(t.Context()); err != nil || len(values) != 0 {
		t.Fatalf("catalog=%v err=%v", values, err)
	}
	if err := binding.Requirements().SynchronizeConnections(t.Context(), []integrationsdk.ConnectionRequirement{{Key: "primary", WorkspaceID: "workspace-a", ConnectorKey: "crm", ProviderKey: "probe", Config: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	readiness, err := binding.(integrationsdk.WebPushBinding).WebPushSubscriptions().Readiness(t.Context(), "workspace-a")
	if err != nil || readiness.Status != "unconfigured" {
		t.Fatalf("readiness=%#v err=%v", readiness, err)
	}
}
