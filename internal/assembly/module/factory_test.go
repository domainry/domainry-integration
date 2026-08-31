package moduleassembly

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	_ "modernc.org/sqlite"
)

type testHost struct {
	database  *sql.DB
	dialect   modulehost.Dialect
	registrar *testRegistrar
}

func (h *testHost) Database() modulehost.Database               { return h.database }
func (h *testHost) Dialect() modulehost.Dialect                 { return h.dialect }
func (h *testHost) Migrations() modulehost.MigrationRegistrar   { return h.registrar }
func (*testHost) Providers() modulehost.ProviderRegistry        { return testProviders{} }
func (*testHost) SecretCipher() modulehost.SecretMaterialCipher { return testCipher{} }

type testRegistrar struct {
	database *sql.DB
	owners   []string
}

func (*testRegistrar) Driver() string { return "sqlite" }
func (*testRegistrar) Schema() string { return "" }
func (r *testRegistrar) ApplyOwnedMigrations(ctx context.Context, owner string, migrations []modulehost.SchemaMigration) error {
	r.owners = append(r.owners, owner)
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := r.database.ExecContext(ctx, statement); err != nil {
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

func openTestHost(t *testing.T) *testHost {
	t.Helper()
	database, err := sql.Open("sqlite", t.TempDir()+"/integration.db")
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = database.Close() })
	dialect, err := ormdialect.New(ormdialect.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	registrar := &testRegistrar{database: database}
	return &testHost{database: database, dialect: dialect.WithSchema(""), registrar: registrar}
}

func TestFactoryAssemblesDeploymentNeutralModuleBinding(t *testing.T) {
	host := openTestHost(t)
	binding, err := NewFactory(Options{}).OpenModule(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "runtime-a"}, host)
	if err != nil {
		t.Fatal(err)
	}
	defer binding.Close(t.Context())
	if err := binding.Descriptor().Validate(); err != nil {
		t.Fatal(err)
	}
	if binding.Descriptor().Mode != integrationsdk.DeploymentModeModule {
		t.Fatalf("mode=%q", binding.Descriptor().Mode)
	}
	if len(host.registrar.owners) != 1 || host.registrar.owners[0] != "integration" {
		t.Fatalf("migration owners=%v", host.registrar.owners)
	}
	if values, err := binding.Catalog().ListConnectorDefinitions(t.Context()); err != nil || len(values) != 0 {
		t.Fatalf("catalog=%v err=%v", values, err)
	}
	if err := binding.Requirements().SynchronizeConnections(t.Context(), []integrationsdk.ConnectionRequirement{{Key: "primary", WorkspaceID: "workspace-a", ConnectorKey: "crm", ProviderKey: "probe", Config: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	webPush, ok := binding.(integrationsdk.WebPushBinding)
	if !ok {
		t.Fatal("binding does not expose Integration-owned Web Push port")
	}
	readiness, err := webPush.WebPushSubscriptions().Readiness(t.Context(), "workspace-a")
	if err != nil || readiness.Status != "unconfigured" {
		t.Fatalf("readiness=%#v err=%v", readiness, err)
	}
}

func TestFactoryRejectsIncompleteHost(t *testing.T) {
	if _, err := NewFactory().OpenModule(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "runtime-a"}, nil); err == nil {
		t.Fatal("incomplete host accepted")
	}
}
