package moduleassembly

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	connectorscatalog "github.com/domainry/domainry-connectors/catalog"
	shareddefinition "github.com/domainry/domainry-foundation/definition"
	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
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
func (*testHost) RuntimeTriggers() integrationsdk.TriggerSink   { return testTrigger{} }

type testTrigger struct{}

func (testTrigger) Trigger(context.Context, integrationsdk.TriggerRequest) (integrationsdk.RuntimeExecutionReceipt, error) {
	return integrationsdk.RuntimeExecutionReceipt{Status: "succeeded"}, nil
}

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
	return connector.CallResult{Payload: json.RawMessage(`{}`)}, nil
}

type testCipher struct{}

func (testCipher) EncryptSecretMaterial(_ context.Context, _, _ string, plaintext string) (string, error) {
	return plaintext, nil
}
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
	binding, err := NewFactory(Options{ConnectorCatalog: connectorscatalog.Definitions}).OpenModule(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "runtime-a"}, host)
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
	if len(host.registrar.owners) != 2 || host.registrar.owners[0] != "integration" || host.registrar.owners[1] != shareddefinition.MigrationOwner {
		t.Fatalf("migration owners=%v", host.registrar.owners)
	}
	provider, ok := binding.(modulehttp.Provider)
	if !ok || len(provider.HTTPAdapters()) != 1 {
		t.Fatalf("Module HTTP adapters=%v", provider)
	}
	if err := modulehttp.ValidateAdapter(provider.HTTPAdapters()[0]); err != nil {
		t.Fatal(err)
	}
	routes := provider.HTTPAdapters()[0].Routes()
	if len(routes) != 51 {
		t.Fatalf("Integration product routes=%d", len(routes))
	}
	for _, required := range []string{"GET /integration/connectors", "PUT /integration/connections/{connectionKey}", "GET /integration/connection-accounts", "GET /integration/web-push/readiness"} {
		found := false
		for _, route := range routes {
			if route.Pattern() == required {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Integration product route %q is missing", required)
		}
	}
	if values, err := binding.Catalog().ListConnectorDefinitions(t.Context()); err != nil || len(values) < 50 {
		t.Fatalf("built-in catalog count=%d err=%v", len(values), err)
	}
	if err := binding.Requirements().SynchronizeConnections(t.Context(), []integrationsdk.ConnectionRequirement{{Key: "primary", WorkspaceID: "workspace-a", ConnectorKey: "crm", ProviderKey: "probe", Config: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	management, ok := binding.(integrationsdk.ManagementBinding)
	if !ok || management.Management() == nil {
		t.Fatal("binding does not expose Integration-owned Management port")
	}
	connection, err := management.Management().GetConnection(t.Context(), "workspace-a", "primary")
	if err != nil || connection.ConnectorKey != "crm" {
		t.Fatalf("connection=%#v err=%v", connection, err)
	}
	accountAdmin, ok := binding.(integrationsdk.ConnectionAccountAdministrationBinding)
	if !ok || accountAdmin.ConnectionAccountAdministration() == nil {
		t.Fatal("binding does not expose Integration connection account administration")
	}
	account, err := accountAdmin.ConnectionAccountAdministration().RegisterConnectionAccount(t.Context(), "workspace-a", "primary", "admin", integrationsdk.ConnectionAccountRegistration{Scope: integrationsdk.ConnectionAccountScopeWorkspace})
	if err != nil || account.Scope != integrationsdk.ConnectionAccountScopeWorkspace {
		t.Fatalf("account=%#v err=%v", account, err)
	}
	accounts, ok := binding.(integrationsdk.ConnectionAccountsBinding)
	if !ok || accounts.ConnectionAccounts() == nil {
		t.Fatal("binding does not expose Integration current-user connection accounts")
	}
	listed, err := accounts.ConnectionAccounts().ListConnectionAccounts(t.Context(), integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: integrationsdk.ConnectionAccountAccess{Personal: true, Workspace: true}})
	if err != nil || len(listed) != 1 || listed[0].Key != "primary" {
		t.Fatalf("listed accounts=%#v err=%v", listed, err)
	}
	payload, _ := json.Marshal(integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Name: "Updated"})
	request := httptest.NewRequest(http.MethodPut, "/integration/connections/primary", bytes.NewReader(payload))
	bundle := &identitysdk.AccessBundle{
		ContractVersion: identitysdk.CurrentPolicyBundleVersion, AuthorizationRevision: "revision-1", ExpiresAt: time.Now().UTC().Add(time.Hour),
		Subject:        identitysdk.Subject{WorkspaceID: "workspace-a", SubjectID: "admin"},
		FunctionGrants: []identitysdk.FunctionGrant{{Resource: "integration.connections", Action: "upsert", Effect: identitysdk.EffectAllow}},
		DataPolicies:   []identitysdk.DataPolicy{{Key: integrationsdk.ActionIntegrationConnectionsUpsert, Resource: "integration.connections", Action: "upsert", Effect: identitysdk.EffectAllow, DataScopes: []identitysdk.DataScope{identitysdk.DataScopeAll}}},
	}
	request = request.WithContext(identitysdk.WithRequestIdentity(request.Context(), identitysdk.RequestIdentity{Principal: identitysdk.Principal{Known: true, WorkspaceID: "workspace-a", UserID: "admin", AccessBundle: bundle}}))
	response := httptest.NewRecorder()
	provider.HTTPAdapters()[0].Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("management adapter status=%d body=%s", response.Code, response.Body.String())
	}
	connection, err = management.Management().GetConnection(t.Context(), "workspace-a", "primary")
	if err != nil || connection.Name != "Updated" {
		t.Fatalf("updated connection=%#v err=%v", connection, err)
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
