package saas_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-integration-sdk/remote"
	"github.com/domainry/domainry-integration-sdk/saashost"
	saasassembly "github.com/domainry/domainry-integration/internal/assembly/saas"
	integrationmodule "github.com/domainry/domainry-integration/module"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	_ "modernc.org/sqlite"
)

const (
	flowWorkspace = "workspace-flow"
	flowConnector = "crm"
	flowProvider  = "contract_probe"
	flowOperation = "deliver"
)

type publicFlowOutcome struct {
	ProviderProjected    bool
	RequirementStatus    string
	FailureStatus        integrationsdk.DeliveryStatus
	FailureErrorCode     string
	SuccessStatus        integrationsdk.DeliveryStatus
	SuccessResultRef     string
	InvocationCount      int
	ProviderCalls        int
	ResolvedSecret       string
	PersistedCiphertext  string
	RollbackMetadataRows int
	RollbackMaterialRows int
	WrongActionStatus    int
	WrongActionCode      string
	WrongWorkspaceStatus int
	WrongWorkspaceCode   string
	AllowedStatus        int
}

func TestPublicBindingsPreserveModuleAndSaaSBusinessSemantics(t *testing.T) {
	moduleOutcome := runPublicFlow(t, openModuleFlow(t))
	saasOutcome := runPublicFlow(t, openSaaSFlow(t))
	if !reflect.DeepEqual(moduleOutcome, saasOutcome) {
		t.Fatalf("Module/SaaS public Binding semantics differ:\nmodule=%#v\nsaas=%#v", moduleOutcome, saasOutcome)
	}
}

type publicFlowTopology struct {
	binding  integrationsdk.Binding
	database *sql.DB
	provider *publicFlowProvider
	close    func()
}

func openModuleFlow(t *testing.T) publicFlowTopology {
	t.Helper()
	host := newPublicFlowHost(t, "module")
	binding, err := integrationmodule.NewFactory().OpenModule(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "runtime-flow"}, host)
	if err != nil {
		host.close()
		t.Fatal(err)
	}
	return publicFlowTopology{binding: binding, database: host.database, provider: host.provider, close: func() {
		_ = binding.Close(context.Background())
		host.close()
	}}
}

func openSaaSFlow(t *testing.T) publicFlowTopology {
	t.Helper()
	host := newPublicFlowHost(t, "saas")
	service, err := saasassembly.Open(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "runtime-flow"}, host, "service-token")
	if err != nil {
		host.close()
		t.Fatal(err)
	}
	server := httptest.NewServer(service.Handler)
	remoteFactory := integrationmodule.NewSaaSFactory(remote.NewFactory(remote.Options{
		BaseURL: server.URL, Token: "service-token", HTTPClient: server.Client(),
	}))
	factory, ok := remoteFactory.(saashost.Factory)
	if !ok {
		server.Close()
		_ = service.Close(context.Background())
		host.close()
		t.Fatal("public SaaS factory does not implement the SDK SaaS factory contract")
	}
	binding, err := factory.OpenSaaS(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "runtime-flow"}, nil)
	if err != nil {
		server.Close()
		_ = service.Close(context.Background())
		host.close()
		t.Fatal(err)
	}
	return publicFlowTopology{binding: binding, database: host.database, provider: host.provider, close: func() {
		_ = binding.Close(context.Background())
		server.Close()
		_ = service.Close(context.Background())
		host.close()
	}}
}

func runPublicFlow(t *testing.T, topology publicFlowTopology) publicFlowOutcome {
	t.Helper()
	defer topology.close()
	if err := topology.binding.Descriptor().Validate(); err != nil {
		t.Fatal(err)
	}

	definitions, err := topology.binding.Catalog().ListConnectorDefinitions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	providerProjected := catalogContainsProvider(t, definitions, flowConnector, flowProvider)
	if !providerProjected {
		t.Fatalf("registered Provider %s/%s is absent from the source-owned catalog projection", flowConnector, flowProvider)
	}

	requirement := integrationsdk.ConnectionRequirement{
		Key: "primary", WorkspaceID: flowWorkspace, ConnectorKey: flowConnector, ProviderKey: flowProvider,
		Name: "Flow", Config: json.RawMessage(`{"endpoint":"https://provider.example"}`),
	}
	if err := topology.binding.Requirements().SynchronizeConnections(t.Context(), []integrationsdk.ConnectionRequirement{requirement}); err != nil {
		t.Fatal(err)
	}
	managementBinding, ok := topology.binding.(integrationsdk.ManagementBinding)
	if !ok || managementBinding.Management() == nil {
		t.Fatal("public Integration Binding does not expose Management")
	}
	management := managementBinding.Management()
	manifestConnection, err := management.GetConnection(t.Context(), flowWorkspace, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if manifestConnection.Status != "configured" {
		t.Fatalf("connection requirement status=%q, want configured until the required secret reference is managed", manifestConnection.Status)
	}
	secret, err := management.UpsertSecret(t.Context(), flowWorkspace, "delivery-token", "admin-flow", integrationsdk.SecretInput{Kind: "bearer_token", Value: "plain-token"})
	if err != nil || !secret.Configured {
		t.Fatalf("secret=%#v err=%v", secret, err)
	}
	connection, err := management.UpsertConnection(t.Context(), flowWorkspace, "primary", "admin-flow", integrationsdk.ConnectionInput{
		ConnectorKey: flowConnector, ProviderKey: flowProvider, Name: "Flow", Status: "active",
		Config: map[string]any{"endpoint": "https://provider.example"}, SecretRefs: map[string]string{"token": "secret:delivery-token"},
	})
	if err != nil || connection.Status != "active" || connection.SecretRefs["token"] != "secret:delivery-token" {
		t.Fatalf("managed connection=%#v err=%v", connection, err)
	}

	request := integrationsdk.DeliveryRequest{
		MessageID: "flow-message", DeduplicationKey: "flow-order:1", WorkspaceID: flowWorkspace,
		ConnectorKey: flowConnector, ConnectionKey: "primary", Operation: flowOperation,
		Payload: json.RawMessage(`{"record_id":"record-1"}`),
	}
	if _, err := topology.binding.Delivery().Accept(t.Context(), request); err == nil {
		t.Fatal("transient Provider failure was not returned through the public Binding")
	}
	failed, err := topology.binding.Delivery().Query(t.Context(), request.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != integrationsdk.DeliveryStatusFailed || failed.ErrorCode != "provider_call_failed" {
		t.Fatalf("failed delivery evidence=%#v", failed)
	}
	succeeded, err := topology.binding.Delivery().Accept(t.Context(), request)
	if err != nil || succeeded.Status != integrationsdk.DeliveryStatusSucceeded || succeeded.ResultRef != "provider-receipt-2" {
		t.Fatalf("retried delivery=%#v err=%v", succeeded, err)
	}
	deduplicated, err := topology.binding.Delivery().Accept(t.Context(), request)
	if err != nil || !reflect.DeepEqual(succeeded, deduplicated) {
		t.Fatalf("deduplicated delivery=%#v want=%#v err=%v", deduplicated, succeeded, err)
	}

	providerCalls, resolvedSecret := topology.provider.snapshot()
	if providerCalls != 2 || resolvedSecret != "plain-token" {
		t.Fatalf("Provider calls=%d resolved_secret=%q", providerCalls, resolvedSecret)
	}
	var invocationCount int
	if err := topology.database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _integration_invocations WHERE workspace_id = ? AND request_ref = ?`, flowWorkspace, request.MessageID).Scan(&invocationCount); err != nil {
		t.Fatal(err)
	}
	if invocationCount != 1 {
		t.Fatalf("source-owned invocation rows=%d, want one durable idempotency record", invocationCount)
	}
	var ciphertext string
	if err := topology.database.QueryRowContext(t.Context(), `SELECT ciphertext FROM _integration_secret_materials WHERE workspace_id = ? AND secret_key = ?`, flowWorkspace, "delivery-token").Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if ciphertext != "encrypted:plain-token" {
		t.Fatalf("persisted ciphertext=%q", ciphertext)
	}

	if _, err := topology.database.ExecContext(t.Context(), `CREATE TRIGGER reject_flow_secret BEFORE INSERT ON _integration_secrets WHEN NEW.secret_key = 'rollback-token' BEGIN SELECT RAISE(ABORT, 'forced metadata failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := management.UpsertSecret(t.Context(), flowWorkspace, "rollback-token", "admin-flow", integrationsdk.SecretInput{Kind: "bearer_token", Value: "must-rollback"}); err == nil {
		t.Fatal("forced secret metadata failure was not returned")
	}
	metadataRows := countFlowRows(t, topology.database, "_integration_secrets", "rollback-token")
	materialRows := countFlowRows(t, topology.database, "_integration_secret_materials", "rollback-token")
	if metadataRows != 0 || materialRows != 0 {
		t.Fatalf("secret transaction partially committed metadata=%d material=%d", metadataRows, materialRows)
	}

	wrongActionStatus, wrongActionCode := requestConnection(t, topology.binding, flowWorkspace, integrationsdk.ActionIntegrationConnectionsList)
	wrongWorkspaceStatus, wrongWorkspaceCode := requestConnection(t, topology.binding, "workspace-other", integrationsdk.ActionIntegrationConnectionsGet)
	allowedStatus, _ := requestConnection(t, topology.binding, flowWorkspace, integrationsdk.ActionIntegrationConnectionsGet)
	if wrongActionStatus != http.StatusForbidden || wrongActionCode != "backend.integration.permission_denied" {
		t.Fatalf("wrong-action authorization status=%d code=%q", wrongActionStatus, wrongActionCode)
	}
	if wrongWorkspaceStatus != http.StatusBadRequest || wrongWorkspaceCode != "backend.integration.management_failed" {
		t.Fatalf("cross-workspace read status=%d code=%q", wrongWorkspaceStatus, wrongWorkspaceCode)
	}
	if allowedStatus != http.StatusOK {
		t.Fatalf("exact workspace/action authorization status=%d", allowedStatus)
	}

	return publicFlowOutcome{
		ProviderProjected: providerProjected,
		RequirementStatus: manifestConnection.Status, FailureStatus: failed.Status, FailureErrorCode: failed.ErrorCode,
		SuccessStatus: succeeded.Status, SuccessResultRef: succeeded.ResultRef, InvocationCount: invocationCount,
		ProviderCalls: providerCalls, ResolvedSecret: resolvedSecret, PersistedCiphertext: ciphertext,
		RollbackMetadataRows: metadataRows, RollbackMaterialRows: materialRows,
		WrongActionStatus: wrongActionStatus, WrongActionCode: wrongActionCode,
		WrongWorkspaceStatus: wrongWorkspaceStatus, WrongWorkspaceCode: wrongWorkspaceCode, AllowedStatus: allowedStatus,
	}
}

func catalogContainsProvider(t *testing.T, definitions []integrationsdk.ConnectorDefinition, connectorKey, providerKey string) bool {
	t.Helper()
	for _, definition := range definitions {
		if definition.Key != connectorKey {
			continue
		}
		var projection struct {
			Providers []struct {
				Key string `json:"key"`
			} `json:"providers"`
		}
		if err := json.Unmarshal(definition.Definition, &projection); err != nil {
			t.Fatal(err)
		}
		for _, provider := range projection.Providers {
			if provider.Key == providerKey {
				return true
			}
		}
	}
	return false
}

func countFlowRows(t *testing.T, database *sql.DB, table, key string) int {
	t.Helper()
	if table != "_integration_secrets" && table != "_integration_secret_materials" {
		t.Fatalf("unsupported flow table %q", table)
	}
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE workspace_id = ? AND secret_key = ?", table)
	if err := database.QueryRowContext(t.Context(), query, flowWorkspace, key).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func requestConnection(t *testing.T, binding integrationsdk.Binding, workspaceID, permissionKey string) (int, string) {
	t.Helper()
	provider, ok := binding.(modulehttp.Provider)
	if !ok || len(provider.HTTPAdapters()) != 1 {
		t.Fatal("public Integration Binding has no product HTTP adapter")
	}
	request := httptest.NewRequest(http.MethodGet, "/integration/connections/primary", nil)
	request = request.WithContext(identitysdk.WithRequestIdentity(request.Context(), identitysdk.RequestIdentity{Principal: publicFlowPrincipal(workspaceID, permissionKey)}))
	recorder := httptest.NewRecorder()
	provider.HTTPAdapters()[0].Handler().ServeHTTP(recorder, request)
	var body struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &body)
	return recorder.Code, body.Code
}

func publicFlowPrincipal(workspaceID, permissionKey string) identitysdk.Principal {
	separator := strings.LastIndex(permissionKey, ".")
	resource, action := permissionKey[:separator], permissionKey[separator+1:]
	bundle := &identitysdk.AccessBundle{
		ContractVersion:       identitysdk.CurrentPolicyBundleVersion,
		AuthorizationRevision: "flow-revision", ExpiresAt: time.Now().UTC().Add(time.Hour),
		Subject:        identitysdk.Subject{WorkspaceID: identitysdk.WorkspaceID(workspaceID), SubjectID: "admin-flow"},
		FunctionGrants: []identitysdk.FunctionGrant{{Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow}},
		DataPolicies: []identitysdk.DataPolicy{{
			Key: permissionKey, Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow,
			DataScopes: []identitysdk.DataScope{identitysdk.DataScopeAll},
		}},
	}
	return identitysdk.Principal{Known: true, WorkspaceID: workspaceID, UserID: "admin-flow", AccessBundle: bundle}
}

type publicFlowHost struct {
	database  *sql.DB
	dialect   modulehost.Dialect
	registrar publicFlowRegistrar
	provider  *publicFlowProvider
}

func newPublicFlowHost(t *testing.T, topology string) *publicFlowHost {
	t.Helper()
	database, err := sql.Open("sqlite", t.TempDir()+"/integration-"+topology+".db")
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	rawDialect, err := ormdialect.New(ormdialect.SQLite)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	host := &publicFlowHost{database: database, dialect: rawDialect.WithSchema(""), provider: &publicFlowProvider{}}
	host.registrar = publicFlowRegistrar{database: database}
	return host
}

func (h *publicFlowHost) Database() modulehost.Database             { return h.database }
func (h *publicFlowHost) Dialect() modulehost.Dialect               { return h.dialect }
func (h *publicFlowHost) Migrations() modulehost.MigrationRegistrar { return h.registrar }
func (h *publicFlowHost) Providers() modulehost.ProviderRegistry {
	return publicFlowProviders{provider: h.provider}
}
func (*publicFlowHost) SecretCipher() modulehost.SecretMaterialCipher { return publicFlowCipher{} }
func (*publicFlowHost) RuntimeTriggers() integrationsdk.TriggerSink   { return publicFlowTrigger{} }
func (h *publicFlowHost) close()                                      { _ = h.database.Close() }

type publicFlowRegistrar struct{ database *sql.DB }

func (publicFlowRegistrar) Driver() string { return "sqlite" }
func (publicFlowRegistrar) Schema() string { return "" }
func (r publicFlowRegistrar) ApplyOwnedMigrations(ctx context.Context, owner string, migrations []modulehost.SchemaMigration) error {
	if owner != "integration" && owner != "metadata" {
		return fmt.Errorf("unexpected migration owner %q", owner)
	}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := r.database.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	}
	return nil
}

type publicFlowProviders struct{ provider *publicFlowProvider }

func (p publicFlowProviders) Provider(connectorKey, providerKey string) (connector.Adapter, bool) {
	return p.provider, connectorKey == flowConnector && providerKey == flowProvider
}
func (p publicFlowProviders) Descriptors() []connector.ProviderDescriptor {
	return []connector.ProviderDescriptor{p.provider.Descriptor()}
}

type publicFlowProvider struct {
	mu             sync.Mutex
	calls          int
	resolvedSecret string
}

func (*publicFlowProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{
		ConnectorKey: flowConnector, ProviderKey: flowProvider, ProviderRevision: "1.0.0",
		ConfigFields: []connector.ConfigField{{Key: "endpoint", Name: "Endpoint", Type: connector.ConfigFieldText, Required: true}},
		SecretFields: []connector.SecretField{{
			Key: "token", Name: "Token", Required: true, CredentialKind: connector.SecretCredentialBearerToken,
			MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual,
			ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound,
		}},
		Operations: []connector.OperationDescriptor{{
			ConnectorKey: flowConnector, ProviderKey: flowProvider, Key: flowOperation, Mode: connector.ModeEnqueue,
			ContractSHA256: strings.Repeat("d", 64),
			Reliability: connector.ReliabilityContract{
				Effect:         connector.EffectWrite,
				Idempotency:    connector.IdempotencyContract{Strategy: connector.IdempotencyProviderKey, KeyRetentionSeconds: 86400},
				Reconciliation: connector.ReconciliationNone,
				Compensation:   connector.CompensationContract{Mode: connector.CompensationNone},
			},
		}},
	}
}

func (p *publicFlowProvider) Call(_ context.Context, request connector.CallRequest) (connector.CallResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.resolvedSecret = request.Secrets["token"]
	if request.Connection.SecretRefs["token"] != "secret:delivery-token" || request.Connection.Config["endpoint"] != "https://provider.example" {
		return connector.CallResult{}, errors.New("connection was not normalized by Integration")
	}
	if p.calls == 1 {
		return connector.CallResult{ResponseRef: "provider-attempt-1"}, errors.New("transient Provider failure")
	}
	return connector.CallResult{ResponseRef: "provider-receipt-2"}, nil
}

func (p *publicFlowProvider) snapshot() (int, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, p.resolvedSecret
}

type publicFlowCipher struct{}

func (publicFlowCipher) EncryptSecretMaterial(_ context.Context, _, _ string, plaintext string) (string, error) {
	return "encrypted:" + plaintext, nil
}
func (publicFlowCipher) DecryptSecretMaterial(_ context.Context, _, _ string, ciphertext string) (string, error) {
	if !strings.HasPrefix(ciphertext, "encrypted:") {
		return "", errors.New("ciphertext is invalid")
	}
	return strings.TrimPrefix(ciphertext, "encrypted:"), nil
}

type publicFlowTrigger struct{}

func (publicFlowTrigger) Trigger(_ context.Context, request integrationsdk.TriggerRequest) (integrationsdk.RuntimeExecutionReceipt, error) {
	return integrationsdk.RuntimeExecutionReceipt{EventID: request.EventID, MappingKey: request.MappingKey, TargetType: request.Target.Type, Status: "succeeded"}, nil
}
