package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	"github.com/domainry/domainry-orm/query"
	_ "modernc.org/sqlite"
)

type operationsTestProvider struct{ calls int }

func (*operationsTestProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{
		ConnectorKey: "crm", ProviderKey: "probe",
		Operations: []connector.OperationDescriptor{{ConnectorKey: "crm", ProviderKey: "probe", Key: "lookup", Mode: connector.ModeCall, ContractSHA256: strings.Repeat("c", 64)}},
	}
}

func (p *operationsTestProvider) Call(_ context.Context, request connector.CallRequest) (connector.CallResult, error) {
	p.calls++
	return connector.CallResult{Payload: json.RawMessage(`{"found":true}`), ResponseRef: "response-1"}, nil
}

func (*operationsTestProvider) VerifyWebhook(_ context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	return connector.VerifiedWebhook{
		EventType: "contact.changed", ExternalID: "external-event-1",
		Payload:  json.RawMessage(`{"contact":{"id":"contact-1","name":"Ada"},"command":"contact.sync"}`),
		Security: &connector.WebhookSecurityEvidence{SignatureVerified: request.Secrets["token"] == "plain-token"},
	}, nil
}

type operationsTriggerProbe struct {
	calls   int
	request integrationsdk.TriggerRequest
}

type backgroundOperationsTestProvider struct{ calls int }

func (*backgroundOperationsTestProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "background", ProviderRevision: "1.0.0", Operations: []connector.OperationDescriptor{{
		ConnectorKey: "crm", ProviderKey: "background", Key: "ack", Mode: connector.ModeEnqueue, ContractSHA256: strings.Repeat("e", 64),
		Reliability: connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}},
	}}}
}
func (*backgroundOperationsTestProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, nil
}
func (*backgroundOperationsTestProvider) BackgroundTasks(connector.Connection) []connector.BackgroundTaskDescriptor {
	return []connector.BackgroundTaskDescriptor{{Key: "poll", StateVersion: 1}}
}
func (p *backgroundOperationsTestProvider) ProcessBackground(context.Context, connector.BackgroundRequest) (connector.BackgroundResult, error) {
	p.calls++
	return connector.BackgroundResult{
		State: json.RawMessage(`{"cursor":"next"}`), NextDueAt: time.Now().UTC().Add(time.Hour),
		Events: []connector.BackgroundEvent{{ExternalID: "polled-1", EventType: "contact.changed", Payload: json.RawMessage(`{"contact":{"id":"contact-2"}}`)}},
	}, nil
}

func (p *operationsTriggerProbe) Trigger(_ context.Context, request integrationsdk.TriggerRequest) (integrationsdk.RuntimeExecutionReceipt, error) {
	p.calls++
	p.request = request
	return integrationsdk.RuntimeExecutionReceipt{
		EventID: request.EventID, MappingKey: request.MappingKey, ExecutionID: "action-1",
		TargetType: request.Target.Type, Status: "succeeded", CompletedAt: "2026-08-31T00:00:00Z",
	}, nil
}

func TestOperationsStoreOwnsCallWebhookMappingAndRuntimeReceipt(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-operations?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	rawDialect, _ := ormdialect.New(ormdialect.SQLite)
	dialect := rawDialect.WithSchema("")
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := database.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	insert, args, err := query.NewInsertBuilder(dialect, "_integration_connections").
		Columns("id", "connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").
		Values("connection-1", "primary", "workspace-a", "crm", "probe", "Primary", "active", `{}`, `{"token":"secret:token"}`, "admin", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z").Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), insert, args...); err != nil {
		t.Fatal(err)
	}

	provider := &operationsTestProvider{}
	providers := deliveryTestProviders{provider: provider}
	delivery := NewDeliveryStore(database, dialect, providers, deliveryTestSecrets{})
	trigger := &operationsTriggerProbe{}
	store := NewOperationsStore(database, dialect, delivery, trigger)
	requirements := NewRequirementsStore(database, dialect, providers)
	if err := requirements.SynchronizeEventMappings(t.Context(), []integrationmodel.EventMappingRequirement{{
		Key: "contact-change", WorkspaceID: "workspace-a", Provider: "probe", ConnectionKey: "primary",
		EventType: "contact.changed", CommandPrefix: "contact.", TargetType: "action", ObjectKey: "contact",
		RecordIDPath: "contact.id", ActionKey: "sync", ActionInput: map[string]string{"name": "contact.name"},
		EventFields: []integrationmodel.EventFieldRequirement{{Path: "contact.id", Type: "text", Required: true}, {Path: "contact.name", Type: "text"}, {Path: "command", Type: "text"}},
		Payload:     map[string]any{"source": "webhook"}, Enabled: true,
	}}); err != nil {
		t.Fatal(err)
	}

	call := integrationmodel.ProviderCallRequest{RequestID: "request-1", WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "primary", Operation: "lookup", Payload: json.RawMessage(`{"id":"contact-1"}`)}
	for attempt := 0; attempt < 2; attempt++ {
		result, err := store.Call(t.Context(), call)
		if err != nil || result.Invocation.Status != "succeeded" || string(result.Response) != `{"found":true}` {
			t.Fatalf("attempt=%d result=%#v err=%v", attempt, result, err)
		}
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls=%d, want 1", provider.calls)
	}
	recent, err := store.ListInvocations(t.Context(), integrationmodel.InvocationQuery{WorkspaceID: "workspace-a", CreatedFrom: "2000-01-01T00:00:00Z", Limit: 10})
	if err != nil || len(recent) != 1 {
		t.Fatalf("recent invocations=%#v err=%v", recent, err)
	}
	future, err := store.ListInvocations(t.Context(), integrationmodel.InvocationQuery{WorkspaceID: "workspace-a", CreatedFrom: "2100-01-01T00:00:00Z", Limit: 10})
	if err != nil || len(future) != 0 {
		t.Fatalf("future invocations=%#v err=%v", future, err)
	}

	webhook := integrationmodel.WebhookRequest{WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "primary", Body: []byte(`{}`), ReceivedAt: time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)}
	for attempt := 0; attempt < 2; attempt++ {
		receipt, err := store.AcceptWebhook(t.Context(), webhook)
		if err != nil || receipt.Event.Status != "processed" || receipt.Event.Execution == nil || receipt.Event.Execution.ExecutionID != "action-1" {
			t.Fatalf("attempt=%d receipt=%#v err=%v", attempt, receipt, err)
		}
	}
	if trigger.calls != 1 {
		t.Fatalf("trigger calls=%d, want 1", trigger.calls)
	}
	if trigger.request.Target.ObjectKey != "contact" || trigger.request.Target.RecordID != "contact-1" || trigger.request.Target.ActionKey != "sync" || trigger.request.Target.Input["name"] != "Ada" || trigger.request.Target.Input["source"] != "webhook" {
		t.Fatalf("trigger request=%#v", trigger.request)
	}
}

func TestLocalWorkersPersistProviderStateBeforeDispatchingEvent(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-provider-worker?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	rawDialect, _ := ormdialect.New(ormdialect.SQLite)
	dialect := rawDialect.WithSchema("")
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := database.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	insert, args, err := query.NewInsertBuilder(dialect, "_integration_connections").Columns("id", "connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").Values("connection-background", "primary", "workspace-a", "crm", "background", "Primary", "active", `{}`, `{}`, "admin", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z").Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), insert, args...); err != nil {
		t.Fatal(err)
	}
	provider := &backgroundOperationsTestProvider{}
	providers := deliveryTestProviders{provider: provider}
	delivery := NewDeliveryStore(database, dialect, providers, emptyDeliveryTestSecrets{})
	trigger := &operationsTriggerProbe{}
	operations := NewOperationsStore(database, dialect, delivery, trigger)
	requirements := NewRequirementsStore(database, dialect, providers)
	if err := requirements.SynchronizeEventMappings(t.Context(), []integrationmodel.EventMappingRequirement{{Key: "contact-change", WorkspaceID: "workspace-a", Provider: "background", EventType: "contact.changed", TargetType: "action", ObjectKey: "contact", RecordIDPath: "contact.id", ActionKey: "sync", EventFields: []integrationmodel.EventFieldRequirement{{Path: "contact.id", Type: "text", Required: true}}, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	workers := NewWorkerStore(database, dialect, delivery, operations, "runtime-a")
	processed, err := workers.ProcessDueProviderTasks(t.Context(), 10)
	if err != nil || processed != 1 || provider.calls != 1 {
		t.Fatalf("processed=%d provider_calls=%d err=%v", processed, provider.calls, err)
	}
	var state, status string
	if err := database.QueryRowContext(t.Context(), "SELECT payload_json,status FROM _integration_connector_provider_states WHERE task_key=?", "poll").Scan(&state, &status); err != nil {
		t.Fatal(err)
	}
	if state != `{"cursor":"next"}` || status != "ready" {
		t.Fatalf("state=%s status=%s", state, status)
	}
	processed, err = workers.ProcessDueEvents(t.Context(), 10)
	if err != nil || processed != 1 || trigger.calls != 1 || trigger.request.Target.RecordID != "contact-2" {
		t.Fatalf("processed=%d trigger=%#v err=%v", processed, trigger, err)
	}
}
