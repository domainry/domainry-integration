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

type operationsTestProvider struct {
	calls      int
	duringCall func()
}

func (*operationsTestProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{
		ConnectorKey: "crm", ProviderKey: "probe",
		Operations: []connector.OperationDescriptor{{ConnectorKey: "crm", ProviderKey: "probe", Key: "lookup", Mode: connector.ModeCall, ContractSHA256: strings.Repeat("c", 64)}},
	}
}

func (p *operationsTestProvider) Call(_ context.Context, request connector.CallRequest) (connector.CallResult, error) {
	p.calls++
	if p.duringCall != nil {
		p.duringCall()
	}
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

type backgroundOperationsTestProvider struct {
	calls       int
	commitCalls int
}

func (*backgroundOperationsTestProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "background", ProviderRevision: "1.0.0", Operations: []connector.OperationDescriptor{{
		ConnectorKey: "crm", ProviderKey: "background", Key: "ack", Mode: connector.ModeEnqueue, ContractSHA256: strings.Repeat("e", 64),
		Reliability: connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}},
	}}}
}
func (p *backgroundOperationsTestProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	p.commitCalls++
	return connector.CallResult{ResponseRef: "background-commit-receipt"}, nil
}
func (*backgroundOperationsTestProvider) BackgroundTasks(connector.Connection) []connector.BackgroundTaskDescriptor {
	return []connector.BackgroundTaskDescriptor{{Key: "poll", StateVersion: 1}}
}
func (p *backgroundOperationsTestProvider) ProcessBackground(context.Context, connector.BackgroundRequest) (connector.BackgroundResult, error) {
	p.calls++
	return connector.BackgroundResult{
		State: json.RawMessage(`{"cursor":"next"}`), NextDueAt: time.Now().UTC().Add(time.Hour),
		Events: []connector.BackgroundEvent{{ExternalID: "polled-1", EventType: "contact.changed", Payload: json.RawMessage(`{"contact":{"id":"contact-2"}}`)}},
		Commit: []connector.BackgroundCommit{{OperationKey: "ack", ContractSHA256: strings.Repeat("e", 64), Payload: json.RawMessage(`{"external_id":"polled-1"}`)}},
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
		Values("connection-1", "primary", "workspace-a", "crm", "probe", "Primary", "active", `{}`, `{"token":"secret:token"}`, "admin", timestampMillis("2026-01-01T00:00:00Z"), timestampMillis("2026-01-01T00:00:00Z")).Build()
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
	definitions := newTestDefinitionStore()
	store := NewOperationsStore(database, dialect, delivery, trigger, definitions)
	requirements := NewRequirementsStore(database, dialect, providers, definitions)
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
	var executionJSON string
	if err := database.QueryRowContext(t.Context(), `SELECT execution_json FROM _integration_events WHERE workspace_id=? LIMIT 1`, "workspace-a").Scan(&executionJSON); err != nil || !strings.Contains(executionJSON, `"completed_at":1788134400000`) {
		t.Fatalf("execution receipt must store completed_at as Unix milliseconds: json=%s err=%v", executionJSON, err)
	}
	if trigger.request.Target.ObjectKey != "contact" || trigger.request.Target.RecordID != "contact-1" || trigger.request.Target.ActionKey != "sync" || trigger.request.Target.Input["name"] != "Ada" || trigger.request.Target.Input["source"] != "webhook" {
		t.Fatalf("trigger request=%#v", trigger.request)
	}
	if trigger.request.MappingRevision == "" || trigger.request.Source.Provider != "probe" || trigger.request.Source.EventType != "contact.changed" || trigger.request.Source.ExternalID != "external-event-1" || trigger.request.Source.ReceivedAt != webhook.ReceivedAt.Format(time.RFC3339Nano) {
		t.Fatalf("trigger provenance=%#v", trigger.request)
	}
}

func TestOperationsStoreMapsVerifiedEventToFiniteAgentTargetAndCurrentIdentity(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-agent-event?mode=memory&cache=shared")
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
			if _, err = database.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	insert, args, err := query.NewInsertBuilder(dialect, "_integration_connections").
		Columns("id", "connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").
		Values("connection-agent", "primary", "workspace-a", "crm", "probe", "Primary", "active", `{}`, `{"token":"secret:token"}`, "admin", timestampMillis("2026-01-01T00:00:00Z"), timestampMillis("2026-01-01T00:00:00Z")).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = database.ExecContext(t.Context(), insert, args...); err != nil {
		t.Fatal(err)
	}
	if _, err = database.ExecContext(t.Context(), `INSERT INTO _integration_external_identities (id,identity_key,workspace_id,provider,external_subject,external_subject_type,external_name,external_organization,external_department,external_group,external_bot_id,actor_id,role_key,status,last_resolved_at,created_by,created_at,updated_at,disabled_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"identity-agent", "contact-1", "workspace-a", "probe", "contact-1", "user", "Ada", "", "", "", "", "user-7", "support", "active", int64(0), "admin", timestampMillis("2026-01-01T00:00:00Z"), timestampMillis("2026-01-01T00:00:00Z"), int64(0)); err != nil {
		t.Fatal(err)
	}
	provider := &operationsTestProvider{}
	providers := deliveryTestProviders{provider: provider}
	delivery := NewDeliveryStore(database, dialect, providers, deliveryTestSecrets{})
	trigger := &operationsTriggerProbe{}
	definitions := newTestDefinitionStore()
	store := NewOperationsStore(database, dialect, delivery, trigger, definitions)
	requirements := NewRequirementsStore(database, dialect, providers, definitions)
	if err = requirements.SynchronizeEventMappings(t.Context(), []integrationmodel.EventMappingRequirement{{
		Key: "contact-agent", WorkspaceID: "workspace-a", Provider: "probe", ConnectionKey: "primary", EventType: "contact.changed", TargetType: "agent_task",
		AgentID: "support-agent", ConversationID: "conversation-support", AgentTaskMode: "start", AgentInput: map[string]string{"customer_name": "contact.name"},
		EventFields:      []integrationmodel.EventFieldRequirement{{Path: "contact.id", Type: "text", Required: true}, {Path: "contact.name", Type: "text"}},
		ExternalIdentity: integrationmodel.ExternalIdentityMappingRequirement{Provider: "probe", SubjectPath: "contact.id", OnUnmapped: "error"},
		Payload:          map[string]any{"goal": "Review changed contact", "allowed_tools": []any{}}, Enabled: true,
	}}); err != nil {
		t.Fatal(err)
	}
	webhook := integrationmodel.WebhookRequest{WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "primary", Body: []byte(`{}`), ReceivedAt: time.Date(2026, 9, 16, 9, 30, 0, 0, time.UTC)}
	receipt, err := store.AcceptWebhook(t.Context(), webhook)
	if err != nil || receipt.Event.Status != "processed" || trigger.calls != 1 {
		t.Fatalf("receipt=%+v trigger=%+v err=%v", receipt, trigger, err)
	}
	request := trigger.request
	if request.Target.Type != "agent_task" || request.Target.AgentID != "support-agent" || request.Target.ConversationID != "conversation-support" || request.Target.AgentTaskMode != "start" || request.Target.RelatedTaskID != "" ||
		request.Target.Input["goal"] != "Review changed contact" || request.Target.Input["customer_name"] != "Ada" || request.Principal.ActorID != "user-7" || request.Principal.RoleKey != "support" || len(request.MappingRevision) != 64 || request.Source.ReceivedAt != webhook.ReceivedAt.Format(time.RFC3339Nano) {
		t.Fatalf("Agent trigger request=%#v", request)
	}
}

func TestOperationsStoreSensitiveCallNeverPersistsPlaintext(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-sensitive-operations?mode=memory&cache=shared")
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
		Values("connection-sensitive", "primary", "workspace-a", "crm", "probe", "Primary", "active", `{}`, `{"token":"secret:token"}`, "admin", timestampMillis("2026-01-01T00:00:00Z"), timestampMillis("2026-01-01T00:00:00Z")).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), insert, args...); err != nil {
		t.Fatal(err)
	}
	provider := &operationsTestProvider{}
	var duringCallMetadata string
	var duringCallReadErr error
	provider.duringCall = func() {
		duringCallReadErr = database.QueryRowContext(t.Context(), "SELECT metadata_json FROM _integration_invocations WHERE request_ref = ?", "otp-1").Scan(&duringCallMetadata)
	}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, deliveryTestSecrets{})
	store := NewOperationsStore(database, dialect, delivery, nil, newTestDefinitionStore())
	request := integrationmodel.ProviderCallRequest{
		RequestID: "otp-1", WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "primary", Operation: "lookup",
		Payload: json.RawMessage(`{"pin":"917204","destination":"+15555550123"}`), PersistenceMode: integrationmodel.ProviderCallPersistenceSensitive, MaskedDestination: "+1*******0123",
	}
	result, err := store.Call(t.Context(), request)
	if err != nil || result.Invocation.Status != "succeeded" || string(result.Response) != `{"found":true}` {
		t.Fatalf("sensitive call result=%#v err=%v", result, err)
	}
	if duringCallReadErr != nil {
		t.Fatalf("read invocation while provider was handling payload: %v", duringCallReadErr)
	}
	if strings.Contains(duringCallMetadata, "917204") || strings.Contains(duringCallMetadata, "+15555550123") {
		t.Fatalf("sensitive payload was transiently persisted before provider return: %s", duringCallMetadata)
	}
	var persisted string
	if err := database.QueryRowContext(t.Context(), "SELECT metadata_json FROM _integration_invocations WHERE request_ref = ?", "otp-1").Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(persisted, "917204") || strings.Contains(persisted, "+15555550123") || strings.Contains(persisted, `"request"`) || strings.Contains(persisted, `"response"`) {
		t.Fatalf("sensitive payload entered invocation evidence: %s", persisted)
	}
	if strings.Contains(persisted, `"request_sha256"`) || strings.Contains(persisted, `"response_ref_sha256"`) || !strings.Contains(persisted, `"masked_destination":"+1*******0123"`) {
		t.Fatalf("sensitive audit evidence incomplete: %s", persisted)
	}
	if result.Invocation.ResponseRef != "" {
		t.Fatalf("sensitive response reference was not redacted: invocation=%#v metadata=%s", result.Invocation, persisted)
	}
	replay, err := store.Call(t.Context(), request)
	if err != nil || len(replay.Response) != 0 || provider.calls != 1 {
		t.Fatalf("sensitive replay result=%#v calls=%d err=%v", replay, provider.calls, err)
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
	insert, args, err := query.NewInsertBuilder(dialect, "_integration_connections").Columns("id", "connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").Values("connection-background", "primary", "workspace-a", "crm", "background", "Primary", "active", `{}`, `{}`, "admin", timestampMillis("2026-01-01T00:00:00Z"), timestampMillis("2026-01-01T00:00:00Z")).Build()
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
	definitions := newTestDefinitionStore()
	operations := NewOperationsStore(database, dialect, delivery, trigger, definitions)
	requirements := NewRequirementsStore(database, dialect, providers, definitions)
	if err := requirements.SynchronizeEventMappings(t.Context(), []integrationmodel.EventMappingRequirement{{Key: "contact-change", WorkspaceID: "workspace-a", Provider: "background", EventType: "contact.changed", TargetType: "action", ObjectKey: "contact", RecordIDPath: "contact.id", ActionKey: "sync", EventFields: []integrationmodel.EventFieldRequirement{{Path: "contact.id", Type: "text", Required: true}}, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	workers := NewWorkerStore(database, dialect, delivery, operations, "runtime-a")
	// Force the previously flaky precision prefix: .123Z sorts after .1231Z,
	// although the second instant is later. Do not rely on the machine clock.
	clockCalls := 0
	workers.clock = func() time.Time {
		clockCalls++
		nanos := 123000000
		if clockCalls > 1 {
			nanos = 123100000
		}
		return time.Date(2026, 9, 11, 10, 0, 0, nanos, time.UTC)
	}
	processed, err := workers.ProcessDueProviderTasks(t.Context(), 10)
	if err != nil || processed != 2 || provider.calls != 1 || provider.commitCalls != 1 {
		t.Fatalf("processed=%d provider_calls=%d commit_calls=%d err=%v", processed, provider.calls, provider.commitCalls, err)
	}
	var state, status string
	if err := database.QueryRowContext(t.Context(), "SELECT payload_json,status FROM _integration_provider_runs WHERE run_kind=? AND run_key=?", providerRunKindState, "poll").Scan(&state, &status); err != nil {
		t.Fatal(err)
	}
	if state != `{"cursor":"next"}` || status != "ready" {
		t.Fatalf("state=%s status=%s", state, status)
	}
	var stateRuns, commitRuns int
	if err := database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_provider_runs WHERE run_kind=?", providerRunKindState).Scan(&stateRuns); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_provider_runs WHERE run_kind=?", providerRunKindCommit).Scan(&commitRuns); err != nil {
		t.Fatal(err)
	}
	if stateRuns != 1 || commitRuns != 1 {
		t.Fatalf("provider run kinds state=%d commit=%d", stateRuns, commitRuns)
	}
	processed, err = workers.ProcessDueEvents(t.Context(), 10)
	if err != nil || processed != 1 || trigger.calls != 1 || trigger.request.Target.RecordID != "contact-2" {
		t.Fatalf("processed=%d trigger=%#v err=%v", processed, trigger, err)
	}
	// SQL deliberately scans the whole current second. Precision checks must
	// still prevent a slightly future deadline or an unexpired lease from firing.
	for _, deadline := range []struct{ due, lease int64 }{
		{time.Date(2026, 9, 11, 10, 0, 0, 124000000, time.UTC).UnixMilli(), 0},
		{time.Date(2026, 9, 11, 10, 0, 0, 123000000, time.UTC).UnixMilli(), time.Date(2026, 9, 11, 10, 0, 0, 124000000, time.UTC).UnixMilli()},
	} {
		if _, err := database.ExecContext(t.Context(), "UPDATE _integration_provider_runs SET due_at=?,lease_expires_at=? WHERE run_kind=? AND run_key=?", deadline.due, deadline.lease, providerRunKindState, "poll"); err != nil {
			t.Fatal(err)
		}
		processed, err := workers.processProviderTasks(t.Context(), 10)
		if err != nil || processed != 0 || provider.calls != 1 || provider.commitCalls != 1 {
			t.Fatalf("future work claimed: processed=%d calls=%d commit_calls=%d err=%v", processed, provider.calls, provider.commitCalls, err)
		}
	}

}
