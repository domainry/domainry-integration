package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	ormbuilder "github.com/domainry/domainry-orm/builder"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	_ "modernc.org/sqlite"
)

type deliveryTestProvider struct {
	descriptor connector.ProviderDescriptor
	calls      int
}

func (p *deliveryTestProvider) Descriptor() connector.ProviderDescriptor { return p.descriptor }
func (p *deliveryTestProvider) Call(_ context.Context, request connector.CallRequest) (connector.CallResult, error) {
	p.calls++
	if !request.Delivery || request.RequestRef != "message-1" || request.Secrets["token"] != "plain-token" {
		return connector.CallResult{}, sql.ErrNoRows
	}
	return connector.CallResult{ResponseRef: "provider-receipt-1"}, nil
}

type deliveryTestProviders struct{ provider connector.Adapter }

func (p deliveryTestProviders) Provider(connectorKey, providerKey string) (connector.Adapter, bool) {
	descriptor := p.provider.Descriptor()
	return p.provider, connectorKey == descriptor.ConnectorKey && providerKey == descriptor.ProviderKey
}

type webPushDeliveryProvider struct{ payload map[string]any }

func (*webPushDeliveryProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "notification", ProviderKey: "web_push", Operations: []connector.OperationDescriptor{{ConnectorKey: "notification", ProviderKey: "web_push", Key: "send", Mode: connector.ModeEnqueue, ContractSHA256: strings.Repeat("b", 64)}}}
}
func (p *webPushDeliveryProvider) Call(_ context.Context, request connector.CallRequest) (connector.CallResult, error) {
	if err := json.Unmarshal(request.Payload, &p.payload); err != nil {
		return connector.CallResult{}, err
	}
	return connector.CallResult{ResponseRef: "push-receipt"}, nil
}

func TestModuleDeliveryResolvesWebPushMaterialInsideIntegrationOwner(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-web-push-delivery?mode=memory&cache=shared")
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
	insert, args, err := ormbuilder.NewInsertBuilder(dialect, "integration_connections").Columns("id", "connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").Values("push-connection", "push", "workspace-a", "notification", "web_push", "Push", "active", `{}`, `{}`, "admin", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z").Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), insert, args...); err != nil {
		t.Fatal(err)
	}
	subscriptions := NewWebPushSubscriptionStore(database, dialect)
	if _, err := subscriptions.Upsert(t.Context(), "workspace-a", "user-a", "browser-a", integrationsdk.WebPushSubscriptionInput{Endpoint: "https://push.example/a", P256DH: "p256dh", Auth: "auth"}); err != nil {
		t.Fatal(err)
	}
	provider := &webPushDeliveryProvider{}
	store := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{}, subscriptions)
	receipt, err := store.Accept(t.Context(), integrationsdk.DeliveryRequest{MessageID: "push-message", DeduplicationKey: "push-dedup", WorkspaceID: "workspace-a", ConnectorKey: "notification", ConnectionKey: "push", Operation: "send", Payload: json.RawMessage(`{"subscription_id":"browser-a","title":"Hello"}`)})
	if err != nil || receipt.Status != integrationsdk.DeliveryStatusSucceeded {
		t.Fatalf("receipt=%#v err=%v", receipt, err)
	}
	if provider.payload["endpoint"] != "https://push.example/a" || provider.payload["p256dh"] != "p256dh" || provider.payload["auth"] != "auth" {
		t.Fatalf("provider payload=%#v", provider.payload)
	}
	var persisted string
	if err := database.QueryRowContext(t.Context(), "SELECT metadata_json FROM integration_invocations WHERE request_ref = ?", "push-message").Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(persisted, "push.example") || strings.Contains(persisted, "p256dh") || strings.Contains(persisted, "auth") {
		t.Fatalf("sensitive material persisted in invocation: %s", persisted)
	}
}
func (p deliveryTestProviders) Descriptors() []connector.ProviderDescriptor {
	return []connector.ProviderDescriptor{p.provider.Descriptor()}
}

type deliveryTestSecrets struct{}

type emptyDeliveryTestSecrets struct{}

func (emptyDeliveryTestSecrets) ResolveSecretReferences(context.Context, string, map[string]string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (deliveryTestSecrets) ResolveSecretReferences(_ context.Context, workspaceID string, references map[string]string) (map[string]string, error) {
	if workspaceID != "workspace-a" || references["token"] != "secret:token" {
		return nil, sql.ErrNoRows
	}
	return map[string]string{"token": "plain-token"}, nil
}

func TestModuleDeliveryPersistsInvocationAndDeduplicatesProviderCall(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-delivery?mode=memory&cache=shared")
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
	insert, args, err := ormbuilder.NewInsertBuilder(dialect, "integration_connections").Columns("id", "connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").Values("connection-1", "primary", "workspace-a", "crm", "probe", "Primary", "active", `{}`, `{"token":"secret:token"}`, "admin", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z").Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), insert, args...); err != nil {
		t.Fatal(err)
	}
	provider := &deliveryTestProvider{descriptor: connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "probe", Operations: []connector.OperationDescriptor{{ConnectorKey: "crm", ProviderKey: "probe", Key: "upsert", Mode: connector.ModeEnqueue, ContractSHA256: strings.Repeat("a", 64)}}}}
	store := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, deliveryTestSecrets{})
	request := integrationsdk.DeliveryRequest{MessageID: "message-1", DeduplicationKey: "record:1", WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "primary", Operation: "upsert", Payload: json.RawMessage(`{"id":"1"}`)}
	for attempt := 0; attempt < 2; attempt++ {
		receipt, err := store.Accept(t.Context(), request)
		if err != nil || receipt.Status != integrationsdk.DeliveryStatusSucceeded || receipt.ResultRef != "provider-receipt-1" {
			t.Fatalf("attempt=%d receipt=%#v err=%v", attempt, receipt, err)
		}
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls=%d, want 1", provider.calls)
	}
	receipt, err := store.Query(t.Context(), request.MessageID)
	if err != nil || receipt.InvocationID == "" || receipt.Status != integrationsdk.DeliveryStatusSucceeded {
		t.Fatalf("query=%#v err=%v", receipt, err)
	}
}
