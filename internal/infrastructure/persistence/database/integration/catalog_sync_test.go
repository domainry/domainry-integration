package integration

import (
	"encoding/json"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
)

func TestConnectorCatalogPublishesSharedDefinitionsWithProviderOverlay(t *testing.T) {
	definitions := newTestDefinitionStore()
	catalog := []connector.ConnectorDefinition{{
		Key: "crm", Name: "Customer CRM",
		Payload: json.RawMessage(`{"key":"crm","name":"Customer CRM","description":"Connectors owns this text","classification":"business"}`),
	}}
	descriptor := connector.ProviderDescriptor{
		ConnectorKey: "crm", ProviderKey: "probe", ProviderRevision: "1.0.0", StartupActivation: connector.StartupActivationDefaultSafe,
		Operations: []connector.OperationDescriptor{{
			ConnectorKey: "crm", ProviderKey: "probe", Key: "lookup", Mode: connector.ModeCall,
			ContractSHA256: strings.Repeat("d", 64),
			Reliability: connector.ReliabilityContract{
				Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural},
				Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone},
			},
		}},
	}
	if err := SyncConnectorCatalog(t.Context(), definitions, catalog, []connector.ProviderDescriptor{descriptor}); err != nil {
		t.Fatal(err)
	}
	definition, found, err := definitions.Get(t.Context(), metadatasdk.DefinitionOwnerIntegration, integrationConnectorDefinitionKind, "crm")
	if err != nil || !found {
		t.Fatal(definition, found, err)
	}
	if definition.SourceID != integrationConnectorSource || !strings.HasPrefix(definition.SchemaVersion, integrationConnectorSchemaVersion+"-") {
		t.Fatalf("unexpected shared Definition identity: %#v", definition)
	}
	var projected map[string]any
	if err := json.Unmarshal(definition.Payload, &projected); err != nil {
		t.Fatal(err)
	}
	if projected["name"] != "Customer CRM" || projected["description"] != "Connectors owns this text" || projected["classification"] != "business" {
		t.Fatalf("Connectors-owned fields were overwritten: %#v", projected)
	}
	providers, _ := projected["providers"].([]any)
	operations, _ := projected["operations"].([]any)
	if len(providers) != 1 || providers[0].(map[string]any)["key"] != "probe" || len(operations) != 1 || operations[0].(map[string]any)["key"] != "lookup" {
		t.Fatalf("provider overlay missing: %#v", projected)
	}
	items, err := NewCatalogStore(definitions).ListConnectorDefinitions(t.Context())
	if err != nil || len(items) != 1 || items[0].Key != "crm" || items[0].DisplayName != "Customer CRM" {
		t.Fatalf("catalog read did not use shared Definitions: %#v err=%v", items, err)
	}
}

func TestConnectorCatalogReplacementDisablesOmittedDefinitionsAndRetainsHistory(t *testing.T) {
	definitions := newTestDefinitionStore()
	initial := []connector.ConnectorDefinition{
		{Key: "crm", Name: "CRM", Payload: json.RawMessage(`{"key":"crm","name":"CRM"}`)},
		{Key: "mail", Name: "Mail", Payload: json.RawMessage(`{"key":"mail","name":"Mail"}`)},
	}
	if err := SyncConnectorCatalog(t.Context(), definitions, initial, nil); err != nil {
		t.Fatal(err)
	}
	old, found, err := definitions.Get(t.Context(), metadatasdk.DefinitionOwnerIntegration, integrationConnectorDefinitionKind, "crm")
	if err != nil || !found {
		t.Fatal(old, found, err)
	}
	updated := []connector.ConnectorDefinition{{Key: "crm", Name: "CRM 2", Payload: json.RawMessage(`{"key":"crm","name":"CRM 2"}`)}}
	if err := SyncConnectorCatalog(t.Context(), definitions, updated, nil); err != nil {
		t.Fatal(err)
	}
	if _, found, err := definitions.Get(t.Context(), metadatasdk.DefinitionOwnerIntegration, integrationConnectorDefinitionKind, "mail"); err != nil || found {
		t.Fatalf("omitted connector remained active: found=%v err=%v", found, err)
	}
	current, found, err := definitions.Get(t.Context(), metadatasdk.DefinitionOwnerIntegration, integrationConnectorDefinitionKind, "crm")
	if err != nil || !found || current.CurrentVersionID == old.CurrentVersionID || current.Name != "CRM 2" {
		t.Fatalf("connector replacement failed: old=%#v current=%#v found=%v err=%v", old, current, found, err)
	}
	version, found, err := definitions.GetVersion(t.Context(), metadatasdk.DefinitionVersionQuery{
		Owner: metadatasdk.DefinitionOwnerIntegration, ResourceType: integrationConnectorDefinitionKind,
		ResourceKey: "crm", VersionID: old.CurrentVersionID,
	})
	if err != nil || !found || !strings.Contains(string(version.Payload), `"name":"CRM"`) {
		t.Fatalf("immutable connector history missing: %#v found=%v err=%v", version, found, err)
	}
}
