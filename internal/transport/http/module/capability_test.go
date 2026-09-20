package module

import (
	"context"
	"encoding/json"
	"testing"

	connectorscatalog "github.com/domainry/domainry-connectors/catalog"
	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulecapability/contracttest"
)

func TestIntegrationCapabilityTracksRoutesConnectorsAndValidation(t *testing.T) {
	definitions := []connectorscatalog.ConnectorSchema{{
		Key: "crm", Name: "CRM", Type: "http",
		Providers: []connectorscatalog.ConnectorProviderSchema{
			{Key: "planned"},
			{Key: "probe", OperationKeys: []string{"lookup"}},
		},
		Operations: []connectorscatalog.ConnectorOperationSchema{{Key: "lookup", ExecutionMode: "call", SideEffect: "read"}},
	}}
	releasedProviders := []connectorscatalog.ProviderEntry{{
		ConnectorKey: "crm", ProviderKey: "probe", ProviderRevision: "1.0.0",
		Operations: []connectorscatalog.OperationEntry{{Key: "lookup", Mode: "call"}},
	}}
	binding, err := NewCapabilityBinding(definitions, releasedProviders, func(ctx context.Context, request modulecapability.ValidationRequest) (modulecapability.ValidationResult, error) {
		return ValidateCapabilityCandidate(ctx, request, definitions, releasedProviders)
	})
	if err != nil {
		t.Fatal(err)
	}
	contracttest.VerifyBinding(t, binding)
	summary, err := binding.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	operations, projections := 0, 0
	for _, category := range summary.Categories {
		operations += category.OperationCount
		projections += category.ProjectionCount
	}
	routes, err := integrationRoutes()
	if err != nil {
		t.Fatal(err)
	}
	if operations != len(routes) || projections != 1 {
		t.Fatalf("Integration operations=%d/%d projections=%d", operations, len(routes), projections)
	}
	var connector connectorProjection
	for _, category := range summary.Categories {
		if category.ProjectionCount == 0 {
			continue
		}
		document, categoryErr := binding.CapabilityCategory(t.Context(), category.Key)
		if categoryErr != nil {
			t.Fatal(categoryErr)
		}
		if err := json.Unmarshal(document.Projections[0].Payload, &connector); err != nil {
			t.Fatal(err)
		}
	}
	if connector.Availability != "released" || len(connector.Providers) != 1 || connector.Providers[0].Key != "probe" || len(connector.Operations) != 1 || connector.Operations[0].Key != "lookup" {
		t.Fatalf("Connector projection=%+v", connector)
	}
	for _, capability := range summary.Scenarios.ProvidedCapabilities {
		if capability == "connector.crm.planned" {
			t.Fatalf("unreleased Provider leaked into provided capabilities: %q", capability)
		}
	}
	request := modulecapability.ValidationRequest{
		ContractVersion: modulecapability.ValidationContractVersion, ModuleKey: "integration", CategoryKey: integrationConnectionsCategory,
		ContractSHA256: summary.Identity.ContractSHA256, Kind: "integration.connection_requirement",
		Candidate: modulecapability.AuthoringFragment{
			Collection: "integrations.connections", Key: "primary",
			Value: json.RawMessage(`{"key":"primary","connector_key":"crm","provider_key":"planned"}`),
		},
	}
	result, err := binding.ValidateCapabilityCandidate(t.Context(), request)
	if err != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0].Owner != "integration" {
		t.Fatalf("Integration diagnostics=%+v err=%v", result.Diagnostics, err)
	}
	contracttest.VerifyModuleRemoteParity(t, binding, contracttest.ValidationCase{Name: "unreleased provider", Request: request})
}
