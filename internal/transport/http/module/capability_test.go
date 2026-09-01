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
		Providers:  []connectorscatalog.ConnectorProviderSchema{{Key: "probe", OperationKeys: []string{"lookup"}}},
		Operations: []connectorscatalog.ConnectorOperationSchema{{Key: "lookup", ExecutionMode: "call", SideEffect: "read"}},
	}}
	binding, err := NewCapabilityBinding(definitions, func(ctx context.Context, request modulecapability.ValidationRequest) (modulecapability.ValidationResult, error) {
		return ValidateCapabilityCandidate(ctx, request, definitions)
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
	if operations != len(integrationRoutes()) || projections != 1 {
		t.Fatalf("Integration operations=%d/%d projections=%d", operations, len(integrationRoutes()), projections)
	}
	request := modulecapability.ValidationRequest{
		ContractVersion: modulecapability.ValidationContractVersion, ModuleKey: "integration", CategoryKey: integrationConnectionsCategory,
		ContractSHA256: summary.Identity.ContractSHA256, Kind: "integration.connection_requirement",
		Candidate: modulecapability.AuthoringFragment{
			Collection: "integrations.connections", Key: "primary",
			Value: json.RawMessage(`{"key":"primary","connector_key":"crm","provider_key":"missing"}`),
		},
	}
	result, err := binding.ValidateCapabilityCandidate(t.Context(), request)
	if err != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0].Owner != "integration" {
		t.Fatalf("Integration diagnostics=%+v err=%v", result.Diagnostics, err)
	}
	contracttest.VerifyModuleRemoteParity(t, binding, contracttest.ValidationCase{Name: "missing provider", Request: request})
}
