package capability

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulecapability/contracttest"
)

func TestOpenUsesImmutableOfficialConnectorCatalog(t *testing.T) {
	first, err := Open(Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	contracttest.VerifyBinding(t, first)
	firstSummary, err := first.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	secondSummary, err := second.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if firstSummary.Identity.ContractSHA256 != secondSummary.Identity.ContractSHA256 {
		t.Fatalf("Integration digest changed across explicit opens: %q != %q", firstSummary.Identity.ContractSHA256, secondSummary.Identity.ContractSHA256)
	}
	projectionCount := 0
	for _, category := range firstSummary.Categories {
		projectionCount += category.ProjectionCount
	}
	if projectionCount < 50 {
		t.Fatalf("official Connector projections=%d", projectionCount)
	}
	for _, category := range firstSummary.Categories {
		if category.ProjectionCount == 0 {
			continue
		}
		document, categoryErr := first.CapabilityCategory(t.Context(), category.Key)
		if categoryErr != nil {
			t.Fatal(categoryErr)
		}
		for _, projection := range document.Projections {
			for _, forbidden := range [][]byte{[]byte(`"adapter_ready"`), []byte(`"connection_ready"`), []byte(`"definition_ready"`), []byte(`"readiness"`), []byte(`"startup_activation"`), []byte(`"verification"`), []byte(`"test_command"`), []byte(`"import_path"`)} {
				if bytes.Contains(projection.Payload, forbidden) {
					t.Fatalf("projection %q leaked operational field %s", projection.Key, forbidden)
				}
			}
		}
	}
	request := modulecapability.ValidationRequest{
		ContractVersion: modulecapability.ValidationContractVersion,
		ModuleKey:       "integration",
		CategoryKey:     "integration.connections",
		ContractSHA256:  firstSummary.Identity.ContractSHA256,
		Kind:            "integration.connection_requirement",
		Candidate: modulecapability.AuthoringFragment{
			Collection: "integrations.connections", Key: "primary",
			Value: json.RawMessage(`{"key":"primary","connector_key":"missing","provider_key":"missing"}`),
		},
	}
	result, err := first.ValidateCapabilityCandidate(t.Context(), request)
	if err != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0].RuleKey != "integration.connection_requirement.provider_not_found" {
		t.Fatalf("connection diagnostics=%+v err=%v", result.Diagnostics, err)
	}
}
