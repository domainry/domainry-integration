// Package capability exposes Integration's source-owned capability contract
// and its nested Connector projections for explicit Plane composition.
package capability

import (
	"context"

	connectorscatalog "github.com/domainry/domainry-connectors/catalog"
	"github.com/domainry/domainry-foundation/modulecapability"
	integrationhttp "github.com/domainry/domainry-integration/internal/transport/http/module"
)

// Inputs is intentionally empty. The official Connectors catalog is the
// source-owned Integration capability contract; a Runtime ProviderRegistry is
// deployment evidence and must not change disclosure or its digest.
type Inputs struct{}

func Open(inputs Inputs) (*modulecapability.StaticBinding, error) {
	_ = inputs
	definitions, err := connectorscatalog.DefinitionDocuments()
	if err != nil {
		return nil, err
	}
	validator := func(ctx context.Context, request modulecapability.ValidationRequest) (modulecapability.ValidationResult, error) {
		return integrationhttp.ValidateCapabilityCandidate(ctx, request, definitions)
	}
	return integrationhttp.NewCapabilityBinding(definitions, validator)
}
