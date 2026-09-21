// Package capability exposes Integration's source-owned capability contract
// and its nested Connector projections for explicit Plane composition.
package capability

import (
	"github.com/domainry/domainry-foundation/modulecapability"
)

// Inputs is intentionally empty. The official Connectors catalog is the
// source-owned Integration capability contract; a Runtime ProviderRegistry is
// deployment evidence and must not change disclosure or its digest.
type Inputs struct{}

func Open(inputs Inputs) (*modulecapability.StaticBinding, error) {
	return openContract(inputs)
}
