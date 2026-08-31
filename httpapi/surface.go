// Package httpapi exposes Integration's product HTTP contract independently
// from whether its SDK Binding is local or remote.
package httpapi

import (
	foundationhttp "github.com/domainry/domainry-foundation/modulehttp"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationhttp "github.com/domainry/domainry-integration/internal/transport/http/module"
)

// NewSurface adapts an Integration Binding to the module-owned product HTTP
// paths. The same Surface is used with Module and SaaS Bindings.
func NewSurface(binding integrationsdk.Binding) (foundationhttp.Surface, error) {
	return integrationhttp.NewSurface(binding)
}
