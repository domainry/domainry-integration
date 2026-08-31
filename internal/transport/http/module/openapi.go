package module

import (
	"github.com/domainry/domainry-foundation/modulehttp"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

// OpenAPIOperations publishes Integration-owned operation contracts beside
// the Integration-owned Surface. Runtime only mounts and annotates these
// operations; it does not recreate Integration routes or schemas.
func (s *surface) OpenAPIOperations() map[string]map[string]any {
	return integrationsdk.IntegrationHTTPSurfaceContract().OpenAPI
}

var _ modulehttp.OpenAPIProvider = (*surface)(nil)
