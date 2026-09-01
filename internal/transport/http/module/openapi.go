package module

import (
	"github.com/domainry/domainry-foundation/modulehttp"
)

// OpenAPIOperations publishes Integration-owned operation contracts beside
// the Integration-owned Surface. Runtime only mounts and annotates these
// operations; it does not recreate Integration routes or schemas.
func (s *surface) OpenAPIOperations() map[string]map[string]any {
	return s.operations
}

var _ modulehttp.OpenAPIProvider = (*surface)(nil)
