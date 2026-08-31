package module

import (
	"strings"
	"testing"

	"github.com/domainry/domainry-foundation/modulehttp"
)

func TestIntegrationSurfacePublishesOpenAPIForEveryOwnedRoute(t *testing.T) {
	routes := integrationRoutes()
	surface := &surface{routes: routes}
	operations := surface.OpenAPIOperations()
	if len(operations) != len(routes) {
		t.Fatalf("OpenAPI operations = %d, routes = %d", len(operations), len(routes))
	}
	for _, route := range routes {
		operation, ok := operations[route.Pattern]
		if !ok {
			t.Errorf("missing OpenAPI operation for %q", route.Pattern)
			continue
		}
		if strings.TrimSpace(operation["operationId"].(string)) == "" {
			t.Errorf("route %q has no operationId", route.Pattern)
		}
		security, exists := operation["security"]
		if !exists {
			t.Errorf("route %q has no security contract", route.Pattern)
			continue
		}
		if route.Authentication == modulehttp.AuthenticationAnonymous {
			if values, ok := security.([]any); !ok || len(values) != 0 {
				t.Errorf("anonymous route %q is not disclosed as anonymous: %#v", route.Pattern, security)
			}
		}
	}
}

func TestIntegrationRoutesContainNoRetiredRuntimeOperations(t *testing.T) {
	for _, route := range integrationRoutes() {
		if strings.Contains(route.Pattern, "/operations/integrations") || strings.Contains(route.Pattern, "/integrations/outbox") {
			t.Errorf("retired Runtime-owned Integration route returned: %q", route.Pattern)
		}
	}
}
