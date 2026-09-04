package module

import (
	"strings"
	"testing"

	actioncontract "github.com/domainry/domainry-foundation/action"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

func TestIntegrationSurfacePublishesOpenAPIForEveryOwnedRoute(t *testing.T) {
	routes, err := integrationRoutes()
	if err != nil {
		t.Fatal(err)
	}
	adapter := &adapter{routes: routes, operations: integrationsdk.IntegrationHTTPAdapterContract().OpenAPI}
	operations := adapter.OpenAPIOperations()
	if len(operations) != len(routes) {
		t.Fatalf("OpenAPI operations = %d, routes = %d", len(operations), len(routes))
	}
	for _, route := range routes {
		pattern := route.Pattern()
		operation, ok := operations[pattern]
		if !ok {
			t.Errorf("missing OpenAPI operation for %q", pattern)
			continue
		}
		if strings.TrimSpace(operation["operationId"].(string)) == "" {
			t.Errorf("route %q has no operationId", pattern)
		}
		security, exists := operation["security"]
		if !exists {
			t.Errorf("route %q has no security contract", pattern)
			continue
		}
		if route.Action.Authorization.Strategy == actioncontract.AuthorizationSigned {
			if values, ok := security.([]any); !ok || len(values) != 0 {
				t.Errorf("anonymous route %q is not disclosed as anonymous: %#v", pattern, security)
			}
		}
	}
}

func TestIntegrationRoutesContainNoRetiredRuntimeOperations(t *testing.T) {
	routes, err := integrationRoutes()
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range routes {
		if strings.Contains(route.Pattern(), "/operations/integrations") || strings.Contains(route.Pattern(), "/integrations/outbox") {
			t.Errorf("retired Runtime-owned Integration route returned: %q", route.Pattern())
		}
	}
}
