package saas

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdk "github.com/domainry/domainry-integration-sdk"
)

type apiKeyAuthenticationHTTPStub struct{ calls int }

func (s *apiKeyAuthenticationHTTPStub) AuthenticateAPIKey(_ context.Context, token, workspace string) (sdk.APIKey, error) {
	s.calls++
	if token == "unavailable" {
		return sdk.APIKey{}, context.Canceled
	}
	if token != "itg_private" || workspace != "workspace-a" {
		return sdk.APIKey{}, sdk.ErrAPIKeyInvalid
	}
	return sdk.APIKey{Key: "service", WorkspaceID: workspace, ActorID: "service-a", RoleKey: "sales", Scopes: []string{"account.read"}, Status: "active"}, nil
}

func TestAPIKeyAuthenticationSaaSRequiresHostCredentialAndHidesInvalidToken(t *testing.T) {
	port := &apiKeyAuthenticationHTTPStub{}
	h := &handler{token: "host-service-token", apiKeys: port, mux: http.NewServeMux()}
	h.register()
	for _, test := range []struct {
		name, serviceToken, runtimeID, body string
		status                              int
	}{
		{"missing host credential", "", "", `{"token":"itg_private","workspace_id":"workspace-a"}`, http.StatusUnauthorized},
		{"invalid business token", "host-service-token", "runtime-a", `{"token":"bad","workspace_id":"workspace-a"}`, http.StatusUnauthorized},
		{"owner unavailable", "host-service-token", "runtime-a", `{"token":"unavailable","workspace_id":"workspace-a"}`, http.StatusServiceUnavailable},
		{"valid business token", "host-service-token", "runtime-a", `{"token":"itg_private","workspace_id":"workspace-a"}`, http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/integration/v1/api-key-authentication", strings.NewReader(test.body))
			request.Header.Set("Authorization", "Bearer "+test.serviceToken)
			request.Header.Set("X-Domainry-Runtime-ID", test.runtimeID)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, request)
			if response.Code != test.status || !json.Valid(response.Body.Bytes()) {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if test.status == http.StatusUnauthorized && strings.Contains(response.Body.String(), "itg_private") {
				t.Fatalf("authentication response leaked token: %s", response.Body.String())
			}
		})
	}
	if port.calls != 3 {
		t.Fatalf("authentication calls=%d, want invalid, unavailable and valid business tokens", port.calls)
	}
}
