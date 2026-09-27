package saas

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

type providerRunManagementHTTPPort struct {
	integrationsdk.Management
	calls int
}

func (p *providerRunManagementHTTPPort) ProviderRunSnapshot(context.Context) (integrationsdk.ProviderRunSnapshot, error) {
	p.calls++
	bucket := 7
	return integrationsdk.ProviderRunSnapshot{ReadyDue: 7, GlobalRateLimit: 20000, GooglePushReceivedHour: 9, GooglePushTimedHour: 8, GooglePushHotBucket: &bucket}, nil
}

func TestProviderRunSnapshotSaaSRequiresHostToken(t *testing.T) {
	port := &providerRunManagementHTTPPort{}
	h := &handler{token: "host-service-token", mux: http.NewServeMux(), management: port}
	h.registerOperations()
	for _, test := range []struct {
		token, runtime string
		status         int
	}{
		{status: http.StatusUnauthorized},
		{token: "host-service-token", status: http.StatusUnauthorized},
		{token: "wrong", runtime: "runtime-a", status: http.StatusUnauthorized},
		{token: "host-service-token", runtime: "runtime-a", status: http.StatusOK},
	} {
		request := httptest.NewRequest(http.MethodGet, "/integration/v1/operations/provider-run-snapshot", nil)
		request.Header.Set("Authorization", "Bearer "+test.token)
		request.Header.Set("X-Domainry-Runtime-ID", test.runtime)
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("status=%d want=%d body=%s", response.Code, test.status, response.Body.String())
		}
		if test.status == http.StatusOK {
			var snapshot integrationsdk.ProviderRunSnapshot
			if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil || snapshot.ReadyDue != 7 || snapshot.GlobalRateLimit != 20000 || snapshot.GooglePushReceivedHour != 9 || snapshot.GooglePushTimedHour != 8 || snapshot.GooglePushHotBucket == nil || *snapshot.GooglePushHotBucket != 7 {
				t.Fatalf("snapshot=%+v err=%v", snapshot, err)
			}
		}
	}
	if port.calls != 1 {
		t.Fatalf("monitor called %d times, want only the authorized call", port.calls)
	}
}
