package saas

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	sdk "github.com/domainry/domainry-integration-sdk"
)

type accountWriteHTTPPort struct{ calls atomic.Int32 }

func (p *accountWriteHTTPPort) AuthorizeConnectionAccountWrite(context.Context, sdk.ConnectionAccountSubject, string, sdk.ConnectionAccountWriteOperation) (sdk.ConnectionAccountWriteAccess, error) {
	p.calls.Add(1)
	return sdk.ConnectionAccountWriteAccess{}, nil
}
func (p *accountWriteHTTPPort) WriteConnectionAccount(context.Context, sdk.ConnectionAccountSubject, string, sdk.ConnectionAccountWriteRequest) (sdk.ConnectionAccountWriteResult, error) {
	p.calls.Add(1)
	return sdk.ConnectionAccountWriteResult{}, nil
}
func (p *accountWriteHTTPPort) ReadConnectionAccountWriteReceipt(context.Context, sdk.ConnectionAccountSubject, string, sdk.ConnectionAccountWriteRequest) (sdk.ConnectionAccountWriteResult, error) {
	p.calls.Add(1)
	return sdk.ConnectionAccountWriteResult{}, nil
}

func TestAccountWriteServiceHTTPRequiresHostAuthenticationAndBoundedSingleJSON(t *testing.T) {
	port := &accountWriteHTTPPort{}
	h := &handler{token: "host-service-token", mux: http.NewServeMux(), accountWrites: port}
	h.registerAccountWrites()
	server := httptest.NewServer(h)
	defer server.Close()
	for _, test := range []struct {
		name, body, token, runtime string
		status                     int
	}{
		{"unauthenticated", `{}`, "", "", 401},
		{"browser-token", `{}`, "browser-token", "runtime", 401},
		{"missing-runtime", `{}`, "host-service-token", "", 401},
		{"forged-authority", `{"scopes":["send"]}`, "host-service-token", "runtime", 400},
		{"trailing-json", `{} {}`, "host-service-token", "runtime", 400},
		{"oversized", `{"request_id":"` + strings.Repeat("x", (2<<20)+1) + `"}`, "host-service-token", "runtime", 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, action := range []string{"write-access", "write", "write-receipt"} {
				r, err := http.NewRequest("POST", server.URL+"/integration/v1/connection-accounts/account/"+action, strings.NewReader(test.body))
				if err != nil {
					t.Fatal(err)
				}
				r.Header.Set("Authorization", "Bearer "+test.token)
				r.Header.Set("X-Domainry-Runtime-ID", test.runtime)
				response, err := server.Client().Do(r)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || response.StatusCode != test.status || port.calls.Load() != 0 || !json.Valid(body) {
					t.Fatal(action, response.StatusCode, string(body), err, port.calls.Load())
				}
			}
		})
	}
}
