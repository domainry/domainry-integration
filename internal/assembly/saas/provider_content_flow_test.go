package saas_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	kb "github.com/domainry/domainry-connectors/providers/knowledge_base/http_api"
	sdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-integration-sdk/remote"
	"github.com/domainry/domainry-integration-sdk/saashost"
	saasassembly "github.com/domainry/domainry-integration/internal/assembly/saas"
	integrationmodule "github.com/domainry/domainry-integration/module"
)

type contentFlowRegistry struct{ adapter connector.Adapter }

func (r contentFlowRegistry) Provider(c, p string) (connector.Adapter, bool) {
	d := r.adapter.Descriptor()
	return r.adapter, c == d.ConnectorKey && p == d.ProviderKey
}
func (r contentFlowRegistry) Descriptors() []connector.ProviderDescriptor {
	return []connector.ProviderDescriptor{r.adapter.Descriptor()}
}

type contentFlowHost struct {
	*publicFlowHost
	registry contentFlowRegistry
}

func (h *contentFlowHost) Providers() modulehost.ProviderRegistry { return h.registry }

type contentFlowTransport struct {
	body  []byte
	calls atomic.Int32
}

func (*contentFlowTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("SQL not allowed")
}
func (tr *contentFlowTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	tr.calls.Add(1)
	if r.Content == nil || r.ContentLength != int64(len(tr.body)) || r.SecretHeaders["Authorization"][0] != "Bearer content-fixture-key" {
		return connector.HTTPResponse{}, errors.New("host content or credential missing")
	}
	body, err := io.ReadAll(r.Content)
	if err != nil || !bytes.Equal(body, tr.body) {
		return connector.HTTPResponse{}, errors.New("content changed")
	}
	return connector.HTTPResponse{StatusCode: 202, Body: []byte(`{"err_code":0,"data":{"doc_id":"verified-solution","status":"PENDING"}}`)}, nil
}

func TestPublicContentCallPreservesModuleAndSaaSOfficialKBContracts(t *testing.T) {
	for _, topology := range []string{"module", "saas"} {
		t.Run(topology, func(t *testing.T) {
			body := bytes.Repeat([]byte("# Small verified guidance\n"), 100)
			tr := &contentFlowTransport{body: body}
			adapter, err := kb.New(tr)
			if err != nil {
				t.Fatal(err)
			}
			host := &contentFlowHost{publicFlowHost: newPublicFlowHost(t, "content-"+topology), registry: contentFlowRegistry{adapter}}
			defer host.close()
			var binding sdk.Binding
			if topology == "module" {
				binding, err = integrationmodule.NewFactory().OpenModule(t.Context(), sdk.ApplicationRef{RuntimeID: "content-flow"}, host)
			} else {
				service, openErr := saasassembly.Open(t.Context(), sdk.ApplicationRef{RuntimeID: "content-flow"}, host, "service-token")
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer service.Close(context.Background())
				server := httptest.NewServer(service.Handler)
				defer server.Close()
				factory := integrationmodule.NewSaaSFactory(remote.NewFactory(remote.Options{BaseURL: server.URL, Token: "service-token", HTTPClient: server.Client()})).(saashost.Factory)
				binding, err = factory.OpenSaaS(t.Context(), sdk.ApplicationRef{RuntimeID: "content-flow"}, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer binding.Close(context.Background())
			management := binding.(sdk.ManagementBinding).Management()
			operations := binding.(sdk.OperationsBinding).Operations()
			if _, err = management.UpsertSecret(t.Context(), "workspace-a", "content-key", "author", sdk.SecretInput{Kind: "api_key", Value: "content-fixture-key"}); err != nil {
				t.Fatal(err)
			}
			if _, err = management.UpsertConnection(t.Context(), "workspace-a", "shared-kb", "author", sdk.ConnectionInput{ConnectorKey: "knowledge_base", ProviderKey: "http_api", Name: "Shared KB", Status: "active", Config: map[string]any{"base_url": "https://api.example.test", "team_id": "team-a", "kb_id": "kb-support"}, SecretRefs: map[string]string{"api_key": "secret:content-key"}}); err != nil {
				t.Fatal(err)
			}
			current, err := management.GetConnection(t.Context(), "workspace-a", "shared-kb")
			if err != nil {
				t.Fatal(err)
			}
			target, err := sdk.NewProviderCallTarget(current, kb.PutDocument.Descriptor())
			if err != nil {
				t.Fatal(err)
			}
			content, err := sdk.NewProviderCallContent(body, "application/octet-stream")
			if err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(kb.PutDocumentInput{DocID: "verified-solution", Filename: "verified-solution.md", Bytes: int64(len(body)), SHA256: content.SHA256})
			request := sdk.ProviderCallRequest{RequestID: "publication-a", WorkspaceID: "workspace-a", ActorID: "author", RoleKey: "publisher", ConnectorKey: "knowledge_base", ConnectionKey: "shared-kb", Operation: kb.PutDocument.Key, Source: sdk.InvocationSource{ExecutionID: "publication-a", ObjectKey: "knowledge_article", RecordID: "article-a"}, Payload: payload, Content: content, ExpectedConnection: target, PersistenceMode: sdk.ProviderCallPersistenceSensitive, MaskedDestination: "shared-kb"}
			out, err := operations.Call(t.Context(), request)
			if err != nil || out.Invocation.Status != "succeeded" || out.Invocation.Metadata["guarded_call_sha256"] != request.GuardedCallSHA256() || len(out.Response) == 0 || tr.calls.Load() != 1 {
				t.Fatal("public content bridge failed", err)
			}
			replay, err := operations.Call(t.Context(), request)
			if err != nil || replay.Invocation.InvocationID != out.Invocation.InvocationID || len(replay.Response) != 0 || tr.calls.Load() != 1 {
				t.Fatal("public replay repeated I/O or returned old content", err)
			}
			request.Source.RecordID = "foreign-article"
			if _, err = operations.Call(t.Context(), request); err == nil || tr.calls.Load() != 1 {
				t.Fatal("changed source reused the original write")
			}
		})
	}
}
