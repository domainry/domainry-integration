package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	kb "github.com/domainry/domainry-connectors/providers/knowledge_base/http_api"
	sdk "github.com/domainry/domainry-integration-sdk"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type contentTransport struct {
	calls   atomic.Int32
	body    []byte
	during  func()
	started chan struct{}
	release chan struct{}
	fail    bool
}

func (tr *contentTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("SQL not allowed")
}

func (tr *contentTransport) RoundTripHTTP(ctx context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	n := tr.calls.Add(1)
	u, err := url.Parse(r.URL)
	if err != nil || r.Method != "POST" || u.Path != "/v1/kb/kbs/kb-support/documents" || u.Query().Get("doc_id") != "verified-solution" || u.Query().Get("filename") != "verified-solution.md" || r.SecretHeaders["Authorization"][0] != "Bearer fixture-private-key" || r.Headers["Content-Type"][0] != "application/octet-stream" || r.Content == nil || r.ContentLength != int64(len(tr.body)) {
		return connector.HTTPResponse{}, errors.New("official document scope or host content envelope changed")
	}
	body, err := io.ReadAll(r.Content)
	digest := sha256.Sum256(body)
	if err != nil || !bytes.Equal(body, tr.body) || hex.EncodeToString(digest[:]) != r.ContentSHA256 {
		return connector.HTTPResponse{}, errors.New("original content bytes changed")
	}
	if tr.during != nil {
		tr.during()
	}
	if tr.started != nil && n == 1 {
		close(tr.started)
	}
	if tr.release != nil {
		select {
		case <-tr.release:
		case <-ctx.Done():
			return connector.HTTPResponse{}, ctx.Err()
		}
	}
	if tr.fail {
		return connector.HTTPResponse{}, context.DeadlineExceeded
	}
	return connector.HTTPResponse{StatusCode: 202, Body: []byte(`{"err_code":0,"data":{"doc_id":"verified-solution","status":"PENDING","private":"PRIVATE-ACK"}}`)}, nil
}

func contentFixture(t *testing.T) (*ManagementStore, *OperationsStore, *contentTransport, model.ProviderCallRequest) {
	t.Helper()
	store, _ := setupConnectionAccountStore(t)
	body := []byte("# Verified solution\nPRIVATE-KB-CONTENT\nOnly deidentified, verified guidance.")
	tr := &contentTransport{body: body}
	provider, err := kb.New(tr)
	if err != nil {
		t.Fatal(err)
	}
	store.delivery.providers = deliveryTestProviders{provider}
	ctx := accountScope(t.Context(), "admin", true)
	if _, err := store.UpsertSecret(ctx, "workspace-a", "kb-key", "admin", sdk.SecretInput{Kind: "api_key", Value: "fixture-private-key"}); err != nil {
		t.Fatal(err)
	}
	c, err := store.UpsertConnection(ctx, "workspace-a", "team-kb", "admin", sdk.ConnectionInput{ConnectorKey: "knowledge_base", ProviderKey: "http_api", Name: "Shared KB", Status: "active", Config: map[string]any{"base_url": "https://api.example.test", "team_id": "team-a", "kb_id": "kb-support"}, SecretRefs: map[string]string{"api_key": "secret:kb-key"}})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	hash := hex.EncodeToString(digest[:])
	payload, _ := json.Marshal(kb.PutDocumentInput{DocID: "verified-solution", Filename: "verified-solution.md", Bytes: int64(len(body)), SHA256: hash})
	request := model.ProviderCallRequest{RequestID: "publish-a", WorkspaceID: "workspace-a", ActorID: "admin", RoleKey: "administrator", ConnectorKey: "knowledge_base", ConnectionKey: "team-kb", Operation: kb.PutDocument.Key, Source: model.InvocationSource{ExecutionID: "publish-a", ObjectKey: "knowledge_article", RecordID: "article-a"}, Payload: payload, Content: &model.ProviderCallContent{Data: body, SHA256: hash, ContentType: "application/octet-stream"}, ExpectedConnection: &model.ProviderCallTarget{ConnectionUpdatedAt: c.UpdatedAt, ProviderKey: c.ProviderKey, ContractSHA256: kb.PutDocument.ContractSHA256}, PersistenceMode: model.ProviderCallPersistenceSensitive, MaskedDestination: "team-shared-kb"}
	ops := NewOperationsStore(store.transactions, store.dialect, store.delivery, nil, newTestDefinitionStore())
	current, err := store.delivery.connection(t.Context(), model.DeliveryRequest{WorkspaceID: "workspace-a", ConnectorKey: "knowledge_base", ConnectionKey: "team-kb"})
	if err != nil {
		t.Fatal(err)
	}
	request.ExpectedConnection.ConnectionSHA256 = providerConnectionSHA256(current)
	return store, ops, tr, request
}

func TestOfficialKBContentCallClaimsBeforeIOAndNeverPersistsBodies(t *testing.T) {
	store, ops, tr, request := contentFixture(t)
	tr.during = func() {
		id, _ := operationInvocationID(request.WorkspaceID, request.RequestID)
		r, err := ops.GetInvocation(t.Context(), request.WorkspaceID, id)
		if err != nil || r.Status != "running" || r.Metadata[guardedCallIdentityKey] != request.GuardedCallSHA256() {
			t.Fatal("write was not durably claimed before I/O", err)
		}
		raw, _ := json.Marshal(r)
		if bytes.Contains(raw, request.Content.Data) || bytes.Contains(raw, []byte("PRIVATE-KB-CONTENT")) {
			t.Fatal("body was transiently persisted")
		}
	}
	out, err := ops.Call(t.Context(), request)
	if err != nil || out.Invocation.Status != "succeeded" || len(out.Response) == 0 || tr.calls.Load() != 1 {
		t.Fatal("official content call failed", err, tr.calls.Load())
	}
	raw, _ := json.Marshal(out.Invocation)
	for _, private := range []string{"PRIVATE-KB-CONTENT", "PRIVATE-ACK", "fixture-private-key"} {
		if bytes.Contains(raw, []byte(private)) {
			t.Fatal("private content entered the persisted receipt")
		}
	}
	// A restarted owner sees the same insert-only claim, not a new attempt.
	reopened := NewOperationsStore(store.transactions, store.dialect, store.delivery, nil, newTestDefinitionStore())
	replay, err := reopened.Call(t.Context(), request)
	if err != nil || replay.Invocation.InvocationID != out.Invocation.InvocationID || len(replay.Response) != 0 || tr.calls.Load() != 1 {
		t.Fatal("success replay repeated upload or exposed old body", err)
	}
}

func TestGuardedKBContentIdentityCannotBeReusedForChangedInput(t *testing.T) {
	_, ops, tr, original := contentFixture(t)
	if _, err := ops.Call(t.Context(), original); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*model.ProviderCallRequest){
		"actor":     func(r *model.ProviderCallRequest) { r.ActorID = "other" },
		"role":      func(r *model.ProviderCallRequest) { r.RoleKey = "other" },
		"source":    func(r *model.ProviderCallRequest) { r.Source.RecordID = "other" },
		"operation": func(r *model.ProviderCallRequest) { r.Operation = "delete_document" },
		"metadata":  func(r *model.ProviderCallRequest) { r.Payload = json.RawMessage(`{"doc_id":"other"}`) },
		"target":    func(r *model.ProviderCallRequest) { r.ExpectedConnection.ConnectionUpdatedAt = "other" },
		"bytes": func(r *model.ProviderCallRequest) {
			r.Content.Data = []byte("another body")
			d := sha256.Sum256(r.Content.Data)
			r.Content.SHA256 = hex.EncodeToString(d[:])
		},
		"unguarded": func(r *model.ProviderCallRequest) { r.Content = nil; r.ExpectedConnection = nil },
	} {
		t.Run(name, func(t *testing.T) {
			r := freezeProviderContentRequest(original)
			mutate(&r)
			if _, err := ops.Call(t.Context(), r); err == nil || tr.calls.Load() != 1 {
				t.Fatal("changed logical effect reused a published identity", err)
			}
		})
	}
}

func TestKBContentTimeoutAndConcurrentReplayNeverUploadTwice(t *testing.T) {
	t.Run("lost response and owner restart", func(t *testing.T) {
		store, ops, tr, r := contentFixture(t)
		tr.fail = true
		if _, err := ops.Call(t.Context(), r); err == nil {
			t.Fatal("fixture timeout disappeared")
		}
		reopened := NewOperationsStore(store.transactions, store.dialect, store.delivery, nil, newTestDefinitionStore())
		if _, err := reopened.Call(t.Context(), r); err == nil || tr.calls.Load() != 1 {
			t.Fatal("timeout was treated as permission to resend", err)
		}
	})
	t.Run("in flight", func(t *testing.T) {
		_, ops, tr, r := contentFixture(t)
		tr.started = make(chan struct{})
		tr.release = make(chan struct{})
		done := make(chan error, 1)
		go func() { _, err := ops.Call(t.Context(), r); done <- err }()
		select {
		case <-tr.started:
		case <-time.After(3 * time.Second):
			t.Fatal("first upload did not begin")
		}
		_, duplicateErr := ops.Call(t.Context(), r)
		close(tr.release)
		if err := <-done; err != nil || duplicateErr == nil || tr.calls.Load() != 1 {
			t.Fatal("concurrent claim uploaded twice", err, duplicateErr)
		}
	})
}

type contentSecretHook struct {
	base  SecretReferenceResolver
	after func()
}

func (h contentSecretHook) ResolveSecretReferences(ctx context.Context, w string, refs map[string]string) (map[string]string, error) {
	v, err := h.base.ResolveSecretReferences(ctx, w, refs)
	if err == nil {
		h.after()
	}
	return v, err
}

func TestKBContentRejectsWrongTargetAndChangesDuringSecretResolution(t *testing.T) {
	for _, kind := range []string{"revision", "provider", "contract", "workspace", "inactive", "invalid-body", "after-secret-resolution", "same-revision-scope"} {
		t.Run(kind, func(t *testing.T) {
			store, ops, tr, r := contentFixture(t)
			switch kind {
			case "revision":
				r.ExpectedConnection.ConnectionUpdatedAt = "stale"
			case "provider":
				r.ExpectedConnection.ProviderKey = "other"
			case "contract":
				r.ExpectedConnection.ContractSHA256 = strings.Repeat("a", 64)
			case "workspace":
				r.WorkspaceID = "workspace-b"
			case "invalid-body":
				r.Content.SHA256 = strings.Repeat("a", 64)
			case "inactive":
				_, err := store.database.ExecContext(t.Context(), "UPDATE _integration_connections SET status='inactive' WHERE connection_key='team-kb'")
				if err != nil {
					t.Fatal(err)
				}
			case "after-secret-resolution", "same-revision-scope":
				store.delivery.secrets = contentSecretHook{base: store.delivery.secrets, after: func() {
					statement := "UPDATE _integration_connections SET config_json=? WHERE connection_key='team-kb'"
					if kind == "after-secret-resolution" {
						statement = "UPDATE _integration_connections SET updated_at=updated_at+1,config_json=? WHERE connection_key='team-kb'"
					}
					_, err := store.database.ExecContext(t.Context(), statement, `{"base_url":"https://api.example.test","team_id":"team-b","kb_id":"other"}`)
					if err != nil {
						t.Fatal(err)
					}
				}}
			}
			if _, err := ops.Call(t.Context(), r); err == nil || tr.calls.Load() != 0 {
				t.Fatal("wrong or changed KB target reached the provider", err)
			}
		})
	}
}
