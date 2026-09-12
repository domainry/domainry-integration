package connectortransport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/web"
	connectormodule "github.com/domainry/domainry-connectors/module"
)

func webHTTPRequest(origin string) connector.HTTPRequest {
	return connector.HTTPRequest{Method: "POST", URL: origin + "/tool/web_search", Body: []byte(`{"objective":"news"}`), Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer private-service-token"}}, MaxResponseBytes: 4 << 20}
}

func TestPublicWebOriginAndRequestPolicy(t *testing.T) {
	for _, origin := range []string{"", "http://example.com", "https://u:p@example.com", "https://example.com/a", "https://example.com?", "https://example.com#", "https://example.com:", "https://example.com:70000", "https://example.com\\evil", " https://example.com"} {
		if _, err := NewPublicWeb(origin); err == nil {
			t.Fatal("origin accepted", origin)
		}
	}
	for _, origin := range []string{"https://proxy.example.com", "https://proxy.example.com:9443/", "http://127.0.0.1:8080", "http://[::1]:8080"} {
		if _, err := NewPublicWeb(origin); err != nil {
			t.Fatal(origin, err)
		}
	}
	transport, _ := NewPublicWeb("https://proxy.example.com")
	var calls int
	transport.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Header.Get("Authorization") != "Bearer private-service-token" || r.GetBody != nil {
			t.Error("private injection or replay protection missing")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Set-Cookie": {"private-cookie"}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})
	mutations := []func(*connector.HTTPRequest){
		func(r *connector.HTTPRequest) { r.URL = "https://evil.example.com/tool/web_search" },
		func(r *connector.HTTPRequest) { r.URL += "?url=secret" }, func(r *connector.HTTPRequest) { r.URL += "#x" },
		func(r *connector.HTTPRequest) { r.URL = "https://proxy.example.com/tool/../tool/web_search" },
		func(r *connector.HTTPRequest) { r.URL = "https://proxy.example.com/tool/%77eb_search" },
		func(r *connector.HTTPRequest) { r.URL = "https://proxy.example.com/llm/expense/parse" },
		func(r *connector.HTTPRequest) { r.Method = "GET" }, func(r *connector.HTTPRequest) { r.Headers["Cookie"] = []string{"private"} },
		func(r *connector.HTTPRequest) { r.Headers["Authorization"] = []string{"forged"} },
		func(r *connector.HTTPRequest) { r.Headers["accept"] = []string{"application/json"} },
		func(r *connector.HTTPRequest) { r.Headers["Content-Type"] = []string{"text/plain"} },
		func(r *connector.HTTPRequest) { r.SecretHeaders["Cookie"] = []string{"private"} },
		func(r *connector.HTTPRequest) { r.SecretHeaders["Authorization"] = []string{"Bearer secret\n"} },
		func(r *connector.HTTPRequest) { r.SecretHeaders = nil }, func(r *connector.HTTPRequest) { r.SecretQuery = map[string]string{"token": "secret"} },
		func(r *connector.HTTPRequest) { r.SecretJSON = map[string]string{"token": "secret"} }, func(r *connector.HTTPRequest) { r.SecretForm = map[string]string{"token": "secret"} },
		func(r *connector.HTTPRequest) { r.Body = []byte(strings.Repeat(" ", 16<<10) + `{}`) }, func(r *connector.HTTPRequest) { r.Body = []byte(`{`) },
	}
	for i, mutate := range mutations {
		r := webHTTPRequest(transport.origin)
		mutate(&r)
		if _, err := transport.RoundTripHTTP(t.Context(), r); err == nil || calls != 0 {
			t.Fatalf("mutation %d escaped policy", i)
		}
	}
	r := webHTTPRequest(transport.origin)
	before, _ := json.Marshal(r)
	got, err := transport.RoundTripHTTP(t.Context(), r)
	after, _ := json.Marshal(r)
	if err != nil || calls != 1 || got.StatusCode != 200 || len(got.Headers) != 0 || string(before) != string(after) || strings.Contains(string(after), "private-service") {
		t.Fatal("response/serialization boundary", err)
	}
	if _, err := transport.ExecuteSQL(t.Context(), connector.SQLRequest{}); err == nil {
		t.Fatal("SQL available")
	}
	// Google/Microsoft policy has not acquired the proxy origin.
	if _, err := allowedURL(r.URL); err == nil {
		t.Fatal("work-account policy widened")
	}
}

func TestPublicWebHTTPRedirectLimitsAndCancellation(t *testing.T) {
	var mode atomic.Int32
	var calls atomic.Int32
	started, stopped := make(chan struct{}), make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.Copy(io.Discard, r.Body)
		switch mode.Load() {
		case 0:
			w.Header().Set("Location", "/credential-leak")
			w.WriteHeader(307)
		case 1:
			io.WriteString(w, "oversized-private-body")
		case 2:
			close(started)
			<-r.Context().Done()
			close(stopped)
		}
	}))
	defer s.Close()
	transport, err := NewPublicWeb(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	r := webHTTPRequest(s.URL)
	if _, err := transport.RoundTripHTTP(t.Context(), r); err == nil || calls.Load() != 1 {
		t.Fatal("redirect followed", err)
	}
	mode.Store(1)
	r.MaxResponseBytes = 4
	if _, err := transport.RoundTripHTTP(t.Context(), r); err == nil || strings.Contains(err.Error(), "private-body") || calls.Load() != 2 {
		t.Fatal("response limit leaked", err)
	}
	mode.Store(2)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := transport.RoundTripHTTP(ctx, r); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("dispatch missing")
	}
	cancel()
	if err := <-done; err == nil || strings.Contains(err.Error(), "private-service") {
		t.Fatal("cancellation failed", err)
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not observe cancellation")
	}
	if _, err := transport.RoundTripHTTP(ctx, r); !errors.Is(err, context.Canceled) || calls.Load() != 3 {
		t.Fatal("pre-cancel dispatched", err)
	}
}

func TestPublicWebProviderOverProductionHostHTTP(t *testing.T) {
	var searches, fetches atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer private-service-token" {
			t.Error("protocol authorization missing")
		}
		b, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/tool/web_search":
			searches.Add(1)
			if !strings.Contains(string(b), `"objective":"current news"`) || !strings.Contains(string(b), `"sources":["example.com"]`) {
				t.Error("search translation missing")
			}
			io.WriteString(w, `{"search_id":"http-source-1","results":[{"url":"https://example.com/news","title":"今日","excerpts":["新信息"]},{"url":"https://unapproved.com/","excerpts":["filtered"]}]}`)
		case "/tool/web_fetch_jina":
			fetches.Add(1)
			if string(b) != `{"url":"https://example.com/news"}` {
				t.Error("fetch translation missing")
			}
			io.WriteString(w, `{"code":0,"data":{"url":"https://example.com/news","content":"页面正文","title":"今日"}}`)
		default:
			t.Error("unexpected route")
		}
	}))
	defer s.Close()
	transport, err := NewPublicWeb(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	set, err := connectormodule.PublicWebProviders(transport)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := connectormodule.NewFactory(connectormodule.Options{Providers: set}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	p, _ := registry.Provider("web", "llm_proxy")
	for _, op := range []string{web.SearchOperationKey, web.FetchOperationKey} {
		var input any = web.SearchRequest{Query: "current news"}
		if op == web.FetchOperationKey {
			input = web.FetchRequest{URL: "https://example.com/news#part"}
		}
		raw, _ := json.Marshal(input)
		request := connector.CallRequest{ConnectorKey: "web", ProviderKey: "llm_proxy", OperationKey: op, ContractSHA256: web.OperationSHA256(op), Mode: connector.ModeCall, Payload: raw, Connection: connector.Connection{Config: map[string]any{"base_url": s.URL, "allowed_source_hosts": []string{"example.com"}}}, Secrets: map[string]string{"api_token": "private-service-token"}}
		result, err := p.Call(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if op == web.SearchOperationKey {
			var out web.SearchResult
			if json.Unmarshal(result.Payload, &out) != nil || out.SearchID != "http-source-1" || len(out.Items) != 1 || !out.Truncated {
				t.Fatal("source search result changed")
			}
		} else {
			var out web.Page
			if json.Unmarshal(result.Payload, &out) != nil || out.Content != "页面正文" || out.SourceCompleteness != "unknown" {
				t.Fatal("page source changed")
			}
		}
		if strings.Contains(string(result.Payload), "private-service") || strings.Contains(string(result.Payload), "filtered") {
			t.Fatal("private/forbidden output leaked")
		}
	}
	if searches.Load() != 1 || fetches.Load() != 1 {
		t.Fatal("unexpected retries")
	}
}
