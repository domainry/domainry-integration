package connectortransport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWorkAccountsInjectsOnlyAtDispatch(t *testing.T) {
	transport := NewWorkAccounts()
	var count int
	transport.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		count++
		if request.URL.Host != "oauth2.googleapis.com" || request.Header.Get("Authorization") != "Bearer private" || request.GetBody != nil {
			t.Fatal("request not protected")
		}
		body, _ := io.ReadAll(request.Body)
		form, _ := url.ParseQuery(string(body))
		if form.Get("code") != "private-code" || form.Get("grant_type") != "authorization_code" {
			t.Fatal("secret form not injected")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
	})
	input := connector.HTTPRequest{Method: "POST", URL: "https://oauth2.googleapis.com/token", Headers: map[string][]string{"Content-Type": {"application/x-www-form-urlencoded"}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer private"}}, Body: []byte("grant_type=authorization_code"), SecretForm: map[string]string{"code": "private-code"}}
	response, err := transport.RoundTripHTTP(context.Background(), input)
	if err != nil || response.StatusCode != 200 || count != 1 {
		t.Fatalf("dispatch: %v", err)
	}
	if string(input.Body) != "grant_type=authorization_code" || input.Headers["Authorization"] != nil {
		t.Fatal("public request mutated")
	}
	input.Headers["authorization"] = []string{"forged"}
	if _, err := transport.RoundTripHTTP(context.Background(), input); err == nil || count != 1 {
		t.Fatal("header collision accepted")
	}
	delete(input.Headers, "authorization")
	input.Body = []byte("code=forged")
	if _, err := transport.RoundTripHTTP(context.Background(), input); err == nil || count != 1 {
		t.Fatal("form collision accepted")
	}
}

func TestWorkAccountsRejectsUnknownEndpointsAndBoundsResponses(t *testing.T) {
	transport := NewWorkAccounts()
	var count int
	transport.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		count++
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("oversized-private-body"))}, nil
	})
	for _, target := range []string{"http://oauth2.googleapis.com/token", "https://127.0.0.1/token", "https://oauth2.googleapis.com.evil.test/token", "https://oauth2.googleapis.com:444/token", "https://secret@oauth2.googleapis.com/token", "https://oauth2.googleapis.com/token#fragment"} {
		if _, err := transport.RoundTripHTTP(context.Background(), connector.HTTPRequest{URL: target}); err == nil || count != 0 {
			t.Fatalf("endpoint accepted: %s", target)
		}
	}
	_, err := transport.RoundTripHTTP(context.Background(), connector.HTTPRequest{URL: "https://graph.microsoft.com/v1.0/me", MaxResponseBytes: 4})
	if err == nil || strings.Contains(err.Error(), "private-body") || count != 1 {
		t.Fatal("oversized response exposed")
	}
	transport.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) { count++; return nil, errors.New("private network error") })
	_, err = transport.RoundTripHTTP(context.Background(), connector.HTTPRequest{URL: "https://oauth2.googleapis.com/token", Method: "POST"})
	if err == nil || strings.Contains(err.Error(), "private") || count != 2 {
		t.Fatal("dispatch retried or error leaked")
	}
}

func TestWorkAccountsDoesNotFollowRedirects(t *testing.T) {
	var calls int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Location", "https://oauth2.googleapis.com/forward")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	transport := NewWorkAccounts()
	// Only this test redirects sockets to the isolated TLS server. Production URL
	// policy and CheckRedirect are unchanged, and no external host is contacted.
	local := server.Client().Transport.(*http.Transport).Clone()
	local.TLSClientConfig.ServerName = "example.com"
	local.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	transport.client.Transport = local
	defer local.CloseIdleConnections()
	_, err := transport.RoundTripHTTP(context.Background(), connector.HTTPRequest{Method: "POST", URL: "https://oauth2.googleapis.com/token", SecretForm: map[string]string{"code": "private"}, Headers: map[string][]string{"Content-Type": {"application/x-www-form-urlencoded"}}})
	if err == nil || calls != 1 {
		t.Fatalf("redirect replayed: %d %v", calls, err)
	}
}
