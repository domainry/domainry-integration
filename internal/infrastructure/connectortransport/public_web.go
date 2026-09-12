package connectortransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	connector "github.com/domainry/domainry-connector-sdk"
)

// PublicWeb owns the deployment's proxy network policy. It never fetches a
// model-provided source URL itself. DNS, rendering and redirects performed by
// the remote page reader remain that service's responsibility.
type PublicWeb struct {
	origin string
	client *http.Client
}

func NewPublicWeb(origin string) (*PublicWeb, error) {
	invalid := errors.New("Integration public-web service origin is invalid")
	if origin == "" || len(origin) > 2048 || strings.ContainsAny(origin, "\\#") || strings.ContainsFunc(origin, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return nil, invalid
	}
	u, err := url.Parse(origin)
	if err != nil || u.Hostname() == "" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || strings.HasSuffix(u.Host, ":") {
		return nil, invalid
	}
	loopback := u.Hostname() == "127.0.0.1" || u.Hostname() == "::1" || u.Hostname() == "localhost"
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return nil, invalid
	}
	if u.Port() != "" {
		port, e := strconv.Atoi(u.Port())
		if e != nil || port < 1 || port > 65535 {
			return nil, invalid
		}
	}
	return &PublicWeb{origin: strings.TrimSuffix(u.String(), "/"), client: &http.Client{Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (*PublicWeb) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("Integration public-web transport does not support SQL")
}

func (t *PublicWeb) RoundTripHTTP(ctx context.Context, input connector.HTTPRequest) (connector.HTTPResponse, error) {
	if err := ctx.Err(); err != nil {
		return connector.HTTPResponse{}, err
	}
	invalid := errors.New("Integration public-web request is not allowed")
	if input.Method != http.MethodPost || (input.URL != t.origin+"/tool/web_search" && input.URL != t.origin+"/tool/web_fetch_jina") || len(input.Body) > 16<<10 || !json.Valid(input.Body) || len(input.SecretForm)+len(input.SecretJSON)+len(input.SecretQuery) > 0 {
		return connector.HTTPResponse{}, invalid
	}
	headers := make(http.Header)
	for key, values := range input.Headers {
		canonical := http.CanonicalHeaderKey(key)
		if (canonical != "Accept" && canonical != "Content-Type") || len(values) != 1 || values[0] != "application/json" || headers.Get(canonical) != "" {
			return connector.HTTPResponse{}, invalid
		}
		headers.Set(canonical, values[0])
	}
	if headers.Get("Accept") == "" || headers.Get("Content-Type") == "" || len(input.SecretHeaders) != 1 {
		return connector.HTTPResponse{}, invalid
	}
	for key, values := range input.SecretHeaders {
		if http.CanonicalHeaderKey(key) != "Authorization" || len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
			return connector.HTTPResponse{}, invalid
		}
		token := strings.TrimPrefix(values[0], "Bearer ")
		if token == "" || len(token) > 16<<10 || strings.ContainsFunc(token, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			return connector.HTTPResponse{}, invalid
		}
		headers.Set("Authorization", values[0])
	}
	request, err := http.NewRequestWithContext(ctx, input.Method, input.URL, bytes.NewReader(input.Body))
	if err != nil {
		return connector.HTTPResponse{}, invalid
	}
	request.Header = headers
	request.GetBody = nil // Never replay a possibly billable POST.
	response, err := t.client.Do(request)
	if err != nil {
		return connector.HTTPResponse{}, errors.New("Integration public-web dispatch failed")
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		return connector.HTTPResponse{}, errors.New("Integration public-web redirects are not allowed")
	}
	limit := input.MaxResponseBytes
	if limit <= 0 || limit > maximumBytes {
		limit = maximumBytes
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || int64(len(body)) > limit {
		return connector.HTTPResponse{}, errors.New("Integration public-web response exceeds its read limit")
	}
	// Upstream response headers are not required by either wire contract.
	return connector.HTTPResponse{StatusCode: response.StatusCode, Body: body}, nil
}

var _ connector.Transport = (*PublicWeb)(nil)
