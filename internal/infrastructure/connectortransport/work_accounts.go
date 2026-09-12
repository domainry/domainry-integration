// Package connectortransport implements outbound I/O owned by the standalone host.
package connectortransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const maximumBytes int64 = 4 << 20

type WorkAccounts struct{ client *http.Client }

func NewWorkAccounts() *WorkAccounts {
	return &WorkAccounts{client: &http.Client{
		Timeout:       35 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func allowedURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" || (u.Port() != "" && u.Port() != "443") {
		return nil, errors.New("Integration work-account endpoint is not allowed")
	}
	switch u.Hostname() {
	case "www.googleapis.com", "gmail.googleapis.com", "pubsub.googleapis.com", "oauth2.googleapis.com", "login.microsoftonline.com", "graph.microsoft.com":
		return u, nil
	default:
		return nil, errors.New("Integration work-account endpoint is not allowed")
	}
}

func (*WorkAccounts) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("Integration work-account transport does not support SQL")
}

func (t *WorkAccounts) RoundTripHTTP(ctx context.Context, input connector.HTTPRequest) (connector.HTTPResponse, error) {
	if err := ctx.Err(); err != nil {
		return connector.HTTPResponse{}, err
	}
	u, err := allowedURL(input.URL)
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	invalid := errors.New("Integration work-account request is invalid")
	if int64(len(input.Body)) > maximumBytes || len(input.SecretForm) > 0 && len(input.SecretJSON) > 0 {
		return connector.HTTPResponse{}, invalid
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return connector.HTTPResponse{}, invalid
	}
	for key, value := range input.SecretQuery {
		if query.Has(key) {
			return connector.HTTPResponse{}, invalid
		}
		query.Set(key, value)
	}
	u.RawQuery = query.Encode()
	headers := make(http.Header)
	for key, values := range input.Headers {
		for _, value := range values {
			headers.Add(key, value)
		}
	}
	for key, values := range input.SecretHeaders {
		if _, exists := headers[http.CanonicalHeaderKey(key)]; exists {
			return connector.HTTPResponse{}, invalid
		}
		for _, value := range values {
			headers.Add(key, value)
		}
	}
	body := input.Body
	if len(input.SecretForm) > 0 {
		if headers.Get("Content-Type") != "application/x-www-form-urlencoded" {
			return connector.HTTPResponse{}, invalid
		}
		form, err := url.ParseQuery(string(body))
		if err != nil {
			return connector.HTTPResponse{}, invalid
		}
		for key, value := range input.SecretForm {
			if form.Has(key) {
				return connector.HTTPResponse{}, invalid
			}
			form.Set(key, value)
		}
		body = []byte(form.Encode())
	}
	if len(input.SecretJSON) > 0 {
		if headers.Get("Content-Type") != "application/json" {
			return connector.HTTPResponse{}, invalid
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(body, &object); err != nil || object == nil {
			return connector.HTTPResponse{}, invalid
		}
		for key, value := range input.SecretJSON {
			if _, exists := object[key]; exists {
				return connector.HTTPResponse{}, invalid
			}
			object[key], _ = json.Marshal(value)
		}
		body, err = json.Marshal(object)
		if err != nil {
			return connector.HTTPResponse{}, invalid
		}
	}
	if int64(len(body)) > maximumBytes {
		return connector.HTTPResponse{}, invalid
	}
	request, err := http.NewRequestWithContext(ctx, input.Method, u.String(), bytes.NewReader(body))
	if err != nil {
		return connector.HTTPResponse{}, invalid
	}
	request.Header = headers
	// Authorization code exchanges must never be replayed by a redirect or retry.
	request.GetBody = nil
	response, err := t.client.Do(request)
	if err != nil {
		return connector.HTTPResponse{}, errors.New("Integration work-account dispatch failed")
	}
	defer response.Body.Close()
	limit := input.MaxResponseBytes
	if limit <= 0 || limit > maximumBytes {
		limit = maximumBytes
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || int64(len(data)) > limit {
		return connector.HTTPResponse{}, errors.New("Integration work-account response could not be read within its limit")
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		return connector.HTTPResponse{}, errors.New("Integration work-account redirects are not allowed")
	}
	return connector.HTTPResponse{StatusCode: response.StatusCode, Headers: response.Header.Clone(), Body: data}, nil
}

var _ connector.Transport = (*WorkAccounts)(nil)
