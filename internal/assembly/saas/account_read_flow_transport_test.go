package saas

import (
	"bytes"
	"context"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type accountReadFlowTransport struct {
	base   string
	client *http.Client
}

func (t accountReadFlowTransport) RoundTripHTTP(ctx context.Context, in connector.HTTPRequest) (connector.HTTPResponse, error) {
	if !strings.HasPrefix(in.URL, t.base+"/") {
		return connector.HTTPResponse{}, errors.New("test transport denied destination")
	}
	body := in.Body
	if len(in.SecretForm) > 0 {
		form, err := url.ParseQuery(string(body))
		if err != nil {
			return connector.HTTPResponse{}, err
		}
		for k, v := range in.SecretForm {
			if form.Has(k) {
				return connector.HTTPResponse{}, errors.New("duplicate private field")
			}
			form.Set(k, v)
		}
		body = []byte(form.Encode())
	}
	r, err := http.NewRequestWithContext(ctx, in.Method, in.URL, bytes.NewReader(body))
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	for k, v := range in.Headers {
		r.Header[k] = v
	}
	for k, v := range in.SecretHeaders {
		r.Header[k] = v
	}
	response, err := t.client.Do(r)
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	defer response.Body.Close()
	limit := in.MaxResponseBytes
	if limit <= 0 {
		limit = 1 << 20
	} // zero requests the host's bounded default
	b, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if int64(len(b)) > limit {
		return connector.HTTPResponse{}, errors.New("response limit")
	}
	return connector.HTTPResponse{StatusCode: response.StatusCode, Headers: response.Header, Body: b}, err
}
func (accountReadFlowTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
