package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	app "github.com/domainry/domainry-integration/internal/application/integration"
)

type sensitiveClaimProvider struct {
	*accountReadProvider
	count   atomic.Int32
	started chan struct{}
	release chan struct{}
	fail    bool
}

func (p *sensitiveClaimProvider) Call(ctx context.Context, _ connector.CallRequest) (connector.CallResult, error) {
	n := p.count.Add(1)
	if p.started != nil && n == 1 {
		close(p.started)
	}
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return connector.CallResult{}, ctx.Err()
		}
	}
	if p.fail {
		return connector.CallResult{}, connector.UncertainError("fixture.response_lost", errors.New("response lost after dispatch"))
	}
	return connector.CallResult{Payload: json.RawMessage(`{"value":"private-web-result"}`)}, nil
}

func TestSensitiveAccountCallNeverReclaimsFailedOrRunningRequest(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		store, ops, base, _, subject, request := accountReadFixture(t)
		provider := &sensitiveClaimProvider{accountReadProvider: base, fail: true}
		store.delivery.providers = deliveryTestProviders{provider}
		service := app.NewAccountReadService(store, ops)
		for i := 0; i < 2; i++ {
			if _, err := service.ReadConnectionAccount(t.Context(), subject, "shared", request); err == nil {
				t.Fatal("uncertain result succeeded")
			}
		}
		if provider.count.Load() != 1 {
			t.Fatalf("failed sensitive identity dispatched %d times", provider.count.Load())
		}
		// A new explicit request is still possible after current authorization.
		request.RequestID = "new-attempt"
		if _, err := service.ReadConnectionAccount(t.Context(), subject, "shared", request); err == nil {
			t.Fatal("fixture failure disappeared")
		}
		if provider.count.Load() != 2 {
			t.Fatal("new explicit request did not dispatch")
		}
	})
	t.Run("running", func(t *testing.T) {
		store, ops, base, _, subject, request := accountReadFixture(t)
		provider := &sensitiveClaimProvider{accountReadProvider: base, started: make(chan struct{}), release: make(chan struct{})}
		store.delivery.providers = deliveryTestProviders{provider}
		service := app.NewAccountReadService(store, ops)
		first := make(chan error, 1)
		go func() { _, err := service.ReadConnectionAccount(t.Context(), subject, "shared", request); first <- err }()
		select {
		case <-provider.started:
		case <-time.After(3 * time.Second):
			t.Fatal("first request not dispatched")
		}
		second := make(chan error, 1)
		go func() {
			_, err := service.ReadConnectionAccount(t.Context(), subject, "shared", request)
			second <- err
		}()
		var secondErr error
		select {
		case secondErr = <-second:
		case <-time.After(time.Second):
		}
		close(provider.release)
		if err := <-first; err != nil {
			t.Fatal(err)
		}
		if provider.count.Load() != 1 {
			t.Fatalf("running sensitive identity dispatched %d times", provider.count.Load())
		}
		if secondErr == nil {
			t.Fatal("duplicate running request was not rejected promptly")
		}
		replay, err := service.ReadConnectionAccount(t.Context(), subject, "shared", request)
		if err != nil || replay.PayloadAvailable || provider.count.Load() != 1 {
			t.Fatal("successful sensitive replay changed", err)
		}
	})
}
