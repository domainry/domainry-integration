package module

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

type webhookOperationsStub struct {
	received integrationsdk.WebhookRequest
}

func (*webhookOperationsStub) Call(context.Context, integrationsdk.ProviderCallRequest) (integrationsdk.ProviderCallResult, error) {
	return integrationsdk.ProviderCallResult{}, nil
}
func (*webhookOperationsStub) ListInvocations(context.Context, integrationsdk.InvocationQuery) ([]integrationsdk.Invocation, error) {
	return nil, nil
}
func (*webhookOperationsStub) GetInvocation(context.Context, string, string) (integrationsdk.Invocation, error) {
	return integrationsdk.Invocation{}, nil
}
func (stub *webhookOperationsStub) AcceptWebhook(_ context.Context, request integrationsdk.WebhookRequest) (integrationsdk.WebhookReceipt, error) {
	stub.received = request
	return integrationsdk.WebhookReceipt{Event: integrationsdk.Event{EventID: "event-1", Status: "ignored"}}, nil
}
func (*webhookOperationsStub) ListEvents(context.Context, integrationsdk.EventQuery) ([]integrationsdk.Event, error) {
	return nil, nil
}
func (*webhookOperationsStub) GetEvent(context.Context, string, string) (integrationsdk.Event, error) {
	return integrationsdk.Event{}, nil
}
func (*webhookOperationsStub) ReplayEvent(context.Context, string, string) (integrationsdk.Event, error) {
	return integrationsdk.Event{}, nil
}

func TestWebhookReturnsProviderCompatibleSuccessStatus(t *testing.T) {
	operations := &webhookOperationsStub{}
	h := &handler{operations: operations}
	request := httptest.NewRequest(http.MethodPost, "/integration/webhooks/workspace-a/payment/wechat_payment", strings.NewReader(`{"id":"notification-1"}`))
	request.SetPathValue("workspaceID", "workspace-a")
	request.SetPathValue("connectorKey", "payment")
	request.SetPathValue("connectionKey", "wechat_payment")
	recorder := httptest.NewRecorder()

	h.webhook(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("webhook status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if operations.received.WorkspaceID != "workspace-a" || operations.received.ConnectorKey != "payment" || operations.received.ConnectionKey != "wechat_payment" {
		t.Fatalf("webhook route identity = %#v", operations.received)
	}
	if string(operations.received.Body) != `{"id":"notification-1"}` || !strings.Contains(recorder.Body.String(), `"status":"ignored"`) {
		t.Fatalf("webhook request/response body = %q / %q", operations.received.Body, recorder.Body.String())
	}
}
