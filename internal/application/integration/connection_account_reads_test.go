package integrationapplication

import (
	"context"
	"errors"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type accountReadAuthorityStub struct {
	access model.ConnectionAccountReadAccess
}

func (s accountReadAuthorityStub) AuthorizeConnectionAccountRead(context.Context, model.ConnectionAccountSubject, string, model.ConnectionAccountReadOperation) (model.ConnectionAccountReadAccess, error) {
	return s.access, nil
}

type accountReadCallsStub struct{ err error }

func (s accountReadCallsStub) Call(context.Context, model.ProviderCallRequest) (model.ProviderCallResult, error) {
	return model.ProviderCallResult{}, s.err
}

func TestAccountReadPreservesOnlyValidatedProviderFailureContract(t *testing.T) {
	const contract = "574d2b2142e2bb2438d348f111ae843ed07e7de7f6a6b0cfc7852fb0b39bf33d"
	access := model.ConnectionAccountReadAccess{Source: model.ConnectionAccountReadSource{
		WorkspaceID: "workspace-a", ConnectionKey: "connection-a", ConnectorKey: "appointment_scheduling", ProviderKey: "feishu_calendar",
		AccountUpdatedAt: "2026-09-29T14:34:47Z", Operation: "fetch_meeting_content", ContractSHA256: contract,
	}}
	service := NewAccountReadService(accountReadAuthorityStub{access: access}, accountReadCallsStub{
		err: connector.RetryableError("feishu_calendar.http_error", errors.New("provider body with customer data")),
	})
	_, err := service.ReadConnectionAccount(t.Context(), model.ConnectionAccountSubject{
		WorkspaceID: "workspace-a", UserID: "user-a", Access: model.ConnectionAccountAccess{Personal: true},
	}, "connection-a", model.ConnectionAccountReadRequest{
		RequestID: "request-a", Operation: "fetch_meeting_content", ContractSHA256: contract, Payload: []byte(`{"meeting_no":"123456789"}`),
	})
	classification, classified := connector.ErrorClassificationOf(err)
	code, coded := connector.ProviderErrorCodeOf(err)
	if !classified || classification != connector.ErrorRetryable || !coded || code != "feishu_calendar.http_error" || err.Error() != code || errors.Unwrap(err) != nil {
		t.Fatalf("classification=%q classified=%v code=%q coded=%v error=%v cause=%v", classification, classified, code, coded, err, errors.Unwrap(err))
	}
}

func TestAccountReadStillRedactsUnclassifiedFailures(t *testing.T) {
	const contract = "574d2b2142e2bb2438d348f111ae843ed07e7de7f6a6b0cfc7852fb0b39bf33d"
	access := model.ConnectionAccountReadAccess{Source: model.ConnectionAccountReadSource{
		WorkspaceID: "workspace-a", ConnectionKey: "connection-a", ConnectorKey: "appointment_scheduling", ProviderKey: "feishu_calendar",
		AccountUpdatedAt: "2026-09-29T14:34:47Z", Operation: "fetch_meeting_content", ContractSHA256: contract,
	}}
	service := NewAccountReadService(accountReadAuthorityStub{access: access}, accountReadCallsStub{err: errors.New("provider body with customer data")})
	_, err := service.ReadConnectionAccount(t.Context(), model.ConnectionAccountSubject{
		WorkspaceID: "workspace-a", UserID: "user-a", Access: model.ConnectionAccountAccess{Personal: true},
	}, "connection-a", model.ConnectionAccountReadRequest{
		RequestID: "request-a", Operation: "fetch_meeting_content", ContractSHA256: contract, Payload: []byte(`{}`),
	})
	if err == nil || err.Error() != "Integration account read failed" || strings.Contains(err.Error(), "customer data") {
		t.Fatalf("error=%v", err)
	}
}
