package integrationservice

import (
	"context"
	"testing"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type repositories struct{}

func (repositories) ListConnectorDefinitions(context.Context) ([]integrationmodel.ConnectorDefinition, error) {
	return nil, nil
}
func (repositories) SynchronizeConnections(context.Context, []integrationmodel.ConnectionRequirement) error {
	return nil
}
func (repositories) Accept(context.Context, integrationmodel.DeliveryRequest) (integrationmodel.DeliveryReceipt, error) {
	return integrationmodel.DeliveryReceipt{}, nil
}
func (repositories) Query(context.Context, string) (integrationmodel.DeliveryReceipt, error) {
	return integrationmodel.DeliveryReceipt{}, nil
}
func (repositories) Readiness(context.Context, string) (integrationmodel.WebPushReadiness, error) {
	return integrationmodel.WebPushReadiness{}, nil
}
func (repositories) List(context.Context, string, string) ([]integrationmodel.WebPushSubscription, error) {
	return nil, nil
}
func (repositories) Upsert(context.Context, string, string, string, integrationmodel.WebPushSubscriptionInput) (integrationmodel.WebPushSubscription, error) {
	return integrationmodel.WebPushSubscription{}, nil
}
func (repositories) Revoke(context.Context, string, string, string) (integrationmodel.WebPushSubscription, error) {
	return integrationmodel.WebPushSubscription{}, nil
}
func (repositories) CleanupExpired(context.Context, string) (int, error) { return 0, nil }

func TestServiceOwnsDeploymentNeutralValidation(t *testing.T) {
	repository := repositories{}
	service := New(repository, repository, repository, repository)
	if err := service.SynchronizeConnections(t.Context(), []integrationmodel.ConnectionRequirement{{Config: []byte(`{}`)}}); err == nil {
		t.Fatal("invalid requirement accepted")
	}
	if _, err := service.Accept(t.Context(), integrationmodel.DeliveryRequest{Payload: []byte(`{}`)}); err == nil {
		t.Fatal("invalid delivery accepted")
	}
	valid := integrationmodel.DeliveryRequest{MessageID: "message", DeduplicationKey: "dedupe", WorkspaceID: "workspace", ConnectorKey: "crm", Operation: "upsert", Payload: []byte(`{}`)}
	if _, err := service.Accept(t.Context(), valid); err != nil {
		t.Fatal(err)
	}
}
