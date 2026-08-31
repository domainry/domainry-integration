package integrationapplication

import (
	"context"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	integrationservice "github.com/domainry/domainry-integration/internal/domain/integration/service"
)

type Service struct{ domain *integrationservice.Service }

func New(domain *integrationservice.Service) *Service { return &Service{domain: domain} }
func (s *Service) ListConnectorDefinitions(ctx context.Context) ([]integrationmodel.ConnectorDefinition, error) {
	return s.domain.ListConnectorDefinitions(ctx)
}
func (s *Service) SynchronizeConnections(ctx context.Context, v []integrationmodel.ConnectionRequirement) error {
	return s.domain.SynchronizeConnections(ctx, v)
}
func (s *Service) Accept(ctx context.Context, v integrationmodel.DeliveryRequest) (integrationmodel.DeliveryReceipt, error) {
	return s.domain.Accept(ctx, v)
}
func (s *Service) Query(ctx context.Context, id string) (integrationmodel.DeliveryReceipt, error) {
	return s.domain.Query(ctx, id)
}
func (s *Service) WebPushReadiness(ctx context.Context, workspace string) (integrationmodel.WebPushReadiness, error) {
	return s.domain.WebPushReadiness(ctx, workspace)
}
func (s *Service) ListWebPush(ctx context.Context, workspace, user string) ([]integrationmodel.WebPushSubscription, error) {
	return s.domain.ListWebPush(ctx, workspace, user)
}
func (s *Service) UpsertWebPush(ctx context.Context, workspace, user, id string, v integrationmodel.WebPushSubscriptionInput) (integrationmodel.WebPushSubscription, error) {
	return s.domain.UpsertWebPush(ctx, workspace, user, id, v)
}
func (s *Service) RevokeWebPush(ctx context.Context, workspace, user, id string) (integrationmodel.WebPushSubscription, error) {
	return s.domain.RevokeWebPush(ctx, workspace, user, id)
}
func (s *Service) CleanupExpiredWebPush(ctx context.Context, workspace string) (int, error) {
	return s.domain.CleanupExpiredWebPush(ctx, workspace)
}
