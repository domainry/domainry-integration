package integrationservice

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	integrationrepository "github.com/domainry/domainry-integration/internal/domain/integration/repository"
)

type Service struct {
	catalog      integrationrepository.Catalog
	requirements integrationrepository.Requirements
	delivery     integrationrepository.Delivery
	webPush      integrationrepository.WebPushSubscriptions
}

func New(c integrationrepository.Catalog, r integrationrepository.Requirements, d integrationrepository.Delivery, w integrationrepository.WebPushSubscriptions) *Service {
	return &Service{catalog: c, requirements: r, delivery: d, webPush: w}
}

func (s *Service) ListConnectorDefinitions(ctx context.Context) ([]integrationmodel.ConnectorDefinition, error) {
	return s.catalog.ListConnectorDefinitions(ctx)
}
func (s *Service) SynchronizeConnections(ctx context.Context, values []integrationmodel.ConnectionRequirement) error {
	for _, value := range values {
		if err := validateRequirement(value); err != nil {
			return err
		}
	}
	return s.requirements.SynchronizeConnections(ctx, values)
}
func (s *Service) Accept(ctx context.Context, value integrationmodel.DeliveryRequest) (integrationmodel.DeliveryReceipt, error) {
	if err := validateDelivery(value); err != nil {
		return integrationmodel.DeliveryReceipt{}, err
	}
	return s.delivery.Accept(ctx, value)
}
func (s *Service) Query(ctx context.Context, messageID string) (integrationmodel.DeliveryReceipt, error) {
	if strings.TrimSpace(messageID) == "" {
		return integrationmodel.DeliveryReceipt{}, fmt.Errorf("Integration delivery message ID is required")
	}
	return s.delivery.Query(ctx, messageID)
}
func (s *Service) WebPushReadiness(ctx context.Context, workspaceID string) (integrationmodel.WebPushReadiness, error) {
	return s.webPush.Readiness(ctx, workspaceID)
}
func (s *Service) ListWebPush(ctx context.Context, workspaceID, userID string) ([]integrationmodel.WebPushSubscription, error) {
	return s.webPush.List(ctx, workspaceID, userID)
}
func (s *Service) UpsertWebPush(ctx context.Context, workspaceID, userID, id string, input integrationmodel.WebPushSubscriptionInput) (integrationmodel.WebPushSubscription, error) {
	return s.webPush.Upsert(ctx, workspaceID, userID, id, input)
}
func (s *Service) RevokeWebPush(ctx context.Context, workspaceID, userID, id string) (integrationmodel.WebPushSubscription, error) {
	return s.webPush.Revoke(ctx, workspaceID, userID, id)
}
func (s *Service) CleanupExpiredWebPush(ctx context.Context, workspaceID string) (int, error) {
	return s.webPush.CleanupExpired(ctx, workspaceID)
}

func validateRequirement(v integrationmodel.ConnectionRequirement) error {
	for name, value := range map[string]string{"key": v.Key, "workspace_id": v.WorkspaceID, "connector_key": v.ConnectorKey, "provider_key": v.ProviderKey} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("Integration connection requirement %s is required", name)
		}
	}
	if !json.Valid(v.Config) {
		return fmt.Errorf("Integration connection requirement config must be valid JSON")
	}
	return nil
}
func validateDelivery(v integrationmodel.DeliveryRequest) error {
	for name, value := range map[string]string{"message_id": v.MessageID, "deduplication_key": v.DeduplicationKey, "workspace_id": v.WorkspaceID, "connector_key": v.ConnectorKey, "operation": v.Operation} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("Integration delivery %s is required", name)
		}
	}
	if !json.Valid(v.Payload) {
		return fmt.Errorf("Integration delivery payload must be valid JSON")
	}
	return nil
}
