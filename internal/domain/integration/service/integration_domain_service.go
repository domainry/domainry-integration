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
	operations   integrationrepository.Operations
}

func New(c integrationrepository.Catalog, r integrationrepository.Requirements, d integrationrepository.Delivery, w integrationrepository.WebPushSubscriptions, operations ...integrationrepository.Operations) *Service {
	service := &Service{catalog: c, requirements: r, delivery: d, webPush: w}
	if len(operations) != 0 {
		service.operations = operations[0]
	}
	return service
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
func (s *Service) SynchronizeEventMappings(ctx context.Context, values []integrationmodel.EventMappingRequirement) error {
	for _, value := range values {
		if err := value.Validate(); err != nil {
			return err
		}
	}
	return s.requirements.SynchronizeEventMappings(ctx, values)
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

func (s *Service) Call(ctx context.Context, value integrationmodel.ProviderCallRequest) (integrationmodel.ProviderCallResult, error) {
	if s.operations == nil {
		return integrationmodel.ProviderCallResult{}, fmt.Errorf("Integration Operations port is unavailable")
	}
	if strings.TrimSpace(value.RequestID) == "" || strings.TrimSpace(value.WorkspaceID) == "" || strings.TrimSpace(value.ConnectorKey) == "" || strings.TrimSpace(value.Operation) == "" || !json.Valid(value.Payload) {
		return integrationmodel.ProviderCallResult{}, fmt.Errorf("Integration provider call is invalid")
	}
	return s.operations.Call(ctx, value)
}
func (s *Service) ListInvocations(ctx context.Context, query integrationmodel.InvocationQuery) ([]integrationmodel.Invocation, error) {
	if s.operations == nil {
		return nil, fmt.Errorf("Integration Operations port is unavailable")
	}
	return s.operations.ListInvocations(ctx, query)
}
func (s *Service) GetInvocation(ctx context.Context, workspaceID, id string) (integrationmodel.Invocation, error) {
	if s.operations == nil {
		return integrationmodel.Invocation{}, fmt.Errorf("Integration Operations port is unavailable")
	}
	return s.operations.GetInvocation(ctx, workspaceID, id)
}
func (s *Service) AcceptWebhook(ctx context.Context, request integrationmodel.WebhookRequest) (integrationmodel.WebhookReceipt, error) {
	if s.operations == nil || strings.TrimSpace(request.WorkspaceID) == "" || strings.TrimSpace(request.ConnectorKey) == "" || strings.TrimSpace(request.ConnectionKey) == "" || request.ReceivedAt.IsZero() {
		return integrationmodel.WebhookReceipt{}, fmt.Errorf("Integration webhook request is invalid")
	}
	return s.operations.AcceptWebhook(ctx, request)
}
func (s *Service) ListEvents(ctx context.Context, query integrationmodel.EventQuery) ([]integrationmodel.Event, error) {
	if s.operations == nil {
		return nil, fmt.Errorf("Integration Operations port is unavailable")
	}
	return s.operations.ListEvents(ctx, query)
}
func (s *Service) GetEvent(ctx context.Context, workspaceID, id string) (integrationmodel.Event, error) {
	if s.operations == nil {
		return integrationmodel.Event{}, fmt.Errorf("Integration Operations port is unavailable")
	}
	return s.operations.GetEvent(ctx, workspaceID, id)
}
func (s *Service) ReplayEvent(ctx context.Context, workspaceID, id string) (integrationmodel.Event, error) {
	if s.operations == nil {
		return integrationmodel.Event{}, fmt.Errorf("Integration Operations port is unavailable")
	}
	return s.operations.ReplayEvent(ctx, workspaceID, id)
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
