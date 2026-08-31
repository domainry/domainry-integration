package integrationrepository

import (
	"context"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type Catalog interface {
	ListConnectorDefinitions(context.Context) ([]integrationmodel.ConnectorDefinition, error)
}

type Requirements interface {
	SynchronizeConnections(context.Context, []integrationmodel.ConnectionRequirement) error
	SynchronizeEventMappings(context.Context, []integrationmodel.EventMappingRequirement) error
}

type Operations interface {
	Call(context.Context, integrationmodel.ProviderCallRequest) (integrationmodel.ProviderCallResult, error)
	ListInvocations(context.Context, integrationmodel.InvocationQuery) ([]integrationmodel.Invocation, error)
	GetInvocation(context.Context, string, string) (integrationmodel.Invocation, error)
	AcceptWebhook(context.Context, integrationmodel.WebhookRequest) (integrationmodel.WebhookReceipt, error)
	ListEvents(context.Context, integrationmodel.EventQuery) ([]integrationmodel.Event, error)
	GetEvent(context.Context, string, string) (integrationmodel.Event, error)
	ReplayEvent(context.Context, string, string) (integrationmodel.Event, error)
}

type Delivery interface {
	Accept(context.Context, integrationmodel.DeliveryRequest) (integrationmodel.DeliveryReceipt, error)
	Query(context.Context, string) (integrationmodel.DeliveryReceipt, error)
}

type WebPushSubscriptions interface {
	Readiness(context.Context, string) (integrationmodel.WebPushReadiness, error)
	List(context.Context, string, string) ([]integrationmodel.WebPushSubscription, error)
	Upsert(context.Context, string, string, string, integrationmodel.WebPushSubscriptionInput) (integrationmodel.WebPushSubscription, error)
	Revoke(context.Context, string, string, string) (integrationmodel.WebPushSubscription, error)
	CleanupExpired(context.Context, string) (int, error)
}
