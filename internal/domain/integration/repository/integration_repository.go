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
