package integrationsdkadapter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/domainry/domainry-foundation/modulehttp"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationapplication "github.com/domainry/domainry-integration/internal/application/integration"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type Binding struct {
	mode     integrationsdk.DeploymentMode
	service  *integrationapplication.Service
	surfaces []modulehttp.Surface
}

func NewBinding(mode integrationsdk.DeploymentMode, service *integrationapplication.Service) *Binding {
	return &Binding{mode: mode, service: service}
}
func (b *Binding) Descriptor() integrationsdk.Descriptor {
	return integrationsdk.Descriptor{ProtocolVersion: integrationsdk.ProtocolVersionV1, Mode: b.mode, Capabilities: []string{"catalog.read", "requirements.connections.sync", "delivery.accept", "delivery.query", "web_push_subscriptions.manage"}}
}
func (b *Binding) Catalog() integrationsdk.Catalog           { return catalogBinding{b} }
func (b *Binding) Requirements() integrationsdk.Requirements { return requirementsBinding{b} }
func (b *Binding) Delivery() integrationsdk.Delivery         { return deliveryBinding{b} }
func (b *Binding) WebPushSubscriptions() integrationsdk.WebPushSubscriptions {
	return webPushBinding{b}
}
func (*Binding) Close(context.Context) error { return nil }
func (b *Binding) SetHTTPSurfaces(surfaces []modulehttp.Surface) {
	b.surfaces = append([]modulehttp.Surface(nil), surfaces...)
}
func (b *Binding) HTTPSurfaces() []modulehttp.Surface {
	return append([]modulehttp.Surface(nil), b.surfaces...)
}

type catalogBinding struct{ *Binding }

func (b catalogBinding) ListConnectorDefinitions(ctx context.Context) ([]integrationsdk.ConnectorDefinition, error) {
	v, err := b.service.ListConnectorDefinitions(ctx)
	return convert[[]integrationsdk.ConnectorDefinition](v, err)
}

type requirementsBinding struct{ *Binding }

func (b requirementsBinding) SynchronizeConnections(ctx context.Context, values []integrationsdk.ConnectionRequirement) error {
	v, err := convert[[]integrationmodel.ConnectionRequirement](values, nil)
	if err != nil {
		return err
	}
	return b.service.SynchronizeConnections(ctx, v)
}

type deliveryBinding struct{ *Binding }

func (b deliveryBinding) Accept(ctx context.Context, value integrationsdk.DeliveryRequest) (integrationsdk.DeliveryReceipt, error) {
	v, err := convert[integrationmodel.DeliveryRequest](value, nil)
	if err != nil {
		return integrationsdk.DeliveryReceipt{}, err
	}
	result, err := b.service.Accept(ctx, v)
	return convert[integrationsdk.DeliveryReceipt](result, err)
}
func (b deliveryBinding) Query(ctx context.Context, id string) (integrationsdk.DeliveryReceipt, error) {
	v, err := b.service.Query(ctx, id)
	return convert[integrationsdk.DeliveryReceipt](v, err)
}

type webPushBinding struct{ *Binding }

func (b webPushBinding) Readiness(ctx context.Context, workspace string) (integrationsdk.WebPushReadiness, error) {
	v, err := b.service.WebPushReadiness(ctx, workspace)
	return convert[integrationsdk.WebPushReadiness](v, err)
}
func (b webPushBinding) List(ctx context.Context, workspace, user string) ([]integrationsdk.WebPushSubscription, error) {
	v, err := b.service.ListWebPush(ctx, workspace, user)
	return convert[[]integrationsdk.WebPushSubscription](v, err)
}
func (b webPushBinding) Upsert(ctx context.Context, workspace, user, id string, input integrationsdk.WebPushSubscriptionInput) (integrationsdk.WebPushSubscription, error) {
	v, err := convert[integrationmodel.WebPushSubscriptionInput](input, nil)
	if err != nil {
		return integrationsdk.WebPushSubscription{}, err
	}
	result, err := b.service.UpsertWebPush(ctx, workspace, user, id, v)
	return convert[integrationsdk.WebPushSubscription](result, err)
}
func (b webPushBinding) Revoke(ctx context.Context, workspace, user, id string) (integrationsdk.WebPushSubscription, error) {
	v, err := b.service.RevokeWebPush(ctx, workspace, user, id)
	return convert[integrationsdk.WebPushSubscription](v, err)
}
func (b webPushBinding) CleanupExpired(ctx context.Context, workspace string) (int, error) {
	return b.service.CleanupExpiredWebPush(ctx, workspace)
}

func convert[T any](value any, sourceErr error) (T, error) {
	var zero T
	if sourceErr != nil {
		return zero, sourceErr
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return zero, fmt.Errorf("encode Integration contract: %w", err)
	}
	if err := json.Unmarshal(payload, &zero); err != nil {
		return zero, fmt.Errorf("decode Integration contract: %w", err)
	}
	return zero, nil
}

var _ integrationsdk.Binding = (*Binding)(nil)
var _ integrationsdk.WebPushBinding = (*Binding)(nil)
var _ modulehttp.Provider = (*Binding)(nil)
