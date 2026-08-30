package module

import (
	"context"
	"fmt"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationpersistence "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/integration"
)

type Factory struct{}

func NewFactory() *Factory { return &Factory{} }

// SchemaMigrations exposes Integration-owned DDL for hosts and ownership-aware
// test fixtures. Production Module startup applies the same migrations through
// the host's sole migration registrar.
func SchemaMigrations(driver, schema string) ([]modulehost.SchemaMigration, error) {
	return integrationpersistence.SchemaMigrations(driver, schema)
}

func (*Factory) DeploymentMode() integrationsdk.DeploymentMode {
	return integrationsdk.DeploymentModeModule
}

func (*Factory) OpenModule(ctx context.Context, application integrationsdk.ApplicationRef, host modulehost.Host) (integrationsdk.Binding, error) {
	if err := application.Validate(); err != nil {
		return nil, err
	}
	if host == nil || host.Database() == nil || host.Dialect() == nil || host.Migrations() == nil || host.Providers() == nil || host.SecretCipher() == nil {
		return nil, fmt.Errorf("Integration Module persistence host is incomplete")
	}
	migrations, err := integrationpersistence.SchemaMigrations(host.Migrations().Driver(), host.Migrations().Schema())
	if err != nil {
		return nil, err
	}
	if err := host.Migrations().ApplyOwnedMigrations(ctx, "integration", migrations); err != nil {
		return nil, fmt.Errorf("apply Integration Module migrations: %w", err)
	}
	if err := integrationpersistence.SyncProviderCatalog(ctx, host.Database(), host.Dialect(), host.Providers().Descriptors()); err != nil {
		return nil, fmt.Errorf("synchronize Integration provider catalog: %w", err)
	}
	secretResolver := integrationpersistence.NewSecretResolver(host.Database(), host.Dialect(), host.SecretCipher())
	webPushSubscriptions := integrationpersistence.NewWebPushSubscriptionStore(host.Database(), host.Dialect())
	return binding{
		catalog:              integrationpersistence.NewCatalogStore(host.Database(), host.Dialect()),
		requirements:         integrationpersistence.NewRequirementsStore(host.Database(), host.Dialect(), host.Providers()),
		delivery:             integrationpersistence.NewDeliveryStore(host.Database(), host.Dialect(), host.Providers(), secretResolver, webPushSubscriptions),
		webPushSubscriptions: webPushSubscriptions,
	}, nil
}

type binding struct {
	catalog              integrationsdk.Catalog
	requirements         integrationsdk.Requirements
	delivery             integrationsdk.Delivery
	webPushSubscriptions integrationsdk.WebPushSubscriptions
}

func (binding) Descriptor() integrationsdk.Descriptor {
	return integrationsdk.Descriptor{ProtocolVersion: integrationsdk.ProtocolVersionV1, Mode: integrationsdk.DeploymentModeModule, Capabilities: []string{"catalog.read", "requirements.connections.sync", "delivery.accept", "delivery.query", "web_push_subscriptions.manage"}}
}
func (b binding) Catalog() integrationsdk.Catalog           { return b.catalog }
func (b binding) Requirements() integrationsdk.Requirements { return b.requirements }
func (b binding) WebPushSubscriptions() integrationsdk.WebPushSubscriptions {
	return b.webPushSubscriptions
}
func (b binding) Delivery() integrationsdk.Delivery { return b.delivery }
func (binding) Close(context.Context) error         { return nil }

var _ integrationsdk.Binding = binding{}
