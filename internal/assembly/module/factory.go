// Package moduleassembly assembles Integration over host-owned infrastructure.
package moduleassembly

import (
	"context"
	"fmt"

	foundationhttp "github.com/domainry/domainry-foundation/modulehttp"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationsdkadapter "github.com/domainry/domainry-integration/internal/adapter/integrationsdk"
	integrationapplication "github.com/domainry/domainry-integration/internal/application/integration"
	integrationservice "github.com/domainry/domainry-integration/internal/domain/integration/service"
	integrationpersistence "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/integration"
	databaseschema "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/schema"
	modulehttp "github.com/domainry/domainry-integration/internal/transport/http/module"
)

type Options struct{}

func OptionsFromEnvironment() Options { return Options{} }

type Factory struct{ options Options }

func NewFactory(options ...Options) *Factory {
	var value Options
	if len(options) != 0 {
		value = options[0]
	}
	return &Factory{options: value}
}
func (*Factory) DeploymentMode() integrationsdk.DeploymentMode {
	return integrationsdk.DeploymentModeModule
}
func (*Factory) OpenModule(ctx context.Context, application integrationsdk.ApplicationRef, host modulehost.Host) (integrationsdk.Binding, error) {
	return OpenHosted(ctx, application, host, integrationsdk.DeploymentModeModule)
}

func OpenHosted(ctx context.Context, application integrationsdk.ApplicationRef, host modulehost.Host, mode integrationsdk.DeploymentMode) (integrationsdk.Binding, error) {
	if err := application.Validate(); err != nil {
		return nil, err
	}
	if host == nil || host.Database() == nil || host.Dialect() == nil || host.Migrations() == nil || host.Providers() == nil || host.SecretCipher() == nil {
		return nil, fmt.Errorf("Integration Module host is incomplete")
	}
	migrations, err := databaseschema.SchemaMigrations(host.Migrations().Driver(), host.Migrations().Schema())
	if err != nil {
		return nil, err
	}
	if err := host.Migrations().ApplyOwnedMigrations(ctx, "integration", migrations); err != nil {
		return nil, fmt.Errorf("apply Integration Module migrations: %w", err)
	}
	if err := integrationpersistence.SyncProviderCatalog(ctx, host.Database(), host.Dialect(), host.Providers().Descriptors()); err != nil {
		return nil, fmt.Errorf("synchronize Integration provider catalog: %w", err)
	}
	webPush := integrationpersistence.NewWebPushSubscriptionStore(host.Database(), host.Dialect())
	resolver := integrationpersistence.NewSecretResolver(host.Database(), host.Dialect(), host.SecretCipher())
	domain := integrationservice.New(
		integrationpersistence.NewCatalogStore(host.Database(), host.Dialect()),
		integrationpersistence.NewRequirementsStore(host.Database(), host.Dialect(), host.Providers()),
		integrationpersistence.NewDeliveryStore(host.Database(), host.Dialect(), host.Providers(), resolver, webPush),
		webPush,
	)
	binding := integrationsdkadapter.NewBinding(mode, integrationapplication.New(domain))
	if mode == integrationsdk.DeploymentModeModule {
		surface, err := modulehttp.NewSurface(binding)
		if err != nil {
			return nil, err
		}
		binding.SetHTTPSurfaces([]foundationhttp.Surface{surface})
	}
	return binding, nil
}

var _ modulehost.Factory = (*Factory)(nil)

func SchemaMigrations(driver, schema string) ([]modulehost.SchemaMigration, error) {
	return databaseschema.SchemaMigrations(driver, schema)
}
