// Package moduleassembly assembles Integration over host-owned infrastructure.
package moduleassembly

import (
	"context"
	"fmt"

	connectorscatalog "github.com/domainry/domainry-connectors/catalog"
	foundationhttp "github.com/domainry/domainry-foundation/modulehttp"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationcapability "github.com/domainry/domainry-integration/capability"
	"github.com/domainry/domainry-integration/internal/adapter/accountwrite"
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
	if host == nil || host.Database() == nil || host.Dialect() == nil || host.Migrations() == nil || host.Providers() == nil || host.SecretCipher() == nil || host.RuntimeTriggers() == nil {
		return nil, fmt.Errorf("Integration Module host is incomplete")
	}
	migrations, err := databaseschema.SchemaMigrations(host.Migrations().Driver(), host.Migrations().Schema())
	if err != nil {
		return nil, err
	}
	if err := host.Migrations().ApplyOwnedMigrations(ctx, "integration", migrations); err != nil {
		return nil, fmt.Errorf("apply Integration Module migrations: %w", err)
	}
	builtinCatalog, err := connectorscatalog.Definitions()
	if err != nil {
		return nil, err
	}
	if err := integrationpersistence.SyncBuiltinCatalog(ctx, host.Database(), host.Dialect(), builtinCatalog); err != nil {
		return nil, fmt.Errorf("synchronize Integration built-in catalog: %w", err)
	}
	if err := integrationpersistence.SyncProviderCatalog(ctx, host.Database(), host.Dialect(), host.Providers().Descriptors()); err != nil {
		return nil, fmt.Errorf("synchronize Integration provider catalog: %w", err)
	}
	webPush := integrationpersistence.NewWebPushSubscriptionStore(host.Database(), host.Dialect())
	resolver := integrationpersistence.NewSecretResolver(host.Database(), host.Dialect(), host.SecretCipher())
	delivery := integrationpersistence.NewDeliveryStore(host.Database(), host.Dialect(), host.Providers(), resolver, webPush)
	operations := integrationpersistence.NewOperationsStore(host.Database(), host.Dialect(), delivery, host.RuntimeTriggers())
	workers := integrationpersistence.NewWorkerStore(host.Database(), host.Dialect(), delivery, operations, application.RuntimeID)
	domain := integrationservice.New(
		integrationpersistence.NewCatalogStore(host.Database(), host.Dialect()),
		integrationpersistence.NewRequirementsStore(host.Database(), host.Dialect(), host.Providers()),
		delivery,
		webPush,
		operations,
	)
	management := integrationpersistence.NewManagementStore(host.Database(), host.Dialect(), host.SecretCipher(), delivery)
	applicationService := integrationapplication.New(domain)
	capability, err := integrationcapability.Open(integrationcapability.Inputs{})
	if err != nil {
		return nil, fmt.Errorf("build Integration capability disclosure: %w", err)
	}
	binding, err := integrationsdkadapter.NewBinding(mode, applicationService, management, capability, workers)
	if err != nil {
		return nil, err
	}
	binding.SetConnectionAccountReads(integrationapplication.NewAccountReadService(management, domain))
	binding.SetConnectionAccountWrites(integrationapplication.NewAccountWriteService(management, accountwrite.Codec{}, operations, operations))
	binding.SetSubjectLifecycle(integrationsdkadapter.NewSubjectLifecycleBinding(integrationapplication.NewSubjectLifecycleService(integrationpersistence.NewSubjectLifecycleStore(host.Database(), host.Dialect()))))
	if mode == integrationsdk.DeploymentModeModule {
		adapter, err := modulehttp.NewAdapter(binding)
		if err != nil {
			return nil, err
		}
		binding.SetHTTPAdapters([]foundationhttp.Adapter{adapter})
	}
	return binding, nil
}

var _ modulehost.Factory = (*Factory)(nil)

func SchemaMigrations(driver, schema string) ([]modulehost.SchemaMigration, error) {
	return databaseschema.SchemaMigrations(driver, schema)
}
