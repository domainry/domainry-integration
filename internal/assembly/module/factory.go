// Package moduleassembly assembles Integration over host-owned infrastructure.
package moduleassembly

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	shareddefinition "github.com/domainry/domainry-foundation/definition"
	foundationhttp "github.com/domainry/domainry-foundation/modulehttp"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-integration/internal/adapter/accountwrite"
	integrationsdkadapter "github.com/domainry/domainry-integration/internal/adapter/integrationsdk"
	integrationapplication "github.com/domainry/domainry-integration/internal/application/integration"
	integrationservice "github.com/domainry/domainry-integration/internal/domain/integration/service"
	integrationpersistence "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/integration"
	databaseschema "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/schema"
	googlequota "github.com/domainry/domainry-integration/internal/infrastructure/providerquota/googlecloud"
	modulehttp "github.com/domainry/domainry-integration/internal/transport/http/module"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
)

type Options struct {
	ConnectorCatalog             connector.DefinitionCatalog
	GoogleQuotaProjectID         string
	GoogleQuotaServices          []string
	GoogleQuotaCredentialsFile   string
	GoogleQuotaMonitoringBaseURL string
	GoogleQuotaTimeout           time.Duration
}

func OptionsFromEnvironment() Options {
	services := []string{}
	for _, raw := range strings.Split(os.Getenv("INTEGRATION_GOOGLE_QUOTA_SERVICES"), ",") {
		if value := strings.TrimSpace(raw); value != "" {
			services = append(services, value)
		}
	}
	timeout := 10 * time.Second
	if raw := strings.TrimSpace(os.Getenv("INTEGRATION_GOOGLE_QUOTA_TIMEOUT")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil {
			timeout = parsed
		}
	}
	return Options{
		GoogleQuotaProjectID:       strings.TrimSpace(os.Getenv("INTEGRATION_GOOGLE_QUOTA_PROJECT_ID")),
		GoogleQuotaServices:        services,
		GoogleQuotaCredentialsFile: strings.TrimSpace(os.Getenv("INTEGRATION_GOOGLE_QUOTA_CREDENTIALS_FILE")),
		GoogleQuotaTimeout:         timeout,
	}
}

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
func (f *Factory) OpenModule(ctx context.Context, application integrationsdk.ApplicationRef, host modulehost.Host) (integrationsdk.Binding, error) {
	return OpenHosted(ctx, application, host, integrationsdk.DeploymentModeModule, f.options)
}

func OpenHosted(ctx context.Context, application integrationsdk.ApplicationRef, host modulehost.Host, mode integrationsdk.DeploymentMode, configured ...Options) (integrationsdk.Binding, error) {
	var options Options
	if len(configured) > 0 {
		options = configured[0]
	}
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
	if err := host.Migrations().ApplyOwnedMigrations(ctx, databaseschema.MigrationOwner, migrations); err != nil {
		return nil, fmt.Errorf("apply Integration Module migrations: %w", err)
	}
	definitionKernel, err := shareddefinition.Open(ctx, application.RuntimeID, host.Database(), host.Dialect(), host.Migrations())
	if err != nil {
		return nil, fmt.Errorf("open Integration Definition persistence: %w", err)
	}
	definitions := metadatasdk.AdaptDefinitionStore(definitionKernel)
	var builtinCatalog []connector.ConnectorDefinition
	if options.ConnectorCatalog != nil {
		builtinCatalog, err = options.ConnectorCatalog()
		if err != nil {
			return nil, fmt.Errorf("load Integration connector catalog: %w", err)
		}
	}
	if err := integrationpersistence.SyncConnectorCatalog(ctx, definitions, builtinCatalog, host.Providers().Descriptors()); err != nil {
		return nil, fmt.Errorf("synchronize Integration connector Definitions: %w", err)
	}
	subjectLifecyclePersistence := integrationpersistence.NewSubjectLifecyclePersistence()
	webPush := integrationpersistence.NewWebPushSubscriptionStore(host.Database(), host.Dialect(), subjectLifecyclePersistence)
	resolver := integrationpersistence.NewSecretResolver(host.Database(), host.Dialect(), host.SecretCipher(), subjectLifecyclePersistence)
	delivery := integrationpersistence.NewDeliveryStoreWithSubjectLifecycle(host.Database(), host.Dialect(), host.Providers(), resolver, webPush, subjectLifecyclePersistence)
	operations := integrationpersistence.NewOperationsStore(host.Database(), host.Dialect(), delivery, host.RuntimeTriggers(), definitions, subjectLifecyclePersistence)
	workers := integrationpersistence.NewWorkerStore(host.Database(), host.Dialect(), delivery, operations, application.RuntimeID, subjectLifecyclePersistence)
	if notifying, ok := host.(modulehost.EventWorkNotifierHost); ok {
		workers.SetEventWorkNotifier(notifying.EventWorkNotifier())
	}
	routedWebhooks := integrationpersistence.NewRoutedWebhookStore(host.Database(), host.Dialect(), delivery, subjectLifecyclePersistence)
	domain := integrationservice.New(
		integrationpersistence.NewCatalogStore(definitions),
		integrationpersistence.NewRequirementsStore(host.Database(), host.Dialect(), host.Providers(), definitions, subjectLifecyclePersistence),
		delivery,
		webPush,
		operations,
	)
	management := integrationpersistence.NewManagementStore(host.Database(), host.Dialect(), host.SecretCipher(), delivery, subjectLifecyclePersistence)
	if strings.TrimSpace(options.GoogleQuotaProjectID) != "" {
		quotaObserver, quotaErr := googlequota.New(ctx, googlequota.Config{
			ProjectID: options.GoogleQuotaProjectID, Services: options.GoogleQuotaServices,
			CredentialsFile: options.GoogleQuotaCredentialsFile, BaseURL: options.GoogleQuotaMonitoringBaseURL,
			Timeout: options.GoogleQuotaTimeout,
		})
		if quotaErr != nil {
			return nil, fmt.Errorf("configure Google official quota monitoring: %w", quotaErr)
		}
		management.ConfigureProviderQuotaObserver(quotaObserver)
	}
	applicationService := integrationapplication.New(domain)
	binding, err := integrationsdkadapter.NewBinding(mode, applicationService, management, workers)
	if err != nil {
		return nil, err
	}
	binding.SetConnectionAccountReads(integrationapplication.NewAccountReadService(management, domain))
	binding.SetConnectionAccountWrites(integrationapplication.NewAccountWriteService(management, accountwrite.Codec{}, operations, operations))
	binding.SetRoutedWebhooks(routedWebhooks)
	binding.SetSubjectLifecycle(integrationsdkadapter.NewSubjectLifecycleBinding(integrationapplication.NewSubjectLifecycleService(integrationpersistence.NewSubjectLifecycleStore(host.Database(), host.Dialect(), definitions, subjectLifecyclePersistence))))
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
