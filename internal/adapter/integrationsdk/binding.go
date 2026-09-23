package integrationsdkadapter

import (
	"context"
	"encoding/json"
	"fmt"

	actioncontract "github.com/domainry/domainry-foundation/action"
	"github.com/domainry/domainry-foundation/modulehttp"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationapplication "github.com/domainry/domainry-integration/internal/application/integration"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type Binding struct {
	mode          integrationsdk.DeploymentMode
	service       *integrationapplication.Service
	management    integrationsdk.Management
	accounts      integrationsdk.ConnectionAccounts
	accountReads  *integrationapplication.AccountReadService
	accountWrites *integrationapplication.AccountWriteService
	subjects      integrationsdk.SubjectLifecycle
	accountAdmin  integrationsdk.ConnectionAccountAdministration
	workers       integrationsdk.LocalWorkers
	adapters      []modulehttp.Adapter
}

func NewBinding(mode integrationsdk.DeploymentMode, service *integrationapplication.Service, management integrationsdk.Management, workers ...integrationsdk.LocalWorkers) (*Binding, error) {
	accounts, accountsOK := management.(integrationsdk.ConnectionAccounts)
	accountAdmin, accountAdminOK := management.(integrationsdk.ConnectionAccountAdministration)
	if !accountsOK || !accountAdminOK {
		return nil, fmt.Errorf("Integration connection account stores are required")
	}
	binding := &Binding{mode: mode, service: service, management: management, accounts: accounts, accountAdmin: accountAdmin}
	if len(workers) != 0 {
		binding.workers = workers[0]
	}
	return binding, nil
}
func (b *Binding) Descriptor() integrationsdk.Descriptor {
	capabilities := []string{"catalog.read", "requirements.connections.sync", "delivery.accept", "delivery.query", "web_push_subscriptions.manage", "management.connections", "connection_accounts.manage", "management.secrets", "management.api_keys", "management.external_identities", "management.webhook_subscriptions", "operations.call", "operations.invocations.query", "inbound.webhooks.accept", "inbound.events.query"}
	if port, ok := b.management.(integrationsdk.OAuthApplications); ok && port != nil {
		capabilities = append(capabilities, "oauth_applications.manage")
	}
	if port, ok := b.management.(integrationsdk.OAuthAuthorizations); ok && port != nil {
		capabilities = append(capabilities, "oauth_authorizations.manage")
	}
	if b.workers != nil {
		capabilities = append(capabilities, "local_workers")
	}
	if b.accountWrites != nil {
		capabilities = append(capabilities, "connection_accounts.write")
	}
	if b.accountReads != nil {
		capabilities = append(capabilities, "connection_accounts.read")
	}
	if b.subjects != nil {
		capabilities = append(capabilities, "subjects.lifecycle")
	}
	return integrationsdk.Descriptor{ProtocolVersion: integrationsdk.ProtocolVersionV1, Mode: b.mode, Capabilities: capabilities}
}
func (b *Binding) Catalog() integrationsdk.Catalog                       { return catalogBinding{b} }
func (b *Binding) Requirements() integrationsdk.Requirements             { return requirementsBinding{b} }
func (b *Binding) Delivery() integrationsdk.Delivery                     { return deliveryBinding{b} }
func (b *Binding) Management() integrationsdk.Management                 { return b.management }
func (b *Binding) ConnectionAccounts() integrationsdk.ConnectionAccounts { return b.accounts }
func (b *Binding) ConnectionAccountReads() integrationsdk.ConnectionAccountReads {
	if b.accountReads == nil {
		return nil
	}
	return accountReadsBinding{service: b.accountReads}
}
func (b *Binding) SetConnectionAccountReads(reads *integrationapplication.AccountReadService) {
	b.accountReads = reads
}
func (b *Binding) ConnectionAccountAdministration() integrationsdk.ConnectionAccountAdministration {
	return b.accountAdmin
}
func (b *Binding) Operations() integrationsdk.Operations { return operationsBinding{b} }
func (b *Binding) LocalWorkers() (integrationsdk.LocalWorkers, bool) {
	return b.workers, b.workers != nil
}
func (b *Binding) WebPushSubscriptions() integrationsdk.WebPushSubscriptions {
	return webPushBinding{b}
}
func (*Binding) Close(context.Context) error { return nil }
func (b *Binding) SetHTTPAdapters(adapters []modulehttp.Adapter) {
	b.adapters = append([]modulehttp.Adapter(nil), adapters...)
}
func (b *Binding) HTTPAdapters() []modulehttp.Adapter {
	return append([]modulehttp.Adapter(nil), b.adapters...)
}
func (*Binding) AuthorizationActions() ([]actioncontract.ActionDefinition, error) {
	return integrationsdk.IntegrationAuthorizationActions()
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
func (b requirementsBinding) SynchronizeEventMappings(ctx context.Context, values []integrationsdk.EventMappingRequirement) error {
	v, err := convert[[]integrationmodel.EventMappingRequirement](values, nil)
	if err != nil {
		return err
	}
	return b.service.SynchronizeEventMappings(ctx, v)
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

type operationsBinding struct{ *Binding }

func (b operationsBinding) Call(ctx context.Context, request integrationsdk.ProviderCallRequest) (integrationsdk.ProviderCallResult, error) {
	v, err := convert[integrationmodel.ProviderCallRequest](request, nil)
	if err != nil {
		return integrationsdk.ProviderCallResult{}, err
	}
	result, err := b.service.Call(ctx, v)
	return convert[integrationsdk.ProviderCallResult](result, err)
}
func (b operationsBinding) ListInvocations(ctx context.Context, query integrationsdk.InvocationQuery) ([]integrationsdk.Invocation, error) {
	v, err := convert[integrationmodel.InvocationQuery](query, nil)
	if err != nil {
		return nil, err
	}
	result, err := b.service.ListInvocations(ctx, v)
	return convert[[]integrationsdk.Invocation](result, err)
}
func (b operationsBinding) GetInvocation(ctx context.Context, workspaceID, id string) (integrationsdk.Invocation, error) {
	result, err := b.service.GetInvocation(ctx, workspaceID, id)
	return convert[integrationsdk.Invocation](result, err)
}
func (b operationsBinding) AcceptWebhook(ctx context.Context, request integrationsdk.WebhookRequest) (integrationsdk.WebhookReceipt, error) {
	v, err := convert[integrationmodel.WebhookRequest](request, nil)
	if err != nil {
		return integrationsdk.WebhookReceipt{}, err
	}
	result, err := b.service.AcceptWebhook(ctx, v)
	return convert[integrationsdk.WebhookReceipt](result, err)
}
func (b operationsBinding) ListEvents(ctx context.Context, query integrationsdk.EventQuery) ([]integrationsdk.Event, error) {
	v, err := convert[integrationmodel.EventQuery](query, nil)
	if err != nil {
		return nil, err
	}
	result, err := b.service.ListEvents(ctx, v)
	return convert[[]integrationsdk.Event](result, err)
}
func (b operationsBinding) GetEvent(ctx context.Context, workspaceID, id string) (integrationsdk.Event, error) {
	result, err := b.service.GetEvent(ctx, workspaceID, id)
	return convert[integrationsdk.Event](result, err)
}
func (b operationsBinding) ReplayEvent(ctx context.Context, workspaceID, id string) (integrationsdk.Event, error) {
	result, err := b.service.ReplayEvent(ctx, workspaceID, id)
	return convert[integrationsdk.Event](result, err)
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
var _ integrationsdk.ManagementBinding = (*Binding)(nil)
var _ integrationsdk.ConnectionAccountsBinding = (*Binding)(nil)
var _ integrationsdk.ConnectionAccountAdministrationBinding = (*Binding)(nil)
var _ integrationsdk.OperationsBinding = (*Binding)(nil)
var _ integrationsdk.LocalWorkerBinding = (*Binding)(nil)
var _ modulehttp.Provider = (*Binding)(nil)

func (b *Binding) OAuthApplications() integrationsdk.OAuthApplications {
	value, _ := b.management.(integrationsdk.OAuthApplications)
	return value
}
func (b *Binding) OAuthAuthorizations() integrationsdk.OAuthAuthorizations {
	value, _ := b.management.(integrationsdk.OAuthAuthorizations)
	return value
}

func (b *Binding) ConnectionAccountWrites() integrationsdk.ConnectionAccountWrites {
	if b.accountWrites == nil {
		return nil
	}
	return accountWritesBinding{service: b.accountWrites}
}
func (b *Binding) SetConnectionAccountWrites(writes *integrationapplication.AccountWriteService) {
	b.accountWrites = writes
}

func (b *Binding) SetSubjectLifecycle(subjects integrationsdk.SubjectLifecycle) {
	b.subjects = subjects
}
func (b *Binding) SubjectLifecycle() integrationsdk.SubjectLifecycle { return b.subjects }

func (b *Binding) BindSubjectLifecyclePersistence(ctx context.Context) error {
	binder, ok := b.subjects.(interface{ BindSubjectLifecyclePersistence(context.Context) error })
	if !ok {
		return fmt.Errorf("Integration shared subject lifecycle persistence unavailable")
	}
	return binder.BindSubjectLifecyclePersistence(ctx)
}

var _ integrationsdk.SubjectLifecycleBinding = (*Binding)(nil)
