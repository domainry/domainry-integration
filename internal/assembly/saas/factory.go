package saas

import (
	"context"
	"fmt"

	"github.com/domainry/domainry-foundation/modulehttp"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/saashost"
	integrationhttp "github.com/domainry/domainry-integration/internal/transport/http/module"
)

// Factory decorates the SDK Remote Factory with Integration-owned product
// HTTP. Runtime remains unaware of Integration's concrete transports.
type Factory struct {
	remote integrationsdk.Factory
}

func NewFactory(remote integrationsdk.Factory) *Factory {
	return &Factory{remote: remote}
}

func (*Factory) DeploymentMode() integrationsdk.DeploymentMode {
	return integrationsdk.DeploymentModeSaaS
}

func (f *Factory) OpenSaaS(ctx context.Context, application integrationsdk.ApplicationRef, host saashost.Host) (integrationsdk.Binding, error) {
	if f == nil || f.remote == nil || f.remote.DeploymentMode() != integrationsdk.DeploymentModeSaaS {
		return nil, fmt.Errorf("Integration SaaS Remote Factory is required")
	}
	remote, ok := f.remote.(saashost.Factory)
	if !ok {
		return nil, fmt.Errorf("Integration SaaS Remote Factory does not implement saashost.Factory")
	}
	binding, err := remote.OpenSaaS(ctx, application, host)
	if err != nil {
		return nil, err
	}
	webPush, ok := binding.(integrationsdk.WebPushBinding)
	if !ok || webPush.WebPushSubscriptions() == nil {
		_ = binding.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("Integration SaaS Binding does not expose Web Push")
	}
	management, ok := binding.(integrationsdk.ManagementBinding)
	if !ok || management.Management() == nil {
		_ = binding.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("Integration SaaS Binding does not expose Management")
	}
	operations, ok := binding.(integrationsdk.OperationsBinding)
	if !ok || operations.Operations() == nil {
		_ = binding.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("Integration SaaS Binding does not expose Operations")
	}
	adapter, err := integrationhttp.NewAdapter(binding)
	if err != nil {
		_ = binding.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	return &bindingWithHTTPAdapter{Binding: binding, webPush: webPush.WebPushSubscriptions(), management: management.Management(), operations: operations.Operations(), adapters: []modulehttp.Adapter{adapter}}, nil
}

type bindingWithHTTPAdapter struct {
	integrationsdk.Binding
	webPush    integrationsdk.WebPushSubscriptions
	management integrationsdk.Management
	operations integrationsdk.Operations
	adapters   []modulehttp.Adapter
}

func (b *bindingWithHTTPAdapter) WebPushSubscriptions() integrationsdk.WebPushSubscriptions {
	return b.webPush
}
func (b *bindingWithHTTPAdapter) Management() integrationsdk.Management { return b.management }
func (b *bindingWithHTTPAdapter) Operations() integrationsdk.Operations { return b.operations }
func (b *bindingWithHTTPAdapter) HTTPAdapters() []modulehttp.Adapter {
	return append([]modulehttp.Adapter(nil), b.adapters...)
}

var _ integrationsdk.Factory = (*Factory)(nil)
var _ saashost.Factory = (*Factory)(nil)
var _ integrationsdk.WebPushBinding = (*bindingWithHTTPAdapter)(nil)
var _ integrationsdk.ManagementBinding = (*bindingWithHTTPAdapter)(nil)
var _ integrationsdk.OperationsBinding = (*bindingWithHTTPAdapter)(nil)
var _ modulehttp.Provider = (*bindingWithHTTPAdapter)(nil)
