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
	surface, err := integrationhttp.NewSurface(binding)
	if err != nil {
		_ = binding.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	return &bindingWithHTTPSurface{Binding: binding, webPush: webPush.WebPushSubscriptions(), surfaces: []modulehttp.Surface{surface}}, nil
}

type bindingWithHTTPSurface struct {
	integrationsdk.Binding
	webPush  integrationsdk.WebPushSubscriptions
	surfaces []modulehttp.Surface
}

func (b *bindingWithHTTPSurface) WebPushSubscriptions() integrationsdk.WebPushSubscriptions {
	return b.webPush
}
func (b *bindingWithHTTPSurface) HTTPSurfaces() []modulehttp.Surface {
	return append([]modulehttp.Surface(nil), b.surfaces...)
}

var _ integrationsdk.Factory = (*Factory)(nil)
var _ saashost.Factory = (*Factory)(nil)
var _ integrationsdk.WebPushBinding = (*bindingWithHTTPSurface)(nil)
var _ modulehttp.Provider = (*bindingWithHTTPSurface)(nil)
