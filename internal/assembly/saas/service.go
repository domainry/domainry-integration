package saas

import (
	"context"
	"net/http"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	moduleassembly "github.com/domainry/domainry-integration/internal/assembly/module"
	saashttp "github.com/domainry/domainry-integration/internal/transport/http/saas"
)

type Service struct {
	Binding integrationsdk.Binding
	Handler http.Handler
}

func Open(ctx context.Context, application integrationsdk.ApplicationRef, host modulehost.Host, serviceToken string) (*Service, error) {
	binding, err := moduleassembly.OpenHosted(ctx, application, host, integrationsdk.DeploymentModeSaaS)
	if err != nil {
		return nil, err
	}
	handler, err := saashttp.NewHandler(binding, serviceToken)
	if err != nil {
		_ = binding.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	return &Service{Binding: binding, Handler: handler}, nil
}
func (s *Service) Close(ctx context.Context) error {
	if s == nil || s.Binding == nil {
		return nil
	}
	return s.Binding.Close(ctx)
}
