package saas

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	moduleassembly "github.com/domainry/domainry-integration/internal/assembly/module"
	saashttp "github.com/domainry/domainry-integration/internal/transport/http/saas"
)

type Service struct {
	Binding integrationsdk.Binding
	Handler http.Handler
	Workers integrationsdk.LocalWorkers
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
	workerBinding, ok := binding.(integrationsdk.LocalWorkerBinding)
	if !ok {
		_ = binding.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("Integration SaaS local binding exposes no worker boundary")
	}
	workers, ok := workerBinding.LocalWorkers()
	if !ok || workers == nil {
		_ = binding.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("Integration SaaS local binding exposes no workers")
	}
	return &Service{Binding: binding, Handler: handler, Workers: workers}, nil
}

func (s *Service) StartWorkers(ctx context.Context, interval time.Duration, limit int) <-chan struct{} {
	done := make(chan struct{})
	if interval <= 0 {
		interval = time.Second
	}
	if limit <= 0 || limit > 500 {
		limit = 25
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		processes := []struct {
			name    string
			process func(context.Context, int) (int, error)
		}{{"events", s.Workers.ProcessDueEvents}, {"provider_tasks", s.Workers.ProcessDueProviderTasks}, {"reconciliations", s.Workers.ProcessDueReconciliations}, {"credential_expirations", s.Workers.ProcessDueCredentialExpirations}}
		for {
			for _, item := range processes {
				if _, err := item.process(ctx, limit); err != nil && ctx.Err() == nil {
					log.Printf("Integration SaaS %s worker failed: %v", item.name, err)
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}
func (s *Service) Close(ctx context.Context) error {
	if s == nil || s.Binding == nil {
		return nil
	}
	return s.Binding.Close(ctx)
}
