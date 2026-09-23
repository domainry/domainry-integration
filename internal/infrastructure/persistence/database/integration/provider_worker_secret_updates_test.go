package integration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type refreshBackgroundProvider struct {
	calls      int
	duringCall func()
}

func (*refreshBackgroundProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "refresh_background", ProviderRevision: "1.0.0", SecretFields: []connector.SecretField{{Key: "token", Name: "Token", Required: true}}, Operations: []connector.OperationDescriptor{{ConnectorKey: "crm", ProviderKey: "refresh_background", Key: "ack", Mode: connector.ModeEnqueue, ContractSHA256: strings.Repeat("b", 64)}}}
}
func (*refreshBackgroundProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, nil
}
func (*refreshBackgroundProvider) BackgroundTasks(connector.Connection) []connector.BackgroundTaskDescriptor {
	return []connector.BackgroundTaskDescriptor{{Key: "poll", StateVersion: 1}}
}
func (p *refreshBackgroundProvider) ProcessBackground(_ context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	if request.Secrets["token"] != "original-token" {
		return connector.BackgroundResult{}, errors.New("unexpected background credential")
	}
	p.calls++
	if p.duringCall != nil {
		p.duringCall()
	}
	return connector.BackgroundResult{State: json.RawMessage(`{"cursor":"unchanged"}`), NextDueAt: request.Now.Add(time.Hour), SecretUpdates: map[string]string{"token": "rotated-token"}}, refreshFollowupFailure
}

func TestBackgroundProviderPersistsRotationOnFailureAndHonorsRevocation(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		name := "failed-request"
		if revoke {
			name = "revoked-during-request"
		}
		t.Run(name, func(t *testing.T) {
			db, dialect := webPushTestDatabase(t, "credential-background-"+name)
			crypt := &refreshTestCipher{}
			resolver := NewSecretResolver(db, dialect, crypt)
			provider := &refreshBackgroundProvider{}
			delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider}, resolver)
			management := NewManagementStore(db, dialect, crypt, delivery)
			if _, err := management.UpsertSecret(t.Context(), "workspace-a", "token", "user-a", integrationsdk.SecretInput{Kind: "token", Value: "original-token"}); err != nil {
				t.Fatal(err)
			}
			refs := map[string]string{"token": "secret:token"}
			if _, err := management.UpsertConnection(t.Context(), "workspace-a", "account", "user-a", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "refresh_background", Status: "active", SecretRefs: refs}); err != nil {
				t.Fatal(err)
			}
			if revoke {
				provider.duringCall = func() {
					if _, err := management.TransitionSecret(t.Context(), "workspace-a", "token", "revoke", "user-a"); err != nil {
						t.Fatal(err)
					}
				}
			}
			workers := NewWorkerStore(db, dialect, delivery, NewOperationsStore(db, dialect, delivery, nil, newTestDefinitionStore()), "runtime-a")
			processed, err := workers.ProcessDueProviderTasks(t.Context(), 1)
			if processed != 1 || !errors.Is(err, refreshFollowupFailure) || provider.calls != 1 {
				t.Fatal("background provider outcome was lost", processed, provider.calls, err)
			}
			if revoke {
				if !errors.Is(err, errProviderSecretChanged) {
					t.Fatal("background revocation race was hidden", err)
				}
				secret, readErr := management.getSecret(t.Context(), "workspace-a", "token")
				if readErr != nil || secret.Status != "revoked" {
					t.Fatal("background refresh reactivated credential", secret, readErr)
				}
			} else {
				values, readErr := resolver.ResolveSecretReferences(t.Context(), "workspace-a", refs)
				if readErr != nil || values["token"] != "rotated-token" {
					t.Fatal("background refresh result was discarded", values, readErr)
				}
			}
		})
	}
}

type refreshReconcileProvider struct {
	reconcileError bool
	calls          int
}

func (*refreshReconcileProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "refresh_reconcile", ProviderRevision: "1.0.0", SecretFields: []connector.SecretField{{Key: "token", Name: "Token", Required: true}}, Operations: []connector.OperationDescriptor{{ConnectorKey: "crm", ProviderKey: "refresh_reconcile", Key: "create", Mode: connector.ModeCall, ContractSHA256: strings.Repeat("d", 64), Reliability: connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyProviderKey, KeyRetentionSeconds: 3600}, Reconciliation: connector.ReconciliationProviderLookup, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}}}
}
func (*refreshReconcileProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, errors.New("synthetic initial uncertain result")
}
func (p *refreshReconcileProvider) Reconcile(_ context.Context, request connector.ReconcileRequest) (connector.ReconcileResult, error) {
	if request.Secrets["token"] != "original-token" {
		return connector.ReconcileResult{}, errors.New("unexpected reconcile credential")
	}
	p.calls++
	result := connector.ReconcileResult{Outcome: connector.ReconciliationFailed, FailureCode: "probe.operation_failed", Result: &connector.CallResult{SecretUpdates: map[string]string{"token": "rotated-token"}}}
	if p.reconcileError {
		return connector.ReconcileResult{Result: result.Result}, refreshFollowupFailure
	}
	return result, nil
}

func TestReconciliationPersistsRotationForFailedOutcomeAndProviderError(t *testing.T) {
	for _, providerError := range []bool{false, true} {
		name := "failed-outcome"
		if providerError {
			name = "provider-error"
		}
		t.Run(name, func(t *testing.T) {
			db, dialect := webPushTestDatabase(t, "credential-reconcile-"+name)
			crypt := &refreshTestCipher{}
			resolver := NewSecretResolver(db, dialect, crypt)
			provider := &refreshReconcileProvider{reconcileError: providerError}
			delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider}, resolver)
			management := NewManagementStore(db, dialect, crypt, delivery)
			if _, err := management.UpsertSecret(t.Context(), "workspace-a", "token", "user-a", integrationsdk.SecretInput{Kind: "token", Value: "original-token"}); err != nil {
				t.Fatal(err)
			}
			refs := map[string]string{"token": "secret:token"}
			if _, err := management.UpsertConnection(t.Context(), "workspace-a", "account", "user-a", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "refresh_reconcile", Status: "active", SecretRefs: refs}); err != nil {
				t.Fatal(err)
			}
			operations := NewOperationsStore(db, dialect, delivery, nil, newTestDefinitionStore())
			_, callErr := operations.Call(t.Context(), integrationmodel.ProviderCallRequest{RequestID: "uncertain-call", WorkspaceID: "workspace-a", ConnectorKey: "crm", ConnectionKey: "account", Operation: "create", Payload: json.RawMessage(`{}`), ActorID: "user-a"})
			if callErr == nil {
				t.Fatal("initial uncertain call unexpectedly succeeded")
			}
			processed, err := NewWorkerStore(db, dialect, delivery, operations, "runtime-a").ProcessDueReconciliations(t.Context(), 1)
			if err != nil || processed != 1 || provider.calls != 1 {
				t.Fatal("reconciliation did not run", processed, provider.calls, err)
			}
			values, err := resolver.ResolveSecretReferences(t.Context(), "workspace-a", refs)
			if err != nil || values["token"] != "rotated-token" {
				t.Fatal("reconciliation refresh result was discarded", values, err)
			}
			invocations, err := operations.ListInvocations(t.Context(), integrationmodel.InvocationQuery{WorkspaceID: "workspace-a", Limit: 10})
			if err != nil || len(invocations) != 1 || invocations[0].Status != "failed" {
				t.Fatal("reconciliation business outcome changed", invocations, err)
			}
		})
	}
}
