package integration

import (
	"context"
	"errors"
	"testing"

	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type accountWriteSecretResolver struct {
	*SecretResolver
	afterResolve func()
	failUpdates  bool
}

func (s accountWriteSecretResolver) ResolveSecretReferencesSnapshot(ctx context.Context, workspace string, refs map[string]string) (resolvedProviderSecrets, error) {
	value, err := s.SecretResolver.ResolveSecretReferencesSnapshot(ctx, workspace, refs)
	if err == nil && s.afterResolve != nil {
		s.afterResolve()
	}
	return value, err
}
func (s accountWriteSecretResolver) ApplySecretUpdatesIfCurrent(ctx context.Context, workspace string, refs map[string]string, versions map[string]providerSecretVersion, updates map[string]string) error {
	if s.failUpdates {
		return errors.New("simulated private credential storage failure")
	}
	return s.SecretResolver.ApplySecretUpdatesIfCurrent(ctx, workspace, refs, versions, updates)
}

func TestAccountWriteCredentialPersistenceCannotEraseKnownSuccess(t *testing.T) {
	store, ops, p, service, subject, request := accountWriteFixture(t)
	store.delivery.secrets = accountWriteSecretResolver{SecretResolver: store.delivery.secrets.(*SecretResolver), failUpdates: true}
	p.updates = map[string]string{"token": "rotated-token"}
	out, err := service.WriteConnectionAccount(t.Context(), subject, "shared", request)
	if err != nil || out.Status != model.AccountWriteSucceeded || p.calls.Load() != 1 {
		t.Fatal(out, err)
	}
	invocation, err := ops.GetInvocation(t.Context(), subject.WorkspaceID, out.InvocationID)
	if err != nil || invocation.Metadata["credential_update_failed"] != true {
		t.Fatal(invocation.Metadata, err)
	}
	out, err = service.WriteConnectionAccount(t.Context(), subject, "shared", request)
	if err != nil || out.Status != model.AccountWriteSucceeded || p.calls.Load() != 1 {
		t.Fatal(out, err)
	}
}

func TestAccountWriteRechecksGrantAfterResolvingCredentials(t *testing.T) {
	store, _, p, service, subject, request := accountWriteFixture(t)
	store.delivery.secrets = accountWriteSecretResolver{SecretResolver: store.delivery.secrets.(*SecretResolver), afterResolve: func() {
		if err := replaceWriteGrant(t.Context(), store, subject.WorkspaceID, "shared", []string{"mail.read"}); err != nil {
			t.Error(err)
		}
	}}
	if _, err := service.WriteConnectionAccount(t.Context(), subject, "shared", request); err == nil || p.calls.Load() != 0 {
		t.Fatal("grant revoked before I/O was ignored", err)
	}
	if err := replaceWriteGrant(t.Context(), store, subject.WorkspaceID, "shared", []string{"mail.send"}); err != nil {
		t.Fatal(err)
	}
	out, err := service.ReadConnectionAccountWriteReceipt(t.Context(), subject, "shared", request)
	if err != nil || out.Status != model.AccountWriteFailed || p.calls.Load() != 0 {
		t.Fatal(out, err)
	}
}
