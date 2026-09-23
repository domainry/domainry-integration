package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
	sdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration/internal/adapter/accountwrite"
	app "github.com/domainry/domainry-integration/internal/application/integration"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type accountWriteProvider struct {
	calls    atomic.Int64
	during   func(connector.CallRequest)
	callErr  error
	response json.RawMessage
	effect   connector.OperationEffect
	mode     connector.OperationMode
	declared bool
	updates  map[string]string
}

func (p *accountWriteProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "probe", SecretFields: []connector.SecretField{{Key: "token", Required: true}}, Operations: []connector.OperationDescriptor{
		{ConnectorKey: "crm", ProviderKey: "probe", Key: mailwrite.SendOperationKey, ContractSHA256: mailwrite.OperationSHA256(mailwrite.SendOperationKey), Mode: p.mode, Reliability: connector.ReliabilityContract{Effect: p.effect}},
	}}
}
func (p *accountWriteProvider) OAuthOperationScopes(key string) ([][]string, bool) {
	return [][]string{{"mail.send"}}, p.declared && key == mailwrite.SendOperationKey
}
func (p *accountWriteProvider) Call(_ context.Context, r connector.CallRequest) (connector.CallResult, error) {
	p.calls.Add(1)
	if r.Secrets["token"] != "original-token" || !r.Principal.IsAuthenticated || r.Principal.UserID == "" || r.RequestRef != r.Principal.RequestID || !strings.HasPrefix(r.RequestRef, "account-write:") {
		return connector.CallResult{}, errors.New("invalid trusted envelope")
	}
	if p.during != nil {
		p.during(r)
	}
	raw := p.response
	if raw == nil {
		raw, _ = json.Marshal(mailwrite.Result{RequestRef: r.RequestRef, Status: "accepted", Delivery: "unknown", AcceptedAt: "2026-09-12T00:00:00Z"})
	}
	return connector.CallResult{Payload: raw, SecretUpdates: p.updates, ResponseRef: "private-ref"}, p.callErr
}

func accountWriteFixture(t *testing.T) (*ManagementStore, *OperationsStore, *accountWriteProvider, *app.AccountWriteService, model.ConnectionAccountSubject, model.ConnectionAccountWriteRequest) {
	t.Helper()
	store, _ := setupConnectionAccountStore(t)
	p := &accountWriteProvider{effect: connector.EffectWrite, mode: connector.ModeCall, declared: true}
	store.delivery.providers = deliveryTestProviders{p}
	for _, item := range []struct {
		key, owner string
		scope      sdk.ConnectionAccountScope
	}{
		{"personal-a", "user-a", sdk.ConnectionAccountScopePersonal}, {"personal-b", "user-b", sdk.ConnectionAccountScopePersonal}, {"shared", "", sdk.ConnectionAccountScopeWorkspace},
	} {
		createAccountConnection(t, store, item.key, item.key+"-token", item.owner, item.scope)
		if err := store.saveConnectionGrant(t.Context(), "workspace-a", item.key, []string{"mail.send"}); err != nil {
			t.Fatal(err)
		}
	}
	ops := NewOperationsStore(store.transactions, store.dialect, store.delivery, nil, newTestDefinitionStore())
	service := app.NewAccountWriteService(store, accountwrite.Codec{}, ops, ops)
	subject := model.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: model.ConnectionAccountAccess{Personal: true, Workspace: true}}
	access, err := service.AuthorizeConnectionAccountWrite(t.Context(), subject, "shared", model.ConnectionAccountWriteOperation{Operation: mailwrite.SendOperationKey, ContractSHA256: mailwrite.OperationSHA256(mailwrite.SendOperationKey)})
	if err != nil {
		t.Fatal(err)
	}
	request := model.ConnectionAccountWriteRequest{RequestID: "execution-1", ExpectedSource: access.Source, Payload: json.RawMessage(`{"message":{"to":[{"address":"recipient@example.test"}],"cc":[],"bcc":[],"subject":"private subject","text":"private outgoing text"}}`)}
	return store, ops, p, service, subject, request
}

func TestAccountWriteStableIdentityBoundedReceiptAndCurrentAuthorization(t *testing.T) {
	store, ops, p, service, subject, request := accountWriteFixture(t)
	missing, err := service.ReadConnectionAccountWriteReceipt(t.Context(), subject, "shared", request)
	if err != nil || missing.Status != model.AccountWriteNotFound || p.calls.Load() != 0 {
		t.Fatal(missing, err)
	}
	out, err := service.WriteConnectionAccount(t.Context(), subject, "shared", request)
	if err != nil || out.Status != model.AccountWriteSucceeded || len(out.Receipt) == 0 || p.calls.Load() != 1 {
		t.Fatal(out, err)
	}
	invocation, err := ops.GetInvocation(t.Context(), subject.WorkspaceID, out.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(invocation)
	for _, forbidden := range []string{"recipient@example.test", "private subject", "private outgoing text", "original-token", "private-ref", "private-provider-content"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("private content persisted", forbidden)
		}
	}
	// Object order/spacing is not a new action. Empty arrays remain explicit.
	request.Payload = json.RawMessage(`{ "message": {"text":"private outgoing text","subject":"private subject","bcc":[],"cc":[],"to":[{"address":"recipient@example.test"}]}}`)
	again, err := service.WriteConnectionAccount(t.Context(), subject, "shared", request)
	if err != nil || string(again.Receipt) != string(out.Receipt) || again.InvocationID != out.InvocationID || p.calls.Load() != 1 {
		t.Fatal(again, err)
	}
	changed := request
	changed.Payload = json.RawMessage(strings.Replace(string(request.Payload), "private outgoing text", "different authorized text", 1))
	if _, err := service.WriteConnectionAccount(t.Context(), subject, "shared", changed); err == nil || p.calls.Load() != 1 {
		t.Fatal("changed payload reused original identity", err)
	}
	access, err := service.AuthorizeConnectionAccountWrite(t.Context(), subject, "personal-a", request.ExpectedSource.OperationContract())
	if err != nil {
		t.Fatal(err)
	}
	changed = request
	changed.ExpectedSource = access.Source
	if _, err := service.WriteConnectionAccount(t.Context(), subject, "personal-a", changed); err == nil || p.calls.Load() != 1 {
		t.Fatal("changed account reused original identity", err)
	}
	other := subject
	other.UserID = "user-b"
	otherOut, err := service.ReadConnectionAccountWriteReceipt(t.Context(), other, "shared", request)
	if err != nil || otherOut.Status != model.AccountWriteNotFound {
		t.Fatal("another actor saw receipt", otherOut, err)
	}
	if err := replaceWriteGrant(t.Context(), store, "workspace-a", "shared", []string{"mail.read"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReadConnectionAccountWriteReceipt(t.Context(), subject, "shared", request); err == nil || p.calls.Load() != 1 {
		t.Fatal("revoked grant exposed receipt")
	}
}

func TestAccountWriteAuthorityRejectsBeforeProviderIO(t *testing.T) {
	for _, name := range []string{"foreign-personal", "wrong-workspace", "no-access", "wrong-principal", "scope-exceeds-principal", "wrong-source", "wrong-hash", "unknown-operation", "read-effect", "async", "missing-declaration", "missing-grant", "revoked-account", "unknown-payload-field", "invalid-recipient"} {
		t.Run(name, func(t *testing.T) {
			store, _, p, service, subject, request := accountWriteFixture(t)
			ctx, key := t.Context(), "shared"
			switch name {
			case "foreign-personal":
				key = "personal-b"
			case "wrong-workspace":
				subject.WorkspaceID = "workspace-b"
			case "no-access":
				subject.Access = model.ConnectionAccountAccess{}
			case "wrong-principal":
				ctx = accountScope(ctx, "user-b", true)
			case "scope-exceeds-principal":
				ctx = accountScope(ctx, "user-a", false)
			case "wrong-source":
				request.ExpectedSource.AccountUpdatedAt = "old-revision"
			case "wrong-hash":
				request.ExpectedSource.ContractSHA256 = strings.Repeat("a", 64)
			case "unknown-operation":
				request.ExpectedSource.Operation = "arbitrary_write"
			case "read-effect":
				p.effect = connector.EffectRead
			case "async":
				p.mode = connector.ModeEnqueue
			case "missing-declaration":
				p.declared = false
			case "missing-grant":
				if err := replaceWriteGrant(ctx, store, subject.WorkspaceID, key, []string{"mail.read"}); err != nil {
					t.Fatal(err)
				}
			case "revoked-account":
				if _, err := store.RevokeConnectionAccount(ctx, accountReadPublicSubject(subject), key, request.ExpectedSource.AccountUpdatedAt); err != nil {
					t.Fatal(err)
				}
			case "unknown-payload-field":
				request.Payload = json.RawMessage(strings.Replace(string(request.Payload), `"text":`, `"headers":{},"text":`, 1))
			case "invalid-recipient":
				request.Payload = json.RawMessage(strings.Replace(string(request.Payload), "recipient@example.test", "not-an-email", 1))
			}
			if _, err := service.WriteConnectionAccount(ctx, subject, key, request); err == nil || p.calls.Load() != 0 {
				t.Fatal("rejected request reached provider", err, p.calls.Load())
			}
		})
	}
}

func TestAccountWriteExplicitScopeFreeOperationDoesNotRequireGrantRow(t *testing.T) {
	store, ops, _, _, _, request := accountWriteFixture(t)
	if _, err := store.database.ExecContext(t.Context(), "UPDATE _integration_connections SET granted_scopes_json=NULL WHERE workspace_id=? AND connection_key=?", request.ExpectedSource.WorkspaceID, request.ExpectedSource.ConnectionKey); err != nil {
		t.Fatal(err)
	}
	if !ops.accountWriteScopesGranted(t.Context(), request.ExpectedSource, [][]string{{}}) {
		t.Fatal("explicit scope-free operation was rejected without a grant row")
	}
	if ops.accountWriteScopesGranted(t.Context(), request.ExpectedSource, [][]string{{"mail.send"}}) {
		t.Fatal("scoped operation was accepted without a grant row")
	}
}

func TestAccountWriteConcurrentClaimCannotSendTwice(t *testing.T) {
	_, _, p, service, subject, request := accountWriteFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	p.during = func(connector.CallRequest) { close(entered); <-release }
	done := make(chan error, 1)
	go func() {
		out, err := service.WriteConnectionAccount(t.Context(), subject, "shared", request)
		if err == nil && out.Status != model.AccountWriteSucceeded {
			err = errors.New("winner did not succeed")
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("provider was not entered")
	}
	results := make(chan error, 16)
	for range 16 {
		go func() {
			out, err := service.WriteConnectionAccount(t.Context(), subject, "shared", request)
			if err == nil && out.Status != model.AccountWriteUncertain {
				err = errors.New("in-flight claim was replayed")
			}
			results <- err
		}()
	}
	for range 16 {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	close(release)
	if err := <-done; err != nil || p.calls.Load() != 1 {
		t.Fatal(err, p.calls.Load())
	}
}

func TestAccountWriteFailureAndInvalidReceiptsAreNeverReplayed(t *testing.T) {
	for _, name := range []string{"unknown-error", "uncertain", "permanent", "retryable", "wrong-request-ref", "extra-body"} {
		t.Run(name, func(t *testing.T) {
			_, _, p, service, subject, request := accountWriteFixture(t)
			want := model.AccountWriteUncertain
			switch name {
			case "unknown-error":
				p.callErr = errors.New("private upstream error")
			case "uncertain":
				p.callErr = connector.UncertainError("test.lost", errors.New("private text"))
			case "permanent":
				p.callErr = connector.PermanentError("test.denied", errors.New("private text"))
				want = model.AccountWriteFailed
			case "retryable":
				p.callErr = connector.RetryableError("test.limited", errors.New("private text"))
				want = model.AccountWriteFailed
			case "wrong-request-ref":
				p.response = json.RawMessage(`{"request_ref":"foreign","status":"accepted","delivery":"unknown","accepted_at":"2026-09-12T00:00:00Z"}`)
			case "extra-body":
				p.during = func(r connector.CallRequest) {
					p.response = json.RawMessage(`{"request_ref":"` + r.RequestRef + `","status":"accepted","delivery":"unknown","accepted_at":"2026-09-12T00:00:00Z","body":"private outgoing text"}`)
				}
			}
			for range 2 {
				out, err := service.WriteConnectionAccount(t.Context(), subject, "shared", request)
				if err != nil || out.Status != want || len(out.Receipt) != 0 || p.calls.Load() != 1 {
					t.Fatal(out, err, p.calls.Load())
				}
			}
		})
	}
}

func TestAccountWriteDisconnectPersistsSuccessAndRevokeHidesIt(t *testing.T) {
	t.Run("caller-disconnect", func(t *testing.T) {
		_, ops, p, service, subject, request := accountWriteFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		p.during = func(connector.CallRequest) { cancel() }
		if _, err := service.WriteConnectionAccount(ctx, subject, "shared", request); err == nil {
			t.Fatal("canceled response exposed")
		}
		reopened := app.NewAccountWriteService(serviceAuthority{service}, accountwrite.Codec{}, ops, ops)
		out, err := reopened.ReadConnectionAccountWriteReceipt(t.Context(), subject, "shared", request)
		if err != nil || out.Status != model.AccountWriteSucceeded || p.calls.Load() != 1 {
			t.Fatal(out, err)
		}
	})
	t.Run("revoke-during-call", func(t *testing.T) {
		store, _, p, service, subject, request := accountWriteFixture(t)
		p.during = func(connector.CallRequest) {
			if err := replaceWriteGrant(t.Context(), store, subject.WorkspaceID, "shared", []string{"mail.read"}); err != nil {
				t.Error(err)
			}
		}
		if _, err := service.WriteConnectionAccount(t.Context(), subject, "shared", request); err == nil {
			t.Fatal("revoked response exposed")
		}
		if err := replaceWriteGrant(t.Context(), store, subject.WorkspaceID, "shared", []string{"mail.send"}); err != nil {
			t.Fatal(err)
		}
		out, err := service.ReadConnectionAccountWriteReceipt(t.Context(), subject, "shared", request)
		if err != nil || out.Status != model.AccountWriteSucceeded || p.calls.Load() != 1 {
			t.Fatal(out, err)
		}
	})
}

type serviceAuthority struct{ *app.AccountWriteService }

type interruptedWriteLedger struct {
	*OperationsStore
	afterClaim bool
}

func (l interruptedWriteLedger) ClaimAccountWrite(ctx context.Context, claim model.AccountWriteClaim) (model.ConnectionAccountWriteResult, bool, error) {
	out, claimed, err := l.OperationsStore.ClaimAccountWrite(ctx, claim)
	if err == nil && claimed && l.afterClaim {
		return out, false, errors.New("simulated crash after durable claim")
	}
	return out, claimed, err
}
func (l interruptedWriteLedger) FinishAccountWrite(context.Context, model.AccountWriteClaim, model.AccountWriteExecution) (model.ConnectionAccountWriteResult, error) {
	return model.ConnectionAccountWriteResult{}, errors.New("simulated receipt storage failure")
}

func TestAccountWriteInterruptedClaimAndReceiptStorageFailureStayUncertain(t *testing.T) {
	for _, afterClaim := range []bool{true, false} {
		t.Run(fmt.Sprint(afterClaim), func(t *testing.T) {
			store, ops, p, service, subject, request := accountWriteFixture(t)
			interrupted := app.NewAccountWriteService(store, accountwrite.Codec{}, interruptedWriteLedger{ops, afterClaim}, ops)
			if _, err := interrupted.WriteConnectionAccount(t.Context(), subject, "shared", request); err == nil {
				t.Fatal("failure was hidden")
			}
			for range 2 {
				out, err := service.WriteConnectionAccount(t.Context(), subject, "shared", request)
				if err != nil || out.Status != model.AccountWriteUncertain {
					t.Fatal(out, err)
				}
			}
			want := int64(1)
			if afterClaim {
				want = 0
			}
			if p.calls.Load() != want {
				t.Fatal("interrupted action was resent", p.calls.Load())
			}
		})
	}
}

func replaceWriteGrant(ctx context.Context, store *ManagementStore, workspace, key string, scopes []string) error {
	raw, _ := json.Marshal(scopes)
	_, err := store.database.ExecContext(ctx, "UPDATE _integration_connections SET granted_scopes_json=? WHERE workspace_id=? AND connection_key=?", string(raw), workspace, key)
	return err
}

func TestAccountWriteFailuresNeverEnterOrStarveLegacyReconciliation(t *testing.T) {
	store, ops, p, service, subject, request := accountWriteFixture(t)
	p.callErr = connector.PermanentError("test.rejected", errors.New("rejected"))
	for i := range 5 {
		request.RequestID = fmt.Sprintf("failed-%d", i)
		out, err := service.WriteConnectionAccount(t.Context(), subject, "shared", request)
		if err != nil || out.Status != model.AccountWriteFailed {
			t.Fatal(out, err)
		}
	}
	if err := store.delivery.insertInvocation(t.Context(), "call:legacy", model.DeliveryRequest{MessageID: "legacy", WorkspaceID: subject.WorkspaceID, ConnectorKey: "crm", ConnectionKey: "shared", Operation: mailwrite.SendOperationKey}, deliveryConnection{Key: "shared", ProviderKey: "probe"}, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.delivery.finishInvocation(t.Context(), "workspace-a", "call:legacy", "failed", "", ""); err != nil {
		t.Fatal(err)
	}
	worker := NewWorkerStore(store.transactions, store.dialect, store.delivery, ops, "worker")
	values, err := worker.failedInvocationsAcrossWorkspaces(t.Context(), 1, time.Now())
	if err != nil || len(values) != 1 || values[0].ID != "call:legacy" {
		t.Fatal("account writes entered/starved generic reconciliation", values, err)
	}
}
