package integration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	sdk "github.com/domainry/domainry-integration-sdk"
	app "github.com/domainry/domainry-integration/internal/application/integration"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type accountReadProvider struct {
	calls    int
	hash     string
	during   func()
	effect   connector.OperationEffect
	mode     connector.OperationMode
	scopes   [][]string
	declared bool
}

func (p *accountReadProvider) Descriptor() connector.ProviderDescriptor {
	hash := p.hash
	if hash == "" {
		hash = strings.Repeat("c", 64)
	}
	return connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "probe", SecretFields: []connector.SecretField{{Key: "token", Required: true}}, Operations: []connector.OperationDescriptor{{ConnectorKey: "crm", ProviderKey: "probe", Key: "lookup", Mode: p.mode, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: p.effect}}}}
}
func (p *accountReadProvider) OAuthOperationScopes(key string) ([][]string, bool) {
	return p.scopes, p.declared && key == "lookup"
}
func (p *accountReadProvider) Call(_ context.Context, r connector.CallRequest) (connector.CallResult, error) {
	p.calls++
	if r.Secrets["token"] != "original-token" || !r.Principal.IsAuthenticated || r.Principal.UserID == "" {
		return connector.CallResult{}, errors.New("missing private host credential/principal")
	}
	if p.during != nil {
		p.during()
	}
	return connector.CallResult{Payload: json.RawMessage(`{"title":"sensitive event content"}`), ResponseRef: "private-provider-ref"}, nil
}

func accountReadFixture(t *testing.T) (*ManagementStore, *OperationsStore, *accountReadProvider, *app.AccountReadService, model.ConnectionAccountSubject, model.ConnectionAccountReadRequest) {
	t.Helper()
	store, _ := setupConnectionAccountStore(t)
	p := &accountReadProvider{effect: connector.EffectRead, mode: connector.ModeCall, scopes: [][]string{{"calendar.read"}}, declared: true}
	store.delivery.providers = deliveryTestProviders{p}
	createAccountConnection(t, store, "personal-a", "token-a", "user-a", sdk.ConnectionAccountScopePersonal)
	createAccountConnection(t, store, "personal-b", "token-b", "user-b", sdk.ConnectionAccountScopePersonal)
	createAccountConnection(t, store, "shared", "token-shared", "", sdk.ConnectionAccountScopeWorkspace)
	for _, key := range []string{"personal-a", "personal-b", "shared"} {
		if err := store.saveConnectionGrant(t.Context(), "workspace-a", key, []string{"calendar.read"}); err != nil {
			t.Fatal(err)
		}
	}
	ops := NewOperationsStore(store.transactions, store.dialect, store.delivery, nil, newTestDefinitionStore())
	service := app.NewAccountReadService(store, ops)
	subject := model.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: model.ConnectionAccountAccess{Personal: true, Workspace: true}}
	request := model.ConnectionAccountReadRequest{RequestID: "read-1", Operation: "lookup", ContractSHA256: strings.Repeat("c", 64), Payload: json.RawMessage(`{"query":"private query text"}`)}
	return store, ops, p, service, subject, request
}

func TestAccountReadsUseOwnedCredentialsAndSensitiveActorBoundEvidence(t *testing.T) {
	store, ops, p, service, subject, request := accountReadFixture(t)
	out, err := service.ReadConnectionAccount(t.Context(), subject, "shared", request)
	if err != nil || !out.PayloadAvailable || len(out.Payload) == 0 || out.Source.ConnectionKey != "shared" || out.ReadAt == "" || p.calls != 1 {
		t.Fatal(out, err)
	}
	invocation, err := ops.GetInvocation(t.Context(), "workspace-a", out.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(invocation)
	for _, forbidden := range []string{"sensitive event content", "private query text", "original-token", "private-provider-ref"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("sensitive body entered invocation evidence", forbidden)
		}
	}
	replay, err := service.ReadConnectionAccount(t.Context(), subject, "shared", request)
	if err != nil || replay.PayloadAvailable || len(replay.Payload) != 0 || replay.InvocationID != out.InvocationID || p.calls != 1 {
		t.Fatal("sensitive replay disclosed body or repeated I/O", replay, err)
	}
	subject.UserID = "user-b"
	other, err := service.ReadConnectionAccount(t.Context(), subject, "shared", request)
	if err != nil || !other.PayloadAvailable || other.InvocationID == out.InvocationID || p.calls != 2 {
		t.Fatal("actors shared replay evidence", other, err)
	}
	request.Payload = json.RawMessage(`{"query":"other query"}`)
	changed, err := service.ReadConnectionAccount(t.Context(), subject, "shared", request)
	if err != nil || changed.InvocationID == other.InvocationID || p.calls != 3 {
		t.Fatal("different input reused evidence", changed, err)
	}
	if _, err := store.database.ExecContext(t.Context(), "UPDATE _integration_connections SET granted_scopes_json='[]' WHERE connection_key='shared'"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReadConnectionAccount(t.Context(), subject, "shared", request); err == nil || p.calls != 3 {
		t.Fatal("revoked grant exposed cached result")
	}
}

func TestAccountReadAuthorityRejectsBeforeAnyProviderIO(t *testing.T) {
	for _, name := range []string{"foreign-personal", "wrong-workspace", "no-access", "scope-exceeds-principal", "wrong-principal", "wrong-contract", "unknown-operation", "write", "async", "missing-declaration", "invalid-declaration", "missing-grant", "busy-only", "revoked", "credential-revoked"} {
		t.Run(name, func(t *testing.T) {
			store, _, p, service, subject, request := accountReadFixture(t)
			ctx := t.Context()
			key := "personal-a"
			switch name {
			case "foreign-personal":
				key = "personal-b"
			case "wrong-workspace":
				subject.WorkspaceID = "workspace-b"
			case "no-access":
				subject.Access = model.ConnectionAccountAccess{}
			case "scope-exceeds-principal":
				ctx = accountScope(ctx, "user-a", false)
			case "wrong-principal":
				ctx = accountScope(ctx, "user-b", true)
			case "wrong-contract":
				request.ContractSHA256 = strings.Repeat("d", 64)
			case "unknown-operation":
				request.Operation = "send"
			case "write":
				p.effect = connector.EffectWrite
			case "async":
				p.mode = connector.ModeEnqueue
			case "missing-declaration":
				p.declared = false
			case "invalid-declaration":
				p.scopes = [][]string{{"bad scope"}}
			case "missing-grant":
				_, err := store.database.ExecContext(ctx, "UPDATE _integration_connections SET granted_scopes_json=NULL")
				if err != nil {
					t.Fatal(err)
				}
			case "busy-only":
				_, err := store.database.ExecContext(ctx, `UPDATE _integration_connections SET granted_scopes_json='["calendar.freebusy"]'`)
				if err != nil {
					t.Fatal(err)
				}
			case "revoked":
				a, err := store.GetConnectionAccount(ctx, accountReadPublicSubject(subject), key)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.RevokeConnectionAccount(ctx, accountReadPublicSubject(subject), key, a.UpdatedAt); err != nil {
					t.Fatal(err)
				}
			case "credential-revoked":
				_, err := store.database.ExecContext(ctx, "UPDATE _integration_secrets SET status='revoked'")
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := service.ReadConnectionAccount(ctx, subject, key, request); err == nil || p.calls != 0 {
				t.Fatal("account read escaped preflight", err, p.calls)
			}
		})
	}
}

type accountReadBeforeCall struct {
	calls  *OperationsStore
	before func()
}

func (p accountReadBeforeCall) Call(ctx context.Context, r model.ProviderCallRequest) (model.ProviderCallResult, error) {
	p.before()
	return p.calls.Call(ctx, r)
}

func TestAccountReadsRejectChangedTargetBeforeExecutionAndRevokeBeforeReturn(t *testing.T) {
	for _, name := range []string{"before-revision", "before-effect", "before-contract", "after-grant", "after-revoke", "after-ownership"} {
		t.Run(name, func(t *testing.T) {
			store, ops, p, service, subject, request := accountReadFixture(t)
			mutate := func() {
				var err error
				switch name {
				case "before-revision":
					_, err = store.database.ExecContext(t.Context(), "UPDATE _integration_connections SET updated_at='2026-09-11T00:00:00Z' WHERE connection_key='personal-a'")
				case "before-effect":
					p.effect = connector.EffectWrite
				case "before-contract":
					p.hash = strings.Repeat("d", 64)
				case "after-grant":
					_, err = store.database.ExecContext(t.Context(), `UPDATE _integration_connections SET granted_scopes_json='["calendar.freebusy"]' WHERE connection_key='personal-a'`)
				case "after-revoke":
					a, e := store.GetConnectionAccount(t.Context(), accountReadPublicSubject(subject), "personal-a")
					if e != nil {
						t.Fatal(e)
					}
					_, err = store.RevokeConnectionAccount(t.Context(), accountReadPublicSubject(subject), "personal-a", a.UpdatedAt)
				case "after-ownership":
					_, err = store.database.ExecContext(t.Context(), "UPDATE _integration_connection_accounts SET owner_user_id='user-b' WHERE connection_key='personal-a'")
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if strings.HasPrefix(name, "before-") {
				service = app.NewAccountReadService(store, accountReadBeforeCall{calls: ops, before: mutate})
			} else {
				p.during = mutate
			}
			out, err := service.ReadConnectionAccount(t.Context(), subject, "personal-a", request)
			if err == nil || len(out.Payload) != 0 {
				t.Fatal("changed target disclosed event", out, err)
			}
			if strings.HasPrefix(name, "before-") && p.calls != 0 {
				t.Fatal("changed target reached provider")
			}
			if strings.HasPrefix(name, "after-") && p.calls != 1 {
				t.Fatal("post-check test did not execute provider")
			}
		})
	}
}

func accountReadPublicSubject(s model.ConnectionAccountSubject) sdk.ConnectionAccountSubject {
	return sdk.ConnectionAccountSubject{WorkspaceID: s.WorkspaceID, UserID: s.UserID, Access: sdk.ConnectionAccountAccess{Personal: s.Access.Personal, Workspace: s.Access.Workspace}}
}
