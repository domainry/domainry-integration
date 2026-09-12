package integration

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

func accountScope(ctx context.Context, user string, unrestricted bool) context.Context {
	return integrationmodel.WithAccessScope(ctx, integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.connection_accounts.test", ActorID: user,
		Unrestricted: unrestricted, AllowedUserIDs: func() []string {
			if unrestricted {
				return nil
			}
			return []string{user}
		}(),
	})
}

func setupConnectionAccountStore(t *testing.T) (*ManagementStore, *SecretResolver) {
	t.Helper()
	database, dialect := webPushTestDatabase(t, "connection-accounts")
	provider := &refreshResultProvider{succeed: true}
	resolver := NewSecretResolver(database, dialect, &refreshTestCipher{})
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider}, resolver)
	return NewManagementStore(database, dialect, &refreshTestCipher{}, delivery), resolver
}

func createAccountConnection(t *testing.T, store *ManagementStore, key, secret, owner string, scope integrationsdk.ConnectionAccountScope) integrationsdk.ConnectionAccount {
	t.Helper()
	admin := accountScope(t.Context(), "admin", true)
	if _, err := store.UpsertSecret(admin, "workspace-a", secret, "ignored", integrationsdk.SecretInput{Kind: "token", Value: "original-token"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertConnection(admin, "workspace-a", key, "ignored", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Name: key, Status: "active", SecretRefs: map[string]string{"token": "secret:" + secret}}); err != nil {
		t.Fatal(err)
	}
	value, err := store.RegisterConnectionAccount(admin, "workspace-a", key, "ignored", integrationsdk.ConnectionAccountRegistration{Scope: scope, OwnerUserID: owner})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestConnectionAccountsSeparatePersonalWorkspaceAndLegacyConnections(t *testing.T) {
	store, _ := setupConnectionAccountStore(t)
	personalA := createAccountConnection(t, store, "personal-a", "token-a", "user-a", integrationsdk.ConnectionAccountScopePersonal)
	createAccountConnection(t, store, "personal-b", "token-b", "user-b", integrationsdk.ConnectionAccountScopePersonal)
	createAccountConnection(t, store, "workspace", "token-workspace", "", integrationsdk.ConnectionAccountScopeWorkspace)
	admin := accountScope(t.Context(), "admin", true)
	if _, err := store.UpsertSecret(admin, "workspace-a", "token-legacy", "ignored", integrationsdk.SecretInput{Kind: "token", Value: "original-token"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertConnection(admin, "workspace-a", "legacy", "ignored", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Status: "active", SecretRefs: map[string]string{"token": "secret:token-legacy"}}); err != nil {
		t.Fatal(err)
	}

	userA := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: integrationsdk.ConnectionAccountAccess{Personal: true}}
	values, err := store.ListConnectionAccounts(accountScope(t.Context(), "user-a", false), userA)
	if err != nil || len(values) != 1 || values[0].Key != "personal-a" {
		t.Fatalf("accounts=%#v err=%v", values, err)
	}
	if personalA.OwnerUserID != "user-a" || personalA.Scope != integrationsdk.ConnectionAccountScopePersonal {
		t.Fatalf("personal account=%#v", personalA)
	}
	if _, err = store.GetConnectionAccount(accountScope(t.Context(), "user-a", false), userA, "personal-b"); !errors.Is(err, errConnectionAccountNotFound) {
		t.Fatalf("foreign personal account error=%v", err)
	}
	if _, err = store.GetConnectionAccount(accountScope(t.Context(), "user-b", false), userA, "personal-a"); err == nil {
		t.Fatal("mismatched authorized principal selected an account")
	}
	raw, _ := json.Marshal(values)
	for _, forbidden := range []string{"secret_refs", "config_json", "original-token", "created_by"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("safe account projection leaked %q: %s", forbidden, raw)
		}
	}
}

func TestConnectionAccountRevokeIsVersionedAtomicAndRejectsForeignOwner(t *testing.T) {
	store, resolver := setupConnectionAccountStore(t)
	personal := createAccountConnection(t, store, "personal-a", "token-a", "user-a", integrationsdk.ConnectionAccountScopePersonal)
	workspace := createAccountConnection(t, store, "workspace", "token-workspace", "", integrationsdk.ConnectionAccountScopeWorkspace)
	userA := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: integrationsdk.ConnectionAccountAccess{Personal: true}}
	userB := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-b", Access: integrationsdk.ConnectionAccountAccess{Personal: true}}

	if _, err := store.RevokeConnectionAccount(accountScope(t.Context(), "user-b", false), userB, personal.Key, personal.UpdatedAt); !errors.Is(err, errConnectionAccountNotFound) {
		t.Fatalf("foreign revoke error=%v", err)
	}
	if _, err := store.RevokeConnectionAccount(accountScope(t.Context(), "user-a", false), userA, personal.Key, "stale"); !errors.Is(err, errConnectionAccountChanged) {
		t.Fatalf("stale revoke error=%v", err)
	}
	if values, err := resolver.ResolveSecretReferences(t.Context(), "workspace-a", map[string]string{"token": "secret:token-a"}); err != nil || values["token"] != "original-token" {
		t.Fatalf("stale revoke changed material: %#v %v", values, err)
	}
	revoked, err := store.RevokeConnectionAccount(accountScope(t.Context(), "user-a", false), userA, personal.Key, personal.UpdatedAt)
	if err != nil || revoked.Status != "revoked" || revoked.UpdatedAt == personal.UpdatedAt {
		t.Fatalf("revoked=%#v err=%v", revoked, err)
	}
	if _, err = resolver.ResolveSecretReferences(t.Context(), "workspace-a", map[string]string{"token": "secret:token-a"}); err == nil {
		t.Fatal("revoked account credential still resolves")
	}
	if _, err = store.RevokeConnectionAccount(accountScope(t.Context(), "user-a", false), userA, workspace.Key, workspace.UpdatedAt); err == nil {
		t.Fatal("personal scope revoked a workspace account")
	}
	adminSubject := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "admin", Access: integrationsdk.ConnectionAccountAccess{Personal: true, Workspace: true}}
	if value, err := store.RevokeConnectionAccount(accountScope(t.Context(), "admin", true), adminSubject, workspace.Key, workspace.UpdatedAt); err != nil || value.Status != "revoked" {
		t.Fatalf("workspace revoke=%#v err=%v", value, err)
	}
}

func TestConnectionAccountCredentialsCannotBeSharedOrReplacedOutsideManagedMaterial(t *testing.T) {
	store, _ := setupConnectionAccountStore(t)
	createAccountConnection(t, store, "personal-a", "token-a", "user-a", integrationsdk.ConnectionAccountScopePersonal)
	admin := accountScope(t.Context(), "admin", true)
	if _, err := store.UpsertConnection(admin, "workspace-a", "personal-b", "ignored", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Status: "active", SecretRefs: map[string]string{"token": "secret:token-a"}}); err == nil {
		t.Fatal("legacy connection aliased a reserved credential")
	}
	if _, err := store.RegisterConnectionAccount(admin, "workspace-a", "personal-b", "ignored", integrationsdk.ConnectionAccountRegistration{Scope: integrationsdk.ConnectionAccountScopePersonal, OwnerUserID: "user-b"}); err == nil {
		t.Fatal("shared managed credential was assigned to two accounts")
	}
	if _, err := store.connectionAccount(t.Context(), "workspace-a", "personal-b", nil); !errors.Is(err, errConnectionAccountNotFound) {
		t.Fatalf("failed registration left an account row: %v", err)
	}
	if _, err := store.UpsertConnection(admin, "workspace-a", "personal-a", "ignored", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Status: "active", SecretRefs: map[string]string{"token": "env:TOKEN"}}); err == nil {
		t.Fatal("published account accepted unmanaged credential replacement")
	}
	connection, err := store.GetConnection(admin, "workspace-a", "personal-a")
	if err != nil || connection.SecretRefs["token"] != "secret:token-a" {
		t.Fatalf("failed replacement was not rolled back: %#v %v", connection, err)
	}
}

func TestConnectionAccountAccessIsExplicitAndCannotExceedCurrentPolicy(t *testing.T) {
	store, _ := setupConnectionAccountStore(t)
	createAccountConnection(t, store, "personal-a", "token-a", "user-a", integrationsdk.ConnectionAccountScopePersonal)
	createAccountConnection(t, store, "workspace", "token-workspace", "", integrationsdk.ConnectionAccountScopeWorkspace)
	subject := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a"}
	values, err := store.ListConnectionAccounts(t.Context(), subject)
	if err != nil || len(values) != 0 {
		t.Fatalf("absent authorization: %v %v", values, err)
	}
	subject.Access = integrationsdk.ConnectionAccountAccess{Personal: true, Workspace: true}
	if _, err = store.ListConnectionAccounts(accountScope(t.Context(), "user-a", false), subject); err == nil {
		t.Fatal("owner scope elevated to workspace")
	}
	values, err = store.ListConnectionAccounts(accountScope(t.Context(), "user-a", true), subject)
	if err != nil || len(values) != 2 {
		t.Fatalf("explicit workspace access: %v %v", values, err)
	}
	denied := integrationmodel.WithAccessScope(t.Context(), integrationmodel.AccessScope{WorkspaceID: "workspace-a", ActorID: "user-a", PermissionKey: "integration.connection_accounts.list", Unrestricted: true, DeniedAll: true})
	if _, err = store.ListConnectionAccounts(denied, subject); err == nil {
		t.Fatal("explicit deny was ignored")
	}
	subject.WorkspaceID = "workspace-b"
	if _, err = store.ListConnectionAccounts(accountScope(t.Context(), "user-a", true), subject); err == nil {
		t.Fatal("cross workspace scope accepted")
	}
}

func TestConnectionAccountOwnershipCannotChangeAndRevokeCanRecoverLostResponse(t *testing.T) {
	store, resolver := setupConnectionAccountStore(t)
	account := createAccountConnection(t, store, "personal-a", "token-a", "user-a", integrationsdk.ConnectionAccountScopePersonal)
	admin := accountScope(t.Context(), "admin", true)
	for _, registration := range []integrationsdk.ConnectionAccountRegistration{{Scope: integrationsdk.ConnectionAccountScopePersonal, OwnerUserID: "user-b"}, {Scope: integrationsdk.ConnectionAccountScopeWorkspace}} {
		if _, err := store.RegisterConnectionAccount(admin, "workspace-a", account.Key, "ignored", registration); err == nil {
			t.Fatal("ownership mutated")
		}
	}
	if _, err := store.RegisterConnectionAccount(admin, "workspace-a", account.Key, "ignored", integrationsdk.ConnectionAccountRegistration{Scope: integrationsdk.ConnectionAccountScopePersonal, OwnerUserID: "user-a"}); err != nil {
		t.Fatal(err)
	}
	subject := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: integrationsdk.ConnectionAccountAccess{Personal: true}}
	first, err := store.RevokeConnectionAccount(accountScope(t.Context(), "user-a", false), subject, account.Key, account.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.RevokeConnectionAccount(accountScope(t.Context(), "user-a", false), subject, account.Key, account.UpdatedAt)
	if err != nil || !reflect.DeepEqual(replay, first) {
		t.Fatalf("lost response retry: %v %v", replay, err)
	}
	if _, err = resolver.ResolveSecretReferences(t.Context(), "workspace-a", map[string]string{"token": "secret:token-a"}); err == nil {
		t.Fatal("revoked material resolved")
	}
}

func TestConnectionAccountTestRejectsBusinessInputsAndHidesProviderDetails(t *testing.T) {
	store, _ := setupConnectionAccountStore(t)
	account := createAccountConnection(t, store, "personal-a", "token-a", "user-a", integrationsdk.ConnectionAccountScopePersonal)
	subject := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: integrationsdk.ConnectionAccountAccess{Personal: true}}
	for _, request := range []integrationsdk.ConnectionTestRequest{{Operation: "send_mail"}, {Payload: json.RawMessage(`{}`)}, {Input: json.RawMessage(`{}`)}, {Confirm: true}} {
		if _, err := store.TestConnectionAccount(accountScope(t.Context(), "user-a", false), subject, account.Key, request); err == nil {
			t.Fatal("business input accepted")
		}
	}
	value, err := store.TestConnectionAccount(accountScope(t.Context(), "user-a", false), subject, account.Key, integrationsdk.ConnectionTestRequest{})
	if err != nil || !value.Connected {
		t.Fatalf("connection test: %v %v", value, err)
	}
	raw, _ := json.Marshal(value)
	for _, field := range []string{"response", "secret_refs", "config_json", "original-token", "rotated-token"} {
		if strings.Contains(string(raw), field) {
			t.Fatalf("account test exposed %s", field)
		}
	}
	if _, err = store.RevokeConnectionAccount(accountScope(t.Context(), "user-a", false), subject, account.Key, account.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err = store.TestConnectionAccount(accountScope(t.Context(), "user-a", false), subject, account.Key, integrationsdk.ConnectionTestRequest{}); err == nil {
		t.Fatal("revoked account tested")
	}
}

func TestConnectionAccountRejectsCredentialsAlreadyAliasedByLegacyConnections(t *testing.T) {
	store, _ := setupConnectionAccountStore(t)
	admin := accountScope(t.Context(), "admin", true)
	if _, err := store.UpsertSecret(admin, "workspace-a", "aliased", "admin", integrationsdk.SecretInput{Kind: "token", Value: "original-token"}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"legacy-a", "legacy-b"} {
		if _, err := store.UpsertConnection(admin, "workspace-a", key, "admin", integrationsdk.ConnectionInput{ConnectorKey: "crm", ProviderKey: "probe", Status: "active", SecretRefs: map[string]string{"token": "secret:aliased"}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.RegisterConnectionAccount(admin, "workspace-a", "legacy-a", "admin", integrationsdk.ConnectionAccountRegistration{Scope: integrationsdk.ConnectionAccountScopePersonal, OwnerUserID: "user-a"}); err == nil {
		t.Fatal("existing legacy alias was published")
	}
	if _, err := store.connectionAccount(t.Context(), "workspace-a", "legacy-a", nil); !errors.Is(err, errConnectionAccountNotFound) {
		t.Fatal("failed publication persisted ownership", err)
	}
}

func TestConnectionAccountRevocationDuringProviderTestCannotReactivateCredentials(t *testing.T) {
	store, resolver := setupConnectionAccountStore(t)
	account := createAccountConnection(t, store, "personal-a", "token-a", "user-a", integrationsdk.ConnectionAccountScopePersonal)
	subject := integrationsdk.ConnectionAccountSubject{WorkspaceID: "workspace-a", UserID: "user-a", Access: integrationsdk.ConnectionAccountAccess{Personal: true}}
	provider, _ := store.delivery.providers.Provider("crm", "probe")
	provider.(*refreshResultProvider).duringCall = func() {
		if _, err := store.RevokeConnectionAccount(accountScope(t.Context(), "user-a", false), subject, account.Key, account.UpdatedAt); err != nil {
			t.Fatal(err)
		}
	}
	if value, err := store.TestConnectionAccount(accountScope(t.Context(), "user-a", false), subject, account.Key, integrationsdk.ConnectionTestRequest{}); err == nil || value.Connected {
		t.Fatalf("revocation lost to late refresh: %v %v", value, err)
	}
	current, err := store.GetConnectionAccount(accountScope(t.Context(), "user-a", false), subject, account.Key)
	if err != nil || current.Status != "revoked" {
		t.Fatalf("revocation state=%v err=%v", current, err)
	}
	if _, err = resolver.ResolveSecretReferences(t.Context(), "workspace-a", map[string]string{"token": "secret:token-a"}); err == nil {
		t.Fatal("late refresh reactivated credentials")
	}
}
