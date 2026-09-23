package integration

import (
	"encoding/json"
	sdk "github.com/domainry/domainry-integration-sdk"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestConnectionReadinessUsesGrantedScopesAndCurrentCredentialMetadata(t *testing.T) {
	store, provider, subject, _ := setupOAuthStore(t)
	_, state := startOAuthStore(t, store, subject)
	session, err := store.CompleteOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationCallback{State: state, Code: "accepted"})
	if err != nil {
		t.Fatal(err)
	}
	key := session.Account.Key
	list, err := store.ListConnectionAccounts(t.Context(), subject)
	if err != nil || len(list) != 1 || list[0].Readiness == nil || !list[0].Readiness.Available {
		t.Fatalf("readiness list=%+v error=%v", list, err)
	}
	ready := list[0].Readiness
	if len(ready.GrantedScopes) != 1 || ready.GrantedScopes[0] != "read" {
		t.Fatalf("requested write scope mistaken for grant: %+v", ready)
	}
	// Never exchange, refresh or probe as a side effect of catalog queries.
	if provider.calls != 1 {
		t.Fatal("readiness caused network I/O")
	}
	raw, _ := json.Marshal(list)
	for _, private := range []string{"private-access", "private-refresh", "private-client", "secret:", "material:", "fingerprint"} {
		if strings.Contains(string(raw), private) {
			t.Fatalf("safe DTO exposed %s", private)
		}
	}
	connection, err := store.GetConnection(t.Context(), subject.WorkspaceID, key)
	if err != nil {
		t.Fatal(err)
	}
	secretKey := strings.TrimPrefix(connection.SecretRefs["refresh_token"], "secret:")
	for _, test := range []struct{ status, expiry, state string }{
		{"disabled", "", "credential_unavailable"},
		{"active", time.Now().Add(-time.Hour).UTC().Format(time.RFC3339), "credential_expired"},
		{"active", time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "configured"},
	} {
		if _, err = store.UpsertSecret(t.Context(), subject.WorkspaceID, secretKey, subject.UserID, sdk.SecretInput{Kind: "refresh_token", ExpiresAt: test.expiry}); err != nil {
			t.Fatal(err)
		}
		transition := "rotate"
		if test.status == "disabled" {
			transition = "disable"
		}
		if _, err = store.TransitionSecret(t.Context(), subject.WorkspaceID, secretKey, transition, subject.UserID); err != nil {
			t.Fatal(err)
		}
		value, err := store.GetConnectionAccount(t.Context(), subject, key)
		if err != nil || value.Readiness == nil || value.Readiness.State != test.state || value.Readiness.Available != (test.state == "configured") {
			t.Fatalf("state=%s got=%+v error=%v", test.state, value.Readiness, err)
		}
	}
	other := subject
	other.UserID = "other"
	if _, err = store.GetConnectionAccount(t.Context(), other, key); err == nil {
		t.Fatal("readiness bypassed account owner")
	}
	revoked, err := store.RevokeConnectionAccount(t.Context(), subject, key, session.Account.UpdatedAt)
	if err != nil || revoked.Readiness == nil || revoked.Readiness.Available || revoked.Readiness.State != "revoked" {
		t.Fatalf("revoke readiness=%+v error=%v", revoked.Readiness, err)
	}
}

func TestConnectionProbeScopesDoNotInvalidateBusinessGrant(t *testing.T) {
	store, provider, subject, _ := setupOAuthStore(t)
	provider.testScopes, provider.testScopesDeclared = [][]string{{"write"}}, true
	started, err := store.StartOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationInput{ApplicationKey: "google", Name: "partial grant", Scope: sdk.ConnectionAccountScopePersonal, Scopes: []string{"read", "write"}})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := url.Parse(started.AuthorizationURL)
	state := target.Query().Get("state")
	done, err := store.CompleteOAuthAuthorization(t.Context(), subject, sdk.OAuthAuthorizationCallback{State: state, Code: "accepted"})
	if err != nil {
		t.Fatal(err)
	}
	key := done.Account.Key
	assertState := func(want string, allow bool) {
		t.Helper()
		a, err := store.GetConnectionAccount(t.Context(), subject, key)
		if err != nil || a.Readiness == nil || !a.Readiness.Available || a.Readiness.Test == nil || a.Readiness.Test.State != want || a.Readiness.Test.Allowed != allow {
			t.Fatalf("readiness=%+v error=%v", a.Readiness, err)
		}
		before := provider.probes
		result, err := store.TestConnectionAccount(t.Context(), subject, key, sdk.ConnectionTestRequest{})
		if allow {
			if err != nil || !result.Connected || provider.probes != before+1 {
				t.Fatalf("probe=%+v err=%v", result, err)
			}
		} else if err == nil || provider.probes != before {
			t.Fatal("denied probe dispatched")
		}
	}
	assertState("scope_required", false)
	provider.testScopes = [][]string{{"read", "profile"}, {"read"}}
	assertState("ready", true)
	provider.testScopes = [][]string{{"read"}, {"bad scope"}}
	assertState("requirements_invalid", false)
	provider.testScopes = [][]string{{"read"}}
	if _, err = store.database.ExecContext(t.Context(), "UPDATE _integration_connections SET granted_scopes_json=NULL"); err != nil {
		t.Fatal(err)
	}
	assertState("scope_unverified", false)
	if provider.calls != 1 {
		t.Fatal("preflight issued OAuth exchanges")
	}
}
