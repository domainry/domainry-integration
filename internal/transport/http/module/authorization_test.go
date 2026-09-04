package module

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	identitysdk "github.com/domainry/domainry-identity-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

func TestAuthorizeActionRequiresAllScopeForAdministrativeActions(t *testing.T) {
	called := false
	next := func(_ http.ResponseWriter, request *http.Request) {
		called = true
		if _, ok := integrationmodel.AccessScopeFromContext(request.Context()); !ok {
			t.Fatal("authorized scope was not propagated")
		}
	}
	handler := authorizeAction(integrationsdk.ActionIntegrationConnectionsList, next)

	request := httptest.NewRequest(http.MethodGet, "/integration/connections", nil)
	request = request.WithContext(identitysdk.WithRequestIdentity(request.Context(), identitysdk.RequestIdentity{Principal: modulePrincipal("integration.connections", "list", identitysdk.DataScopeOwner)}))
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	if called || recorder.Code != http.StatusForbidden {
		t.Fatalf("owner administrative request called=%v status=%d", called, recorder.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/integration/connections", nil)
	request = request.WithContext(identitysdk.WithRequestIdentity(request.Context(), identitysdk.RequestIdentity{Principal: modulePrincipal("integration.connections", "list", identitysdk.DataScopeAll)}))
	recorder = httptest.NewRecorder()
	handler(recorder, request)
	if !called || recorder.Code != http.StatusOK {
		t.Fatalf("all administrative request called=%v status=%d", called, recorder.Code)
	}
}

func TestAuthorizeActionUsesTheExactPermissionAndAllowsPersonalOwnerScope(t *testing.T) {
	called := false
	handler := authorizeAction(integrationsdk.ActionIntegrationWebPushSubscriptionsList, func(_ http.ResponseWriter, _ *http.Request) { called = true })

	request := httptest.NewRequest(http.MethodGet, "/integration/web-push/subscriptions", nil)
	request = request.WithContext(identitysdk.WithRequestIdentity(request.Context(), identitysdk.RequestIdentity{Principal: modulePrincipal("integration.web_push_subscriptions", "get", identitysdk.DataScopeOwner)}))
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	if called || recorder.Code != http.StatusForbidden {
		t.Fatalf("wrong action called=%v status=%d", called, recorder.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/integration/web-push/subscriptions", nil)
	request = request.WithContext(identitysdk.WithRequestIdentity(request.Context(), identitysdk.RequestIdentity{Principal: modulePrincipal("integration.web_push_subscriptions", "list", identitysdk.DataScopeOwner)}))
	recorder = httptest.NewRecorder()
	handler(recorder, request)
	if !called || recorder.Code != http.StatusOK {
		t.Fatalf("owner personal request called=%v status=%d", called, recorder.Code)
	}
}

func modulePrincipal(resource, action string, dataScope identitysdk.DataScope) identitysdk.Principal {
	bundle := &identitysdk.AccessBundle{
		ContractVersion: identitysdk.CurrentPolicyBundleVersion, AuthorizationRevision: "revision-1", ExpiresAt: time.Now().UTC().Add(time.Hour),
		Subject:        identitysdk.Subject{WorkspaceID: "workspace-a", SubjectID: "user-a", OrgID: "org-a"},
		FunctionGrants: []identitysdk.FunctionGrant{{Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow}},
		DataPolicies:   []identitysdk.DataPolicy{{Key: resource + "." + action, Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow, DataScopes: []identitysdk.DataScope{dataScope}, Predicate: moduleScopePredicate(dataScope)}},
	}
	return identitysdk.Principal{Known: true, WorkspaceID: "workspace-a", UserID: "user-a", AccessBundle: bundle}
}

func moduleScopePredicate(scope identitysdk.DataScope) identitysdk.Predicate {
	if scope == identitysdk.DataScopeOwner {
		return identitysdk.Predicate{Fact: "owner_user_id", Operator: identitysdk.OperatorEqual, Value: "$subject.id"}
	}
	return identitysdk.Predicate{}
}
