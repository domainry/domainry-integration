package module

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	identitysdk "github.com/domainry/domainry-identity-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

type connectionAccountHTTPFixture struct {
	subject integrationsdk.ConnectionAccountSubject
}

func (f *connectionAccountHTTPFixture) ListConnectionAccounts(_ context.Context, subject integrationsdk.ConnectionAccountSubject) ([]integrationsdk.ConnectionAccount, error) {
	f.subject = subject
	return []integrationsdk.ConnectionAccount{{Key: "personal", ConnectorKey: "google_workspace", ProviderKey: "google", Scope: integrationsdk.ConnectionAccountScopePersonal, OwnerUserID: subject.UserID, Status: "active", Background: []integrationsdk.ConnectionAccountBackgroundTask{{TaskKey: "gmail_sync", Status: "ready", LastSuccessAt: "2026-09-27T07:00:00Z"}}}}, nil
}
func (f *connectionAccountHTTPFixture) GetConnectionAccount(_ context.Context, subject integrationsdk.ConnectionAccountSubject, key string) (integrationsdk.ConnectionAccount, error) {
	f.subject = subject
	return integrationsdk.ConnectionAccount{Key: key, ConnectorKey: "google_workspace", ProviderKey: "google", Scope: integrationsdk.ConnectionAccountScopePersonal, OwnerUserID: subject.UserID, Status: "active"}, nil
}
func (f *connectionAccountHTTPFixture) TestConnectionAccount(_ context.Context, subject integrationsdk.ConnectionAccountSubject, key string, _ integrationsdk.ConnectionTestRequest) (integrationsdk.ConnectionAccountTestResult, error) {
	f.subject = subject
	return integrationsdk.ConnectionAccountTestResult{Account: integrationsdk.ConnectionAccount{Key: key, Scope: integrationsdk.ConnectionAccountScopePersonal, OwnerUserID: subject.UserID, Status: "active"}, Operation: "test_connection"}, nil
}
func (f *connectionAccountHTTPFixture) RetryConnectionAccountBackgroundTask(_ context.Context, subject integrationsdk.ConnectionAccountSubject, _, taskKey string, _ integrationsdk.ConnectionAccountBackgroundRetryRequest) (integrationsdk.ConnectionAccountBackgroundTask, error) {
	f.subject = subject
	return integrationsdk.ConnectionAccountBackgroundTask{TaskKey: taskKey, Status: "ready"}, nil
}
func (f *connectionAccountHTTPFixture) RevokeConnectionAccount(_ context.Context, subject integrationsdk.ConnectionAccountSubject, key, _ string) (integrationsdk.ConnectionAccount, error) {
	f.subject = subject
	return integrationsdk.ConnectionAccount{Key: key, Scope: integrationsdk.ConnectionAccountScopePersonal, OwnerUserID: subject.UserID, Status: "revoked"}, nil
}

func TestConnectionAccountHandlerUsesAuthenticatedPrincipalAndSafeProjection(t *testing.T) {
	fixture := &connectionAccountHTTPFixture{}
	h := &handler{accounts: fixture}
	request := httptest.NewRequest(http.MethodGet, "/integration/connection-accounts?workspace_id=forged&user_id=forged&allow_personal=true&allow_workspace=true", nil)
	request = request.WithContext(identitysdk.WithRequestIdentity(request.Context(), identitysdk.RequestIdentity{Principal: modulePrincipal("integration.connection_accounts", "list", identitysdk.DataScopeOwner)}))
	recorder := httptest.NewRecorder()
	authorizeAction(integrationsdk.ActionIntegrationConnectionAccountsList, h.listConnectionAccounts)(recorder, request)
	if recorder.Code != http.StatusOK || fixture.subject.WorkspaceID != "workspace-a" || fixture.subject.UserID != "user-a" || !fixture.subject.Access.Personal || fixture.subject.Access.Workspace {
		t.Fatalf("status=%d subject=%#v body=%s", recorder.Code, fixture.subject, recorder.Body.String())
	}
	var body map[string]any
	if json.Unmarshal(recorder.Body.Bytes(), &body) != nil {
		t.Fatal("invalid account response")
	}
	raw := recorder.Body.String()
	if !strings.Contains(raw, `"task_key":"gmail_sync"`) || !strings.Contains(raw, `"last_success_at":"2026-09-27T07:00:00Z"`) {
		t.Fatalf("account response omitted safe provider-run state: %s", raw)
	}
	for _, forbidden := range []string{"secret_refs", "config_json", "created_by"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("account response leaked %q: %s", forbidden, raw)
		}
	}
}

func TestConnectionAccountBackgroundRetryUsesAuthenticatedPrincipal(t *testing.T) {
	fixture := &connectionAccountHTTPFixture{}
	h := &handler{accounts: fixture}
	request := httptest.NewRequest(http.MethodPost, "/integration/connection-accounts/personal/background/gmail_sync/retry?workspace_id=forged&user_id=forged", strings.NewReader(`{"expected_updated_at":"2026-09-27T00:00:00Z"}`))
	request.SetPathValue("connectionKey", "personal")
	request.SetPathValue("taskKey", "gmail_sync")
	request = request.WithContext(identitysdk.WithRequestIdentity(request.Context(), identitysdk.RequestIdentity{Principal: modulePrincipal("integration.connection_accounts", "retry_background", identitysdk.DataScopeOwner)}))
	recorder := httptest.NewRecorder()
	authorizeAction(integrationsdk.ActionIntegrationConnectionAccountsRetryBackground, h.retryConnectionAccountBackgroundTask)(recorder, request)
	if recorder.Code != http.StatusAccepted || fixture.subject.WorkspaceID != "workspace-a" || fixture.subject.UserID != "user-a" || !fixture.subject.Access.Personal || fixture.subject.Access.Workspace || !strings.Contains(recorder.Body.String(), `"task_key":"gmail_sync"`) {
		t.Fatalf("status=%d subject=%#v body=%s", recorder.Code, fixture.subject, recorder.Body.String())
	}
}

var _ integrationsdk.ConnectionAccounts = (*connectionAccountHTTPFixture)(nil)
