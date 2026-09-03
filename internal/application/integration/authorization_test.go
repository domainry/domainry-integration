package integrationapplication

import (
	"testing"
	"time"

	identitysdk "github.com/domainry/domainry-identity-sdk"
)

func TestAuthorizeDataAccessRequiresExactFunctionAndDataPermission(t *testing.T) {
	principal := authorizationPrincipal("integration.connections", "list", identitysdk.DataScopeAll)

	scope, err := AuthorizeDataAccess(principal, "integration.connections.list")
	if err != nil {
		t.Fatal(err)
	}
	if !scope.Unrestricted || scope.WorkspaceID != "workspace-a" || scope.ActorID != "user-a" || scope.PermissionKey != "integration.connections.list" {
		t.Fatalf("scope=%#v", scope)
	}

	for name, mutate := range map[string]func(*identitysdk.Principal){
		"missing function": func(value *identitysdk.Principal) { value.AccessBundle.FunctionGrants = nil },
		"missing data":     func(value *identitysdk.Principal) { value.AccessBundle.DataPolicies = nil },
		"other action": func(value *identitysdk.Principal) {
			value.AccessBundle.FunctionGrants[0].Action = "get"
			value.AccessBundle.DataPolicies[0].Action = "get"
		},
		"wildcard": func(value *identitysdk.Principal) {
			value.AccessBundle.FunctionGrants[0].Resource = "*"
			value.AccessBundle.FunctionGrants[0].Action = "*"
			value.AccessBundle.DataPolicies[0].Resource = "*"
			value.AccessBundle.DataPolicies[0].Action = "*"
		},
		"custom predicate": func(value *identitysdk.Principal) {
			value.AccessBundle.DataPolicies[0].Predicate = identitysdk.Predicate{Fact: "owner_id", Operator: identitysdk.OperatorEqual, Value: "user-a"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := authorizationPrincipal("integration.connections", "list", identitysdk.DataScopeAll)
			mutate(&value)
			if _, err := AuthorizeDataAccess(value, "integration.connections.list"); err == nil {
				t.Fatal("expected authorization to fail closed")
			}
		})
	}
}

func TestAuthorizeDataAccessProjectsClosedCanonicalScopes(t *testing.T) {
	tests := []struct {
		name          string
		dataScope     identitysdk.DataScope
		users         []string
		organizations []string
	}{
		{name: "owner", dataScope: identitysdk.DataScopeOwner, users: []string{"user-a"}},
		{name: "org", dataScope: identitysdk.DataScopeOrg, organizations: []string{"org-a"}},
		{name: "org child", dataScope: identitysdk.DataScopeOrgChild, organizations: []string{"org-a", "org-child"}},
		{name: "target org", dataScope: identitysdk.DataScopeTargetOrg, organizations: []string{"support-a", "support-child"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scope, err := AuthorizeDataAccess(authorizationPrincipal("integration.web_push_subscriptions", "list", test.dataScope), "integration.web_push_subscriptions.list")
			if err != nil {
				t.Fatal(err)
			}
			if !equalStrings(scope.AllowedUserIDs, test.users) || !equalStrings(scope.AllowedOrgIDs, test.organizations) {
				t.Fatalf("scope=%#v", scope)
			}
		})
	}
}

func TestAuthorizeDataAccessAppliesRecordGuardrails(t *testing.T) {
	denied := authorizationPrincipal("integration.connections", "list", identitysdk.DataScopeAll)
	denied.AccessBundle.Guardrails = []identitysdk.Guardrail{{Key: "maintenance", Resource: "*", Action: "*", Effect: identitysdk.EffectDeny}}
	deniedScope, err := AuthorizeDataAccess(denied, "integration.connections.list")
	if err != nil || !deniedScope.DeniedAll {
		t.Fatalf("global record guardrail scope=%#v err=%v", deniedScope, err)
	}

	fieldOnly := authorizationPrincipal("integration.connections", "list", identitysdk.DataScopeAll)
	fieldOnly.AccessBundle.Guardrails = []identitysdk.Guardrail{{Key: "hide-secret", Resource: "*", Action: "*", Field: "secret", Effect: identitysdk.EffectDeny}}
	if _, err := AuthorizeDataAccess(fieldOnly, "integration.connections.list"); err != nil {
		t.Fatalf("field-only guardrail denied record access: %v", err)
	}
}

func authorizationPrincipal(resource, action string, dataScope identitysdk.DataScope) identitysdk.Principal {
	bundle := &identitysdk.AccessBundle{
		ContractVersion: identitysdk.CurrentPolicyBundleVersion, AuthorizationRevision: "revision-1", ExpiresAt: time.Now().UTC().Add(time.Hour),
		Subject: identitysdk.Subject{
			WorkspaceID: "workspace-a", SubjectID: "user-a", OrgID: "org-a",
			OrgScopeIDs:        []string{"org-child", "org-a"},
			SupportOrgScopeIDs: []string{"support-child", "support-a"},
		},
		FunctionGrants: []identitysdk.FunctionGrant{{Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow}},
		DataPolicies:   []identitysdk.DataPolicy{{Key: resource + "." + action, Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow, DataScopes: []identitysdk.DataScope{dataScope}, Predicate: scopePredicate(dataScope)}},
	}
	return identitysdk.Principal{Known: true, WorkspaceID: "workspace-a", UserID: "user-a", AccessBundle: bundle}
}

func scopePredicate(scope identitysdk.DataScope) identitysdk.Predicate {
	switch scope {
	case identitysdk.DataScopeAll:
		return identitysdk.Predicate{}
	case identitysdk.DataScopeOwner:
		return identitysdk.Predicate{Fact: "owner_user_id", Operator: identitysdk.OperatorEqual, Value: "$subject.id"}
	case identitysdk.DataScopeOrg:
		return identitysdk.Predicate{Fact: "owner_org_id", Operator: identitysdk.OperatorEqual, Value: "$subject.org_id"}
	case identitysdk.DataScopeOrgChild:
		return identitysdk.Predicate{Fact: "owner_org_id", Operator: identitysdk.OperatorIn, Value: "$subject.org_scope_ids"}
	case identitysdk.DataScopeTargetOrg:
		return identitysdk.Predicate{Fact: "owner_org_id", Operator: identitysdk.OperatorIn, Value: "$subject.support_org_scope_ids"}
	default:
		return identitysdk.Predicate{}
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
