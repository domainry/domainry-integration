package integrationapplication

import (
	"fmt"
	"sort"
	"strings"
	"time"

	identitysdk "github.com/domainry/domainry-identity-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

// AuthorizeDataAccess binds one exact function grant to the data scopes
// compiled for the same exact permission key. A function grant without a data
// policy, or a data policy without the function grant, fails closed.
func AuthorizeDataAccess(principal identitysdk.Principal, permissionKey string) (integrationmodel.AccessScope, error) {
	permissionKey = strings.TrimSpace(permissionKey)
	separator := strings.LastIndex(permissionKey, ".")
	if separator <= 0 || separator == len(permissionKey)-1 {
		return integrationmodel.AccessScope{}, fmt.Errorf("Integration permission key is invalid")
	}
	resource, action := permissionKey[:separator], permissionKey[separator+1:]
	if !principal.Known || strings.TrimSpace(principal.WorkspaceID) == "" || strings.TrimSpace(principal.UserID) == "" || principal.AccessBundle == nil {
		return integrationmodel.AccessScope{}, fmt.Errorf("Integration access principal is unavailable")
	}
	bundle := principal.AccessBundle
	if err := bundle.Validate(time.Now().UTC()); err != nil {
		return integrationmodel.AccessScope{}, fmt.Errorf("Integration access bundle is invalid: %w", err)
	}
	if strings.TrimSpace(string(bundle.Subject.WorkspaceID)) != strings.TrimSpace(principal.WorkspaceID) || strings.TrimSpace(string(bundle.Subject.SubjectID)) != strings.TrimSpace(principal.UserID) {
		return integrationmodel.AccessScope{}, fmt.Errorf("Integration access bundle subject does not match the principal")
	}
	functionAllowed := false
	for _, grant := range bundle.FunctionGrants {
		if strings.TrimSpace(string(grant.Resource)) != resource || strings.TrimSpace(string(grant.Action)) != action {
			continue
		}
		if grant.Effect == identitysdk.EffectDeny {
			return integrationmodel.AccessScope{}, fmt.Errorf("Integration permission %q is denied", permissionKey)
		}
		functionAllowed = functionAllowed || grant.Effect == identitysdk.EffectAllow
	}
	if !functionAllowed {
		return integrationmodel.AccessScope{}, fmt.Errorf("Integration permission %q is not granted", permissionKey)
	}
	scope := integrationmodel.AccessScope{
		WorkspaceID: strings.TrimSpace(principal.WorkspaceID), PermissionKey: permissionKey,
		ActorID: strings.TrimSpace(principal.UserID), ActorOrgID: strings.TrimSpace(bundle.Subject.OrgID),
	}
	matchedDataPolicy := false
	for _, policy := range bundle.DataPolicies {
		if strings.TrimSpace(string(policy.Resource)) != resource || strings.TrimSpace(string(policy.Action)) != action {
			continue
		}
		matchedDataPolicy = true
		if len(policy.DataScopes) == 0 {
			return integrationmodel.AccessScope{}, fmt.Errorf("Integration permission %q has no canonical data scope", permissionKey)
		}
		if !predicateMatchesDataScopes(policy.Predicate, policy.DataScopes) {
			return integrationmodel.AccessScope{}, fmt.Errorf("Integration permission %q has a predicate outside the closed data-scope contract", permissionKey)
		}
		for _, dataScope := range policy.DataScopes {
			if !dataScope.Valid() {
				return integrationmodel.AccessScope{}, fmt.Errorf("Integration permission %q has unsupported data scope %q", permissionKey, dataScope)
			}
			applyDataScope(&scope, policy.Effect, dataScope, bundle.Subject)
		}
	}
	if !matchedDataPolicy || !scope.Valid() {
		return integrationmodel.AccessScope{}, fmt.Errorf("Integration permission %q has no usable data policy", permissionKey)
	}
	for _, guardrail := range bundle.Guardrails {
		guardrailResource, guardrailAction := strings.TrimSpace(string(guardrail.Resource)), strings.TrimSpace(string(guardrail.Action))
		resourceMatches := guardrailResource == "" || guardrailResource == "*" || guardrailResource == resource
		actionMatches := guardrailAction == "" || guardrailAction == "*" || guardrailAction == action
		if !resourceMatches || !actionMatches || guardrail.Effect != identitysdk.EffectDeny || strings.TrimSpace(guardrail.Field) != "" {
			continue
		}
		if guardrail.Predicate != nil {
			return integrationmodel.AccessScope{}, fmt.Errorf("Integration permission %q has a data guardrail that cannot be expressed by the closed data-scope contract", permissionKey)
		}
		scope.DeniedAll = true
	}
	scope.AllowedUserIDs = uniqueSorted(scope.AllowedUserIDs)
	scope.AllowedOrgIDs = uniqueSorted(scope.AllowedOrgIDs)
	scope.DeniedUserIDs = uniqueSorted(scope.DeniedUserIDs)
	scope.DeniedOrgIDs = uniqueSorted(scope.DeniedOrgIDs)
	return scope, nil
}

func predicateMatchesDataScopes(predicate identitysdk.Predicate, scopes []identitysdk.DataScope) bool {
	if len(scopes) == 1 {
		if scopes[0] == identitysdk.DataScopeAll {
			return predicate.IsZero()
		}
		return predicateMatchesDataScope(predicate, scopes[0])
	}
	if !predicateContainerOnly(predicate) || len(predicate.Any) != len(scopes) {
		return false
	}
	matched := make([]bool, len(predicate.Any))
	for _, scope := range scopes {
		found := false
		for index := range predicate.Any {
			if !matched[index] && predicateMatchesDataScope(predicate.Any[index], scope) {
				matched[index], found = true, true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func predicateMatchesDataScope(predicate identitysdk.Predicate, scope identitysdk.DataScope) bool {
	if !predicateLeafOnly(predicate) {
		return false
	}
	value, ok := predicate.Value.(string)
	if !ok {
		return false
	}
	switch scope {
	case identitysdk.DataScopeOwner:
		return predicate.Fact == "owner_user_id" && predicate.Operator == identitysdk.OperatorEqual && value == "$subject.id"
	case identitysdk.DataScopeOrg:
		return predicate.Fact == "owner_org_id" && predicate.Operator == identitysdk.OperatorEqual && value == "$subject.org_id"
	case identitysdk.DataScopeOrgChild:
		return predicate.Fact == "owner_org_id" && predicate.Operator == identitysdk.OperatorIn && value == "$subject.org_scope_ids"
	case identitysdk.DataScopeTargetOrg:
		return predicate.Fact == "owner_org_id" && predicate.Operator == identitysdk.OperatorIn && value == "$subject.support_org_scope_ids"
	default:
		return false
	}
}

func predicateLeafOnly(predicate identitysdk.Predicate) bool {
	return strings.TrimSpace(predicate.Fact) != "" && len(predicate.Path) == 0 && len(predicate.All) == 0 && len(predicate.Any) == 0 && predicate.Not == nil
}

func predicateContainerOnly(predicate identitysdk.Predicate) bool {
	return strings.TrimSpace(predicate.Fact) == "" && len(predicate.Path) == 0 && predicate.Operator == "" && predicate.Value == nil && len(predicate.All) == 0 && predicate.Not == nil
}

func applyDataScope(result *integrationmodel.AccessScope, effect identitysdk.Effect, scope identitysdk.DataScope, subject identitysdk.Subject) {
	denied := effect == identitysdk.EffectDeny
	switch scope {
	case identitysdk.DataScopeAll:
		if denied {
			result.DeniedAll = true
		} else {
			result.Unrestricted = true
		}
	case identitysdk.DataScopeOwner:
		appendScopeValue(result, denied, string(subject.SubjectID), false)
	case identitysdk.DataScopeOrg:
		appendScopeValue(result, denied, subject.OrgID, true)
	case identitysdk.DataScopeOrgChild:
		for _, id := range subject.OrgScopeIDs {
			appendScopeValue(result, denied, id, true)
		}
	case identitysdk.DataScopeTargetOrg:
		for _, id := range subject.SupportOrgScopeIDs {
			appendScopeValue(result, denied, id, true)
		}
	}
}

func appendScopeValue(result *integrationmodel.AccessScope, denied bool, value string, organization bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	if organization {
		if denied {
			result.DeniedOrgIDs = append(result.DeniedOrgIDs, value)
		} else {
			result.AllowedOrgIDs = append(result.AllowedOrgIDs, value)
		}
		return
	}
	if denied {
		result.DeniedUserIDs = append(result.DeniedUserIDs, value)
	} else {
		result.AllowedUserIDs = append(result.AllowedUserIDs, value)
	}
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			if _, duplicate := seen[value]; !duplicate {
				seen[value] = struct{}{}
				result = append(result, value)
			}
		}
	}
	sort.Strings(result)
	return result
}
