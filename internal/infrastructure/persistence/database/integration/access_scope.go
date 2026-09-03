package integration

import (
	"context"
	"fmt"
	"strings"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

func scopedWhere(ctx context.Context, workspaceID, ownerUserColumn, ownerOrgColumn string, additional ...query.Predicate) (query.Predicate, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	predicates := []query.Predicate{query.Equal("workspace_id", workspaceID)}
	scope, scoped := integrationmodel.AccessScopeFromContext(ctx)
	if scoped {
		if !scope.Valid() {
			return nil, fmt.Errorf("Integration access scope is invalid")
		}
		if scope.WorkspaceID != workspaceID {
			return nil, fmt.Errorf("Integration access workspace does not match the query workspace")
		}
		if !scope.Unrestricted || scope.DeniedAll || len(scope.DeniedUserIDs) != 0 || len(scope.DeniedOrgIDs) != 0 {
			predicates = append(predicates, scopeDataPredicate(scope, ownerUserColumn, ownerOrgColumn))
		}
	}
	predicates = append(predicates, additional...)
	return query.And(predicates...), nil
}

func scopeDataPredicate(scope integrationmodel.AccessScope, ownerUserColumn, ownerOrgColumn string) query.Predicate {
	if scope.DeniedAll {
		return query.AlwaysFalse()
	}
	if (ownerUserColumn == "" && len(scope.DeniedUserIDs) != 0) || (ownerOrgColumn == "" && len(scope.DeniedOrgIDs) != 0) {
		return query.AlwaysFalse()
	}
	allow := make([]query.Predicate, 0, 2)
	if !scope.Unrestricted {
		if ownerUserColumn != "" && len(scope.AllowedUserIDs) != 0 {
			allow = append(allow, query.In(ownerUserColumn, stringsToAny(scope.AllowedUserIDs)...))
		}
		if ownerOrgColumn != "" && len(scope.AllowedOrgIDs) != 0 {
			allow = append(allow, query.In(ownerOrgColumn, stringsToAny(scope.AllowedOrgIDs)...))
		}
		if len(allow) == 0 {
			return query.AlwaysFalse()
		}
	}
	deny := make([]query.Predicate, 0, 2)
	if ownerUserColumn != "" && len(scope.DeniedUserIDs) != 0 {
		deny = append(deny, query.In(ownerUserColumn, stringsToAny(scope.DeniedUserIDs)...))
	}
	if ownerOrgColumn != "" && len(scope.DeniedOrgIDs) != 0 {
		deny = append(deny, query.In(ownerOrgColumn, stringsToAny(scope.DeniedOrgIDs)...))
	}
	result := query.Predicate(nil)
	if !scope.Unrestricted {
		result = query.Or(allow...)
	}
	if len(deny) != 0 {
		denied := query.Not(query.Or(deny...))
		if result == nil {
			result = denied
		} else {
			result = query.And(result, denied)
		}
	}
	if result == nil {
		// `all` is intentionally represented by the absence of a data-range
		// predicate. The caller still supplies the mandatory workspace clause.
		return query.AlwaysTrue()
	}
	return result
}

func scopeOwner(ctx context.Context, fallbackActorID string) (actorID, ownerOrgID string) {
	if scope, ok := integrationmodel.AccessScopeFromContext(ctx); ok {
		return scope.ActorID, scope.ActorOrgID
	}
	return strings.TrimSpace(fallbackActorID), ""
}

func requireAllDataScope(ctx context.Context, workspaceID string) error {
	scope, scoped := integrationmodel.AccessScopeFromContext(ctx)
	if !scoped {
		return nil
	}
	if !scope.Valid() {
		return fmt.Errorf("Integration access scope is invalid")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if scope.WorkspaceID != workspaceID {
		return fmt.Errorf("Integration access workspace does not match the mutation workspace")
	}
	if !scope.Unrestricted || scope.DeniedAll || len(scope.DeniedUserIDs) != 0 || len(scope.DeniedOrgIDs) != 0 {
		return fmt.Errorf("Integration management mutations require all data scope")
	}
	return nil
}

func stringsToAny(values []string) []any {
	result := make([]any, len(values))
	for index := range values {
		result[index] = values[index]
	}
	return result
}
