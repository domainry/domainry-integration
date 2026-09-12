package integrationmodel

import (
	"context"
	"strings"
)

// AccessScope is the application-authorized, repository-consumable projection
// of one exact Action Permission. It deliberately contains no role-wide
// defaults and no persistence implementation details.
type AccessScope struct {
	WorkspaceID    string
	PermissionKey  string
	ActorID        string
	ActorOrgID     string
	Unrestricted   bool
	AllowedUserIDs []string
	AllowedOrgIDs  []string
	DeniedAll      bool
	DeniedUserIDs  []string
	DeniedOrgIDs   []string
}

func (scope AccessScope) Valid() bool {
	if strings.TrimSpace(scope.WorkspaceID) == "" || strings.TrimSpace(scope.PermissionKey) == "" || strings.TrimSpace(scope.ActorID) == "" {
		return false
	}
	return scope.Unrestricted || len(scope.AllowedUserIDs) != 0 || len(scope.AllowedOrgIDs) != 0
}

type accessScopeContextKey struct{}

func WithAccessScope(ctx context.Context, scope AccessScope) context.Context {
	return context.WithValue(ctx, accessScopeContextKey{}, scope)
}

func AccessScopeFromContext(ctx context.Context) (AccessScope, bool) {
	if ctx == nil {
		return AccessScope{}, false
	}
	scope, ok := ctx.Value(accessScopeContextKey{}).(AccessScope)
	return scope, ok
}

// ConnectionAccountAccess projects the exact action's canonical data scope.
// Account ownership has no organization inheritance. Unsupported organization
// denials fail closed; creator evidence never determines account access.
func (scope AccessScope) ConnectionAccountAccess() (personal, workspace bool) {
	if !scope.Valid() || scope.DeniedAll || len(scope.DeniedOrgIDs) != 0 {
		return false, false
	}
	personal, workspace = scope.Unrestricted, scope.Unrestricted
	for _, id := range scope.AllowedUserIDs {
		personal = personal || id == scope.ActorID
	}
	for _, id := range scope.DeniedUserIDs {
		if id == scope.ActorID {
			personal = false
		}
	}
	return
}
