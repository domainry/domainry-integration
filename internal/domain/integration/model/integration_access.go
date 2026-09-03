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
