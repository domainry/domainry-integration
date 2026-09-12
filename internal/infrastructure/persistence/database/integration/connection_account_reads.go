package integration

import (
	"context"
	"fmt"

	connector "github.com/domainry/domainry-connector-sdk"
	sdk "github.com/domainry/domainry-integration-sdk"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
	integrationservice "github.com/domainry/domainry-integration/internal/domain/integration/service"
)

// Authority remains beside the account and grant owner. No caller-supplied
// scope requirement or provider configuration participates in this decision.
func (s *ManagementStore) AuthorizeConnectionAccountRead(ctx context.Context, subject model.ConnectionAccountSubject, key string, request model.ConnectionAccountReadOperation) (model.ConnectionAccountReadAccess, error) {
	if err := request.Validate(); err != nil {
		return model.ConnectionAccountReadAccess{}, err
	}
	a, err := s.GetConnectionAccount(ctx, sdk.ConnectionAccountSubject{WorkspaceID: subject.WorkspaceID, UserID: subject.UserID, Access: sdk.ConnectionAccountAccess{Personal: subject.Access.Personal, Workspace: subject.Access.Workspace}}, key)
	if err != nil {
		return model.ConnectionAccountReadAccess{}, err
	}
	if a.Status != "active" || a.Readiness == nil || !a.Readiness.Available || a.UpdatedAt == "" {
		return model.ConnectionAccountReadAccess{}, fmt.Errorf("Integration connection account is unavailable")
	}
	p, ok := s.delivery.providers.Provider(a.ConnectorKey, a.ProviderKey)
	if !ok {
		return model.ConnectionAccountReadAccess{}, fmt.Errorf("Integration account read provider is unavailable")
	}
	op, ok := providerOperation(p.Descriptor(), request.Operation)
	if !ok || op.Mode != connector.ModeCall || op.Reliability.Effect != connector.EffectRead || op.ContractSHA256 != request.ContractSHA256 {
		return model.ConnectionAccountReadAccess{}, fmt.Errorf("Integration account read operation is unavailable")
	}
	alternatives, declared := connector.ResolveOAuthOperationScopes(p, op.Key)
	if !declared || integrationservice.OAuthScopeState(a.Readiness.GrantedScopes, len(a.Readiness.GrantedScopes) > 0, alternatives) != "ready" {
		return model.ConnectionAccountReadAccess{}, fmt.Errorf("Integration account read scope is not granted")
	}
	return model.ConnectionAccountReadAccess{Source: model.ConnectionAccountReadSource{WorkspaceID: a.WorkspaceID, ConnectionKey: a.Key, ConnectorKey: a.ConnectorKey, ProviderKey: a.ProviderKey, AccountUpdatedAt: a.UpdatedAt, Operation: op.Key, ContractSHA256: op.ContractSHA256}, ScopeAlternatives: alternatives}, nil
}
