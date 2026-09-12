package integration

import (
	"context"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	sdk "github.com/domainry/domainry-integration-sdk"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
	service "github.com/domainry/domainry-integration/internal/domain/integration/service"
)

func (s *ManagementStore) AuthorizeConnectionAccountWrite(ctx context.Context, subject model.ConnectionAccountSubject, key string, op model.ConnectionAccountWriteOperation) (model.ConnectionAccountWriteAccess, error) {
	if err := op.Validate(); err != nil {
		return model.ConnectionAccountWriteAccess{}, err
	}
	a, err := s.GetConnectionAccount(ctx, sdk.ConnectionAccountSubject{WorkspaceID: subject.WorkspaceID, UserID: subject.UserID, Access: sdk.ConnectionAccountAccess{Personal: subject.Access.Personal, Workspace: subject.Access.Workspace}}, key)
	if err != nil {
		return model.ConnectionAccountWriteAccess{}, err
	}
	if a.Status != "active" || a.Readiness == nil || !a.Readiness.Available || a.UpdatedAt == "" {
		return model.ConnectionAccountWriteAccess{}, fmt.Errorf("Integration connection account is unavailable")
	}
	p, found := s.delivery.providers.Provider(a.ConnectorKey, a.ProviderKey)
	if !found {
		return model.ConnectionAccountWriteAccess{}, fmt.Errorf("Integration account write provider is unavailable")
	}
	operation, found := providerOperation(p.Descriptor(), op.Operation)
	if !found || operation.Mode != connector.ModeCall || operation.Reliability.Effect != connector.EffectWrite || operation.ContractSHA256 != op.ContractSHA256 {
		return model.ConnectionAccountWriteAccess{}, fmt.Errorf("Integration account write operation is unavailable")
	}
	alternatives, declared := connector.ResolveOAuthOperationScopes(p, op.Operation)
	if !declared || service.OAuthScopeState(a.Readiness.GrantedScopes, len(a.Readiness.GrantedScopes) > 0, alternatives) != "ready" {
		return model.ConnectionAccountWriteAccess{}, fmt.Errorf("Integration account write scope is not granted")
	}
	return model.ConnectionAccountWriteAccess{Source: model.ConnectionAccountWriteSource{WorkspaceID: a.WorkspaceID, ConnectionKey: a.Key, ConnectorKey: a.ConnectorKey, ProviderKey: a.ProviderKey, AccountUpdatedAt: a.UpdatedAt, Operation: op.Operation, ContractSHA256: op.ContractSHA256}, ScopeAlternatives: alternatives}, nil
}
