package integrationapplication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type AccountReadAuthority interface {
	AuthorizeConnectionAccountRead(context.Context, model.ConnectionAccountSubject, string, model.ConnectionAccountReadOperation) (model.ConnectionAccountReadAccess, error)
}

type AccountReadCalls interface {
	Call(context.Context, model.ProviderCallRequest) (model.ProviderCallResult, error)
}

// Orchestration depends on owner ports, never database or Provider adapters.
type AccountReadService struct {
	authority AccountReadAuthority
	calls     AccountReadCalls
}

func NewAccountReadService(authority AccountReadAuthority, calls AccountReadCalls) *AccountReadService {
	return &AccountReadService{authority: authority, calls: calls}
}

func (s *AccountReadService) AuthorizeConnectionAccountRead(ctx context.Context, subject model.ConnectionAccountSubject, key string, op model.ConnectionAccountReadOperation) (model.ConnectionAccountReadAccess, error) {
	if err := subject.Validate(); err != nil {
		return model.ConnectionAccountReadAccess{}, err
	}
	if err := op.Validate(); err != nil {
		return model.ConnectionAccountReadAccess{}, err
	}
	return s.authority.AuthorizeConnectionAccountRead(ctx, subject, key, op)
}

func (s *AccountReadService) ReadConnectionAccount(ctx context.Context, subject model.ConnectionAccountSubject, key string, request model.ConnectionAccountReadRequest) (model.ConnectionAccountReadResult, error) {
	if err := request.Validate(); err != nil {
		return model.ConnectionAccountReadResult{}, err
	}
	access, err := s.AuthorizeConnectionAccountRead(ctx, subject, key, request.OperationContract())
	if err != nil {
		return model.ConnectionAccountReadResult{}, err
	}
	// Administrative calls have a different identity namespace. Bind sensitive
	// replay to actor, account revision, operation contract and exact input.
	identity, _ := json.Marshal(struct {
		Subject string
		Source  model.ConnectionAccountReadSource
		Request model.ConnectionAccountReadRequest
	}{subject.UserID, access.Source, request})
	digest := sha256.Sum256(identity)
	trusted := model.WithAccessScope(ctx, model.AccessScope{WorkspaceID: subject.WorkspaceID, ActorID: subject.UserID, PermissionKey: "integration.connection_accounts.read", Unrestricted: true})
	result, err := s.calls.Call(trusted, model.ProviderCallRequest{
		RequestID: "account-read:" + hex.EncodeToString(digest[:]), WorkspaceID: subject.WorkspaceID,
		ConnectorKey: access.Source.ConnectorKey, ConnectionKey: access.Source.ConnectionKey, Operation: request.Operation, Payload: append(json.RawMessage(nil), request.Payload...),
		PersistenceMode: model.ProviderCallPersistenceSensitive, MaskedDestination: "authorized-account", ActorID: subject.UserID,
		ReadExpectation: &model.ProviderReadExpectation{ConnectionUpdatedAt: access.Source.AccountUpdatedAt, ProviderKey: access.Source.ProviderKey, ContractSHA256: access.Source.ContractSHA256},
	})
	if err != nil {
		return model.ConnectionAccountReadResult{}, fmt.Errorf("Integration account read failed")
	}
	current, err := s.AuthorizeConnectionAccountRead(ctx, subject, key, request.OperationContract())
	if err != nil || current.Source != access.Source {
		return model.ConnectionAccountReadResult{}, fmt.Errorf("Integration account read authorization changed")
	}
	if result.Invocation.Status != "succeeded" || len(result.Response) > 4<<20 || len(result.Response) > 0 && !json.Valid(result.Response) {
		return model.ConnectionAccountReadResult{}, fmt.Errorf("Integration account read result is invalid")
	}
	return model.ConnectionAccountReadResult{Source: current.Source, InvocationID: result.Invocation.ID, ReadAt: result.Invocation.CreatedAt, PayloadAvailable: len(result.Response) > 0, Payload: append(json.RawMessage(nil), result.Response...)}, nil
}
