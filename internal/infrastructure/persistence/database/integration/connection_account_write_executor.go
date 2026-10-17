package integration

import (
	"context"
	"database/sql"
	"encoding/json"

	connector "github.com/domainry/domainry-connector-sdk"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
	service "github.com/domainry/domainry-integration/internal/domain/integration/service"
	"github.com/domainry/domainry-orm/query"
)

type accountWriteExecutionContext struct {
	provider   connector.Adapter
	operation  connector.OperationDescriptor
	connection deliveryConnection
	resolved   resolvedProviderSecrets
}

// The account owner makes ownership immutable; configuration, reauthorization
// and revocation change the connection revision. After resolving secrets,
// recheck that revision and the current recorded grant immediately before I/O.
func (s *OperationsStore) ExecuteAccountWrite(ctx context.Context, claim model.AccountWriteClaim) model.AccountWriteExecution {
	failed := model.AccountWriteExecution{Status: model.AccountWriteFailed}
	prepared, ok := s.prepareAccountWrite(ctx, claim)
	if !ok {
		return failed
	}
	source, connection, p, resolved := claim.Request.ExpectedSource, prepared.connection, prepared.provider, prepared.resolved
	result, callErr := p.Call(ctx, connector.CallRequest{
		ConnectorKey: source.ConnectorKey, ProviderKey: source.ProviderKey, OperationKey: source.Operation,
		ContractSHA256: source.ContractSHA256, Mode: connector.ModeCall,
		Connection: connector.Connection{Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs},
		Payload:    append(json.RawMessage(nil), claim.Request.Payload...), RequestRef: claim.InvocationID, Secrets: resolved.Values,
		Principal: connector.Principal{UserID: claim.Subject.UserID, WorkspaceID: source.WorkspaceID, RequestID: claim.InvocationID, IsAuthenticated: true},
	})
	// Credential persistence is separate from the effect already reported by the
	// provider. A failed token rotation write cannot erase a known sent message.
	secretErr := s.delivery.persistProviderSecretUpdates(ctx, source.WorkspaceID, connection.SecretRefs, resolved.Versions, result.SecretUpdates, nil)
	out := model.AccountWriteExecution{Status: model.AccountWriteSucceeded, Payload: append(json.RawMessage(nil), result.Payload...), CredentialUpdateFailed: secretErr != nil}
	if callErr != nil {
		out.Status, out.Payload = model.AccountWriteUncertain, nil
		if classification, known := connector.ErrorClassificationOf(callErr); known && (classification == connector.ErrorPermanent || classification == connector.ErrorRetryable) {
			out.Status = model.AccountWriteFailed
		}
	}
	return out
}

func (s *OperationsStore) ReconcileAccountWrite(ctx context.Context, claim model.AccountWriteClaim) (model.AccountWriteExecution, bool) {
	prepared, ok := s.prepareAccountWrite(ctx, claim)
	if !ok || prepared.operation.Reliability.Reconciliation != connector.ReconciliationProviderLookup {
		return model.AccountWriteExecution{Status: model.AccountWriteUncertain}, false
	}
	reconciler, ok := prepared.provider.(connector.Reconciler)
	if !ok {
		return model.AccountWriteExecution{Status: model.AccountWriteUncertain}, false
	}
	source := claim.Request.ExpectedSource
	result, reconcileErr := reconciler.Reconcile(ctx, connector.ReconcileRequest{
		ConnectorKey: source.ConnectorKey, ProviderKey: source.ProviderKey, OperationKey: source.Operation,
		ContractSHA256: source.ContractSHA256,
		Connection:     connector.Connection{Key: prepared.connection.Key, WorkspaceID: prepared.connection.WorkspaceID, ConnectorKey: prepared.connection.ConnectorKey, ProviderKey: prepared.connection.ProviderKey, Status: prepared.connection.Status, Config: prepared.connection.Config, SecretRefs: prepared.connection.SecretRefs},
		RequestRef:     claim.InvocationID, Payload: append(json.RawMessage(nil), claim.Request.Payload...), Secrets: prepared.resolved.Values,
		Principal: connector.Principal{UserID: claim.Subject.UserID, WorkspaceID: source.WorkspaceID, RequestID: claim.InvocationID, IsAuthenticated: true},
	})
	if reconcileErr == nil {
		reconcileErr = result.Validate()
	}
	updates := map[string]string(nil)
	if result.Result != nil {
		updates = result.Result.SecretUpdates
	}
	secretErr := s.delivery.persistProviderSecretUpdates(ctx, source.WorkspaceID, prepared.connection.SecretRefs, prepared.resolved.Versions, updates, reconcileErr)
	if reconcileErr != nil {
		return model.AccountWriteExecution{Status: model.AccountWriteUncertain, CredentialUpdateFailed: secretErr != nil}, false
	}
	switch result.Outcome {
	case connector.ReconciliationSucceeded:
		if result.Result == nil || len(result.Result.Payload) == 0 {
			return model.AccountWriteExecution{Status: model.AccountWriteUncertain, CredentialUpdateFailed: secretErr != nil}, false
		}
		return model.AccountWriteExecution{Status: model.AccountWriteSucceeded, Payload: append(json.RawMessage(nil), result.Result.Payload...), CredentialUpdateFailed: secretErr != nil}, true
	case connector.ReconciliationFailed:
		return model.AccountWriteExecution{Status: model.AccountWriteFailed, CredentialUpdateFailed: secretErr != nil}, true
	default:
		// not_found is not permission to replay: provider indexing may lag.
		return model.AccountWriteExecution{Status: model.AccountWriteUncertain, CredentialUpdateFailed: secretErr != nil}, false
	}
}

func (s *OperationsStore) prepareAccountWrite(ctx context.Context, claim model.AccountWriteClaim) (accountWriteExecutionContext, bool) {
	source := claim.Request.ExpectedSource
	request := model.DeliveryRequest{WorkspaceID: source.WorkspaceID, ConnectorKey: source.ConnectorKey, ConnectionKey: source.ConnectionKey}
	connection, err := s.delivery.connection(ctx, request)
	if err != nil || connection.UpdatedAt != source.AccountUpdatedAt || connection.ProviderKey != source.ProviderKey {
		return accountWriteExecutionContext{}, false
	}
	p, found := s.delivery.providers.Provider(source.ConnectorKey, source.ProviderKey)
	if !found {
		return accountWriteExecutionContext{}, false
	}
	op, found := providerOperation(p.Descriptor(), source.Operation)
	if !found || op.Mode != connector.ModeCall || op.Reliability.Effect != connector.EffectWrite || op.ContractSHA256 != source.ContractSHA256 {
		return accountWriteExecutionContext{}, false
	}
	resolved, err := s.delivery.resolveProviderSecrets(ctx, source.WorkspaceID, connection.SecretRefs)
	if err != nil {
		return accountWriteExecutionContext{}, false
	}
	current, err := s.delivery.connection(ctx, request)
	if err != nil || current.UpdatedAt != source.AccountUpdatedAt || current.ProviderKey != source.ProviderKey {
		return accountWriteExecutionContext{}, false
	}
	alternatives, declared := connector.ResolveOAuthOperationScopes(p, source.Operation)
	if !declared || !s.accountWriteScopesGranted(ctx, source, alternatives) || ctx.Err() != nil {
		return accountWriteExecutionContext{}, false
	}
	return accountWriteExecutionContext{provider: p, operation: op, connection: connection, resolved: resolved}, true
}

func (s *OperationsStore) accountWriteScopesGranted(ctx context.Context, source model.ConnectionAccountWriteSource, alternatives [][]string) bool {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connections").Columns("granted_scopes_json").Where(query.And(query.Equal("workspace_id", source.WorkspaceID), query.Equal("connection_key", source.ConnectionKey))).Limit(1).Build()
	if err != nil {
		return false
	}
	var raw sql.NullString
	if err = s.database.QueryRowContext(ctx, statement, args...).Scan(&raw); err != nil {
		return false
	}
	var grants []string
	known := raw.Valid
	if known && json.Unmarshal([]byte(raw.String), &grants) != nil {
		return false
	}
	return service.OAuthScopeState(grants, known, alternatives) == "ready"
}
