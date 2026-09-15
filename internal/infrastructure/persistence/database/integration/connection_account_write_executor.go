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

// The account owner makes ownership immutable; configuration, reauthorization
// and revocation change the connection revision. After resolving secrets,
// recheck that revision and the current recorded grant immediately before I/O.
func (s *OperationsStore) ExecuteAccountWrite(ctx context.Context, claim model.AccountWriteClaim) model.AccountWriteExecution {
	failed := model.AccountWriteExecution{Status: model.AccountWriteFailed}
	source := claim.Request.ExpectedSource
	request := model.DeliveryRequest{WorkspaceID: source.WorkspaceID, ConnectorKey: source.ConnectorKey, ConnectionKey: source.ConnectionKey}
	connection, err := s.delivery.connection(ctx, request)
	if err != nil || connection.UpdatedAt != source.AccountUpdatedAt || connection.ProviderKey != source.ProviderKey {
		return failed
	}
	p, found := s.delivery.providers.Provider(source.ConnectorKey, source.ProviderKey)
	if !found {
		return failed
	}
	op, found := providerOperation(p.Descriptor(), source.Operation)
	if !found || op.Mode != connector.ModeCall || op.Reliability.Effect != connector.EffectWrite || op.ContractSHA256 != source.ContractSHA256 {
		return failed
	}
	resolved, err := s.delivery.resolveProviderSecrets(ctx, source.WorkspaceID, connection.SecretRefs)
	if err != nil {
		return failed
	}
	current, err := s.delivery.connection(ctx, request)
	if err != nil || current.UpdatedAt != source.AccountUpdatedAt || current.ProviderKey != source.ProviderKey {
		return failed
	}
	alternatives, declared := connector.ResolveOAuthOperationScopes(p, source.Operation)
	if !declared || !s.accountWriteScopesGranted(ctx, source, alternatives) || ctx.Err() != nil {
		return failed
	}
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

func (s *OperationsStore) accountWriteScopesGranted(ctx context.Context, source model.ConnectionAccountWriteSource, alternatives [][]string) bool {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connection_grants").Columns("scopes_json").Where(query.And(query.Equal("workspace_id", source.WorkspaceID), query.Equal("connection_key", source.ConnectionKey))).Limit(1).Build()
	if err != nil {
		return false
	}
	var raw string
	err = s.database.QueryRowContext(ctx, statement, args...).Scan(&raw)
	if err != nil && err != sql.ErrNoRows {
		return false
	}
	var grants []string
	known := err == nil
	if known && json.Unmarshal([]byte(raw), &grants) != nil {
		return false
	}
	return service.OAuthScopeState(grants, known, alternatives) == "ready"
}
