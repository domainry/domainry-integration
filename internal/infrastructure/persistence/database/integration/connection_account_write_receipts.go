package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

func (s *OperationsStore) FinishAccountWrite(ctx context.Context, claim model.AccountWriteClaim, outcome model.AccountWriteExecution) (model.ConnectionAccountWriteResult, error) {
	if outcome.Status != model.AccountWriteSucceeded && outcome.Status != model.AccountWriteFailed && outcome.Status != model.AccountWriteUncertain {
		return model.ConnectionAccountWriteResult{}, fmt.Errorf("Integration account write outcome is invalid")
	}
	metadata := accountWriteEvidence(claim)
	initial, _ := json.Marshal(metadata)
	if outcome.Status == model.AccountWriteSucceeded {
		if len(outcome.Payload) == 0 || len(outcome.Payload) > 32<<10 || !json.Valid(outcome.Payload) {
			return model.ConnectionAccountWriteResult{}, fmt.Errorf("Integration account write receipt is invalid")
		}
		metadata.Receipt = outcome.Payload
	}
	metadata.CredentialUpdateFailed = outcome.CredentialUpdateFailed
	raw, err := json.Marshal(metadata)
	if err != nil {
		return model.ConnectionAccountWriteResult{}, err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_invocations").Set("status", outcome.Status).Set("metadata_json", string(raw)).Set("updated_at", time.Now().UTC().Format(time.RFC3339Nano)).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, claim.Subject.WorkspaceID, "_integration_invocations", claim.InvocationID), query.And(
		query.Equal("workspace_id", claim.Subject.WorkspaceID), query.Equal("id", claim.InvocationID), query.Equal("status", "running"), query.Equal("metadata_json", string(initial)),
	))).Build()
	if err != nil {
		return model.ConnectionAccountWriteResult{}, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return model.ConnectionAccountWriteResult{}, err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return model.ConnectionAccountWriteResult{}, fmt.Errorf("Integration account write claim changed")
	}
	return s.ReadAccountWrite(ctx, claim)
}

type accountWriteMetadata struct {
	Kind                   string                             `json:"kind"`
	ActorID                string                             `json:"actor_id"`
	Source                 model.ConnectionAccountWriteSource `json:"source"`
	Fingerprint            string                             `json:"fingerprint"`
	Receipt                json.RawMessage                    `json:"receipt,omitempty"`
	CredentialUpdateFailed bool                               `json:"credential_update_failed,omitempty"`
}

func accountWriteEvidence(claim model.AccountWriteClaim) accountWriteMetadata {
	return accountWriteMetadata{Kind: "account-write-v1", ActorID: claim.Subject.UserID, Source: claim.Request.ExpectedSource, Fingerprint: claim.Fingerprint}
}

func (s *OperationsStore) ReadAccountWrite(ctx context.Context, claim model.AccountWriteClaim) (model.ConnectionAccountWriteResult, error) {
	out := model.ConnectionAccountWriteResult{Source: claim.Request.ExpectedSource, Status: model.AccountWriteNotFound}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_invocations").Columns("status", "metadata_json", "updated_at").Where(query.And(query.Equal("workspace_id", claim.Subject.WorkspaceID), query.Equal("id", claim.InvocationID))).Limit(1).Build()
	if err != nil {
		return out, err
	}
	var status, raw, recordedAt string
	err = s.database.QueryRowContext(ctx, statement, args...).Scan(&status, &raw, &recordedAt)
	if err == sql.ErrNoRows {
		return out, nil
	}
	if err != nil {
		return model.ConnectionAccountWriteResult{}, err
	}
	var metadata accountWriteMetadata
	if json.Unmarshal([]byte(raw), &metadata) != nil || metadata.Kind != "account-write-v1" || metadata.ActorID != claim.Subject.UserID || metadata.Source != claim.Request.ExpectedSource || metadata.Fingerprint != claim.Fingerprint {
		return model.ConnectionAccountWriteResult{}, fmt.Errorf("Integration account write request identity conflicts")
	}
	out.InvocationID, out.RecordedAt, out.Status = claim.InvocationID, recordedAt, status
	switch status {
	case model.AccountWriteSucceeded:
		if len(metadata.Receipt) == 0 || len(metadata.Receipt) > 32<<10 || !json.Valid(metadata.Receipt) {
			return model.ConnectionAccountWriteResult{}, fmt.Errorf("Integration account write receipt is invalid")
		}
		out.Receipt = append(json.RawMessage(nil), metadata.Receipt...)
	case model.AccountWriteFailed:
	default:
		out.Status = model.AccountWriteUncertain
	}
	return out, nil
}

func (s *OperationsStore) ClaimAccountWrite(ctx context.Context, claim model.AccountWriteClaim) (model.ConnectionAccountWriteResult, bool, error) {
	if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, claim.Subject.WorkspaceID, subjectFenceReference{"subject", "", claim.Subject.UserID}); err != nil {
		return model.ConnectionAccountWriteResult{}, false, err
	}
	current, err := s.ReadAccountWrite(ctx, claim)
	if err != nil || current.Status != model.AccountWriteNotFound {
		return current, false, err
	}
	source := claim.Request.ExpectedSource
	metadata, err := json.Marshal(accountWriteEvidence(claim))
	if err != nil {
		return model.ConnectionAccountWriteResult{}, false, err
	}
	err = s.delivery.insertInvocation(ctx, claim.InvocationID,
		model.DeliveryRequest{MessageID: claim.InvocationID, WorkspaceID: source.WorkspaceID, ConnectorKey: source.ConnectorKey, ConnectionKey: source.ConnectionKey, Operation: source.Operation},
		deliveryConnection{Key: source.ConnectionKey, ProviderKey: source.ProviderKey}, metadata)
	if err == nil {
		return model.ConnectionAccountWriteResult{}, true, nil
	}
	// A competing insert may have won. Never reset an existing row or interpret
	// a storage error as permission to call the external provider.
	current, readErr := s.ReadAccountWrite(ctx, claim)
	if readErr == nil && current.Status != model.AccountWriteNotFound {
		return current, false, nil
	}
	if readErr != nil {
		return model.ConnectionAccountWriteResult{}, false, readErr
	}
	return model.ConnectionAccountWriteResult{}, false, err
}
