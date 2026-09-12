package integrationapplication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type AccountWriteAuthority interface {
	AuthorizeConnectionAccountWrite(context.Context, model.ConnectionAccountSubject, string, model.ConnectionAccountWriteOperation) (model.ConnectionAccountWriteAccess, error)
}

// The adapter fixes supported contracts and strips responses to validated,
// bounded receipts. Raw provider response JSON must never become ledger data.
type AccountWriteCodec interface {
	ValidateOperation(model.ConnectionAccountWriteOperation) error
	CanonicalPayload(model.ConnectionAccountWriteOperation, json.RawMessage) (json.RawMessage, error)
	Receipt(model.AccountWriteClaim, json.RawMessage) (json.RawMessage, error)
}

type AccountWriteLedger interface {
	ClaimAccountWrite(context.Context, model.AccountWriteClaim) (model.ConnectionAccountWriteResult, bool, error)
	ReadAccountWrite(context.Context, model.AccountWriteClaim) (model.ConnectionAccountWriteResult, error)
	FinishAccountWrite(context.Context, model.AccountWriteClaim, model.AccountWriteExecution) (model.ConnectionAccountWriteResult, error)
}

type AccountWriteExecutor interface {
	ExecuteAccountWrite(context.Context, model.AccountWriteClaim) model.AccountWriteExecution
}

type AccountWriteService struct {
	authority AccountWriteAuthority
	codec     AccountWriteCodec
	ledger    AccountWriteLedger
	executor  AccountWriteExecutor
}

func NewAccountWriteService(authority AccountWriteAuthority, codec AccountWriteCodec, ledger AccountWriteLedger, executor AccountWriteExecutor) *AccountWriteService {
	return &AccountWriteService{authority: authority, codec: codec, ledger: ledger, executor: executor}
}

func (s *AccountWriteService) AuthorizeConnectionAccountWrite(ctx context.Context, subject model.ConnectionAccountSubject, key string, op model.ConnectionAccountWriteOperation) (model.ConnectionAccountWriteAccess, error) {
	if err := subject.Validate(); err != nil {
		return model.ConnectionAccountWriteAccess{}, err
	}
	if strings.TrimSpace(subject.UserID) != subject.UserID || strings.TrimSpace(subject.WorkspaceID) != subject.WorkspaceID {
		return model.ConnectionAccountWriteAccess{}, fmt.Errorf("Integration account write subject is invalid")
	}
	if err := op.Validate(); err != nil {
		return model.ConnectionAccountWriteAccess{}, err
	}
	if err := s.codec.ValidateOperation(op); err != nil {
		return model.ConnectionAccountWriteAccess{}, err
	}
	return s.authority.AuthorizeConnectionAccountWrite(ctx, subject, key, op)
}

func (s *AccountWriteService) claim(ctx context.Context, subject model.ConnectionAccountSubject, key string, request model.ConnectionAccountWriteRequest) (model.AccountWriteClaim, error) {
	if err := request.Validate(); err != nil {
		return model.AccountWriteClaim{}, err
	}
	payload, err := s.codec.CanonicalPayload(request.ExpectedSource.OperationContract(), request.Payload)
	if err != nil {
		return model.AccountWriteClaim{}, fmt.Errorf("Integration account write payload is invalid")
	}
	request.Payload = payload
	claim := model.AccountWriteClaim{Subject: subject, Request: request}
	if err := s.current(ctx, claim, key); err != nil {
		return model.AccountWriteClaim{}, err
	}
	identity, _ := json.Marshal([]string{subject.WorkspaceID, subject.UserID, request.RequestID})
	id := sha256.Sum256(identity)
	claim.InvocationID = "account-write:" + hex.EncodeToString(id[:])
	content, _ := json.Marshal(struct {
		Source  model.ConnectionAccountWriteSource
		Payload json.RawMessage
	}{request.ExpectedSource, request.Payload})
	fingerprint := sha256.Sum256(content)
	claim.Fingerprint = hex.EncodeToString(fingerprint[:])
	return claim, nil
}

func (s *AccountWriteService) current(ctx context.Context, claim model.AccountWriteClaim, key string) error {
	access, err := s.AuthorizeConnectionAccountWrite(ctx, claim.Subject, key, claim.Request.ExpectedSource.OperationContract())
	if err != nil {
		return err
	}
	if access.Source != claim.Request.ExpectedSource || access.Source.WorkspaceID != claim.Subject.WorkspaceID || access.Source.ConnectionKey != key {
		return fmt.Errorf("Integration account write authorization changed")
	}
	return nil
}

func (s *AccountWriteService) WriteConnectionAccount(ctx context.Context, subject model.ConnectionAccountSubject, key string, request model.ConnectionAccountWriteRequest) (model.ConnectionAccountWriteResult, error) {
	claim, err := s.claim(ctx, subject, key, request)
	if err != nil {
		return model.ConnectionAccountWriteResult{}, err
	}
	result, acquired, err := s.ledger.ClaimAccountWrite(ctx, claim)
	if err != nil {
		return model.ConnectionAccountWriteResult{}, err
	}
	if acquired {
		outcome := model.AccountWriteExecution{Status: model.AccountWriteFailed}
		if ctx.Err() == nil && s.current(ctx, claim, key) == nil {
			outcome = s.executor.ExecuteAccountWrite(ctx, claim)
		}
		if outcome.Status == model.AccountWriteSucceeded {
			receipt, receiptErr := s.codec.Receipt(claim, outcome.Payload)
			if receiptErr != nil {
				outcome.Status = model.AccountWriteUncertain
			}
			outcome.Payload = receipt
		} else {
			outcome.Payload = nil
		}
		if outcome.Status != model.AccountWriteSucceeded && outcome.Status != model.AccountWriteFailed {
			outcome.Status = model.AccountWriteUncertain
			outcome.Payload = nil
		}
		// Once I/O might have happened, persist its evidence despite a disconnected
		// caller. A process crash leaves the insert-only claim uncertain forever.
		write, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		result, err = s.ledger.FinishAccountWrite(write, claim, outcome)
		cancel()
		if err != nil {
			return model.ConnectionAccountWriteResult{}, fmt.Errorf("Integration account write receipt unavailable; query the original request")
		}
	}
	if err := s.current(ctx, claim, key); err != nil {
		return model.ConnectionAccountWriteResult{}, err
	}
	return result, nil
}

func (s *AccountWriteService) ReadConnectionAccountWriteReceipt(ctx context.Context, subject model.ConnectionAccountSubject, key string, request model.ConnectionAccountWriteRequest) (model.ConnectionAccountWriteResult, error) {
	claim, err := s.claim(ctx, subject, key, request)
	if err != nil {
		return model.ConnectionAccountWriteResult{}, err
	}
	result, err := s.ledger.ReadAccountWrite(ctx, claim)
	if err != nil {
		return model.ConnectionAccountWriteResult{}, err
	}
	if err := s.current(ctx, claim, key); err != nil {
		return model.ConnectionAccountWriteResult{}, err
	}
	return result, nil
}
