package integrationmodel

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

type ConnectionAccountWriteOperation struct {
	Operation      string `json:"operation"`
	ContractSHA256 string `json:"contract_sha256"`
}

func (o ConnectionAccountWriteOperation) Validate() error {
	hash, err := hex.DecodeString(o.ContractSHA256)
	if !boundedAccountWriteValue(o.Operation, 128) || err != nil || len(hash) != 32 || strings.ToLower(o.ContractSHA256) != o.ContractSHA256 {
		return fmt.Errorf("Integration account write operation is invalid")
	}
	return nil
}

// Source is frozen by the host when authorizing the exact action. It is not a
// grant: the owner must check current ownership, state and OAuth scopes again.
type ConnectionAccountWriteSource struct {
	WorkspaceID      string `json:"workspace_id"`
	ConnectionKey    string `json:"connection_key"`
	ConnectorKey     string `json:"connector_key"`
	ProviderKey      string `json:"provider_key"`
	AccountUpdatedAt string `json:"account_updated_at"`
	Operation        string `json:"operation"`
	ContractSHA256   string `json:"contract_sha256"`
}

func (s ConnectionAccountWriteSource) OperationContract() ConnectionAccountWriteOperation {
	return ConnectionAccountWriteOperation{Operation: s.Operation, ContractSHA256: s.ContractSHA256}
}

type ConnectionAccountWriteAccess struct {
	Source            ConnectionAccountWriteSource `json:"source"`
	ScopeAlternatives [][]string                   `json:"scope_alternatives"`
}

// RequestID is a stable, host-generated execution identity, reused for receipt
// lookup after a lost response. Changing source/content under it must conflict.
// Integration retains a fingerprint and bounded receipt, never outgoing text.
type ConnectionAccountWriteRequest struct {
	RequestID      string                       `json:"request_id"`
	ExpectedSource ConnectionAccountWriteSource `json:"expected_source"`
	Payload        json.RawMessage              `json:"payload"`
}

func (r ConnectionAccountWriteRequest) Validate() error {
	if err := r.ExpectedSource.OperationContract().Validate(); err != nil {
		return err
	}
	for _, value := range []string{r.ExpectedSource.WorkspaceID, r.ExpectedSource.ConnectionKey, r.ExpectedSource.ConnectorKey, r.ExpectedSource.ProviderKey, r.ExpectedSource.AccountUpdatedAt} {
		if !boundedAccountWriteValue(value, 2048) {
			return fmt.Errorf("Integration account write source is invalid")
		}
	}
	if !boundedAccountWriteValue(r.RequestID, 256) || len(r.Payload) > 1<<20 || !json.Valid(r.Payload) {
		return fmt.Errorf("Integration account write request is invalid")
	}
	return nil
}

func boundedAccountWriteValue(value string, limit int) bool {
	return value != "" && len(value) <= limit && strings.TrimSpace(value) == value && strings.IndexFunc(value, unicode.IsControl) < 0
}

const (
	AccountWriteNotFound  = "not_found"
	AccountWriteSucceeded = "succeeded"
	AccountWriteFailed    = "failed"
	AccountWriteUncertain = "uncertain"
)

// Uncertain includes an in-flight or crash-interrupted claim. Neither uncertain
// nor failed authorizes replay. Only succeeded carries the validated receipt.
type ConnectionAccountWriteResult struct {
	Source       ConnectionAccountWriteSource `json:"source"`
	InvocationID string                       `json:"invocation_id,omitempty"`
	Status       string                       `json:"status"`
	RecordedAt   string                       `json:"recorded_at,omitempty"`
	Receipt      json.RawMessage              `json:"receipt,omitempty"`
}

// AccountWriteClaim holds only evidence identity. It never carries credentials.
type AccountWriteClaim struct {
	Subject      ConnectionAccountSubject
	Request      ConnectionAccountWriteRequest
	InvocationID string
	Fingerprint  string
}

type AccountWriteExecution struct {
	Status                 string
	Payload                json.RawMessage
	CredentialUpdateFailed bool
}
