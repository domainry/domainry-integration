package integrationmodel

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

// ConnectionAccountSubject is the owner application's current-action authority.
// The SDK adapter converts its transport DTO at the boundary.
type ConnectionAccountSubject struct {
	WorkspaceID string                  `json:"workspace_id"`
	UserID      string                  `json:"user_id"`
	Access      ConnectionAccountAccess `json:"access"`
}
type ConnectionAccountAccess struct {
	Personal  bool `json:"personal"`
	Workspace bool `json:"workspace"`
}

func (s ConnectionAccountSubject) Validate() error {
	if strings.TrimSpace(s.WorkspaceID) == "" || strings.TrimSpace(s.UserID) == "" {
		return fmt.Errorf("Integration connection account subject is incomplete")
	}
	return nil
}

type ConnectionAccountReadOperation struct {
	Operation      string `json:"operation"`
	ContractSHA256 string `json:"contract_sha256"`
}

func (r ConnectionAccountReadOperation) Validate() error {
	if !boundedAccountReadValue(r.Operation, 128) {
		return fmt.Errorf("Integration account read operation is invalid")
	}
	hash, err := hex.DecodeString(r.ContractSHA256)
	if err != nil || len(hash) != 32 || strings.ToLower(r.ContractSHA256) != r.ContractSHA256 {
		return fmt.Errorf("Integration account read contract is invalid")
	}
	return nil
}

type ConnectionAccountReadRequest struct {
	RequestID      string          `json:"request_id"`
	Operation      string          `json:"operation"`
	ContractSHA256 string          `json:"contract_sha256"`
	Payload        json.RawMessage `json:"payload"`
}

func (r ConnectionAccountReadRequest) OperationContract() ConnectionAccountReadOperation {
	return ConnectionAccountReadOperation{Operation: r.Operation, ContractSHA256: r.ContractSHA256}
}
func (r ConnectionAccountReadRequest) Validate() error {
	if err := r.OperationContract().Validate(); err != nil {
		return err
	}
	if !boundedAccountReadValue(r.RequestID, 256) || len(r.Payload) > 1<<20 || !json.Valid(r.Payload) {
		return fmt.Errorf("Integration account read request is invalid")
	}
	return nil
}

func boundedAccountReadValue(value string, limit int) bool {
	return value != "" && len(value) <= limit && strings.TrimSpace(value) == value && strings.IndexFunc(value, unicode.IsControl) < 0
}

// Source binds retained data to the exact authorized account revision and
// operation contract. It contains no credentials. Consumers must reauthorize
// with a fresh host-derived Subject before exposing a saved result.
type ConnectionAccountReadSource struct {
	WorkspaceID      string `json:"workspace_id"`
	ConnectionKey    string `json:"connection_key"`
	ConnectorKey     string `json:"connector_key"`
	ProviderKey      string `json:"provider_key"`
	AccountUpdatedAt string `json:"account_updated_at"`
	Operation        string `json:"operation"`
	ContractSHA256   string `json:"contract_sha256"`
}

type ConnectionAccountReadAccess struct {
	Source            ConnectionAccountReadSource `json:"source"`
	ScopeAlternatives [][]string                  `json:"scope_alternatives"`
}

type ConnectionAccountReadResult struct {
	Source       ConnectionAccountReadSource `json:"source"`
	InvocationID string                      `json:"invocation_id"`
	ReadAt       string                      `json:"read_at"`
	// Sensitive responses are never persisted by Integration. Replaying a
	// succeeded request returns evidence with PayloadAvailable=false.
	PayloadAvailable bool            `json:"payload_available"`
	Payload          json.RawMessage `json:"payload,omitempty"`
}
