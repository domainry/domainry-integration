package integrationmodel

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mime"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const MaximumProviderCallContentBytes = 256 << 10

type ProviderCallContent struct {
	Data        []byte `json:"data"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type"`
}

type ProviderCallTarget struct {
	ConnectionUpdatedAt string `json:"connection_updated_at"`
	ConnectionSHA256    string `json:"connection_sha256"`
	ProviderKey         string `json:"provider_key"`
	ContractSHA256      string `json:"contract_sha256"`
}

func (r ProviderCallRequest) ValidateContent() error {
	if r.ExpectedConnection != nil {
		v := r.ExpectedConnection
		if !boundedAccountWriteValue(v.ConnectionUpdatedAt, 1024) || !connector.ValidSHA256(v.ConnectionSHA256) || !boundedAccountWriteValue(v.ProviderKey, 255) || !connector.ValidSHA256(v.ContractSHA256) || r.PersistenceMode != ProviderCallPersistenceSensitive || !boundedAccountWriteValue(r.RequestID, 255) || !boundedAccountWriteValue(r.WorkspaceID, 255) || !boundedAccountWriteValue(r.ConnectorKey, 255) || !boundedAccountWriteValue(r.Operation, 128) || !boundedAccountWriteValue(r.ConnectionKey, 255) || !boundedAccountWriteValue(r.ActorID, 255) || !boundedAccountWriteValue(r.RoleKey, 255) || !boundedAccountWriteValue(r.MaskedDestination, 255) || r.Source.ExecutionID == "" || r.Source.Validate() != nil || len(r.Payload) > MaximumProviderCallContentBytes || !json.Valid(r.Payload) {
			return fmt.Errorf("Integration guarded call requires a sensitive named connection, authenticated host source and bounded metadata")
		}
	}
	if c := r.Content; c != nil {
		if len(c.Data) == 0 || len(c.Data) > MaximumProviderCallContentBytes || len(c.ContentType) > 128 || c.ContentType != strings.TrimSpace(c.ContentType) || strings.ContainsAny(c.ContentType, "\r\n\x00") || r.ExpectedConnection == nil {
			return fmt.Errorf("Integration provider content is invalid or exceeds its bound")
		}
		if _, _, err := mime.ParseMediaType(c.ContentType); err != nil {
			return fmt.Errorf("Integration provider content type is invalid")
		}
		digest := sha256.Sum256(c.Data)
		if c.SHA256 != hex.EncodeToString(digest[:]) {
			return fmt.Errorf("Integration provider content SHA-256 does not match")
		}
	}
	return nil
}

// GuardedCallSHA256 binds a once-only invocation to the exact immutable host
// source, target revision, metadata and body identity without retaining bytes.
func (r ProviderCallRequest) GuardedCallSHA256() string {
	if r.ExpectedConnection == nil {
		return ""
	}
	var content any
	if r.Content != nil {
		content = struct {
			Bytes               int
			SHA256, ContentType string
		}{len(r.Content.Data), r.Content.SHA256, r.Content.ContentType}
	}
	raw, err := json.Marshal(struct {
		RequestID, WorkspaceID, ActorID, RoleKey, ConnectorKey, ConnectionKey, Operation, MaskedDestination string
		Source                                                                                              InvocationSource
		Target                                                                                              *ProviderCallTarget
		Payload                                                                                             json.RawMessage
		Content                                                                                             any
	}{r.RequestID, r.WorkspaceID, r.ActorID, r.RoleKey, r.ConnectorKey, r.ConnectionKey, r.Operation, r.MaskedDestination, r.Source, r.ExpectedConnection, r.Payload, content})
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
