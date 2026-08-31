package integrationmodel

import "encoding/json"

type ConnectorDefinition struct {
	Key         string          `json:"key"`
	DisplayName string          `json:"display_name"`
	Operations  []string        `json:"operations"`
	Schema      json.RawMessage `json:"schema,omitempty"`
	Definition  json.RawMessage `json:"definition"`
}

type ConnectionRequirement struct {
	Key          string          `json:"key"`
	WorkspaceID  string          `json:"workspace_id"`
	ConnectorKey string          `json:"connector_key"`
	ProviderKey  string          `json:"provider_key"`
	Name         string          `json:"name,omitempty"`
	Status       string          `json:"status,omitempty"`
	Config       json.RawMessage `json:"config"`
}

type DeliveryRequest struct {
	MessageID        string          `json:"message_id"`
	DeduplicationKey string          `json:"deduplication_key"`
	WorkspaceID      string          `json:"workspace_id"`
	ConnectorKey     string          `json:"connector_key"`
	ConnectionKey    string          `json:"connection_key,omitempty"`
	Operation        string          `json:"operation"`
	Payload          json.RawMessage `json:"payload"`
}

type DeliveryReceipt struct {
	MessageID    string `json:"message_id"`
	InvocationID string `json:"invocation_id"`
	Status       string `json:"status"`
	ResultRef    string `json:"result_ref,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
}

type WebPushSubscription struct {
	ID           string `json:"id"`
	WorkspaceID  string `json:"workspace_id,omitempty"`
	UserID       string `json:"user_id"`
	EndpointHash string `json:"endpoint_hash"`
	Status       string `json:"status"`
	ExpiresAt    string `json:"expires_at,omitempty"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
	RevokedAt    string `json:"revoked_at,omitempty"`
}

type WebPushSubscriptionInput struct {
	Endpoint  string `json:"endpoint"`
	P256DH    string `json:"p256dh"`
	Auth      string `json:"auth"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

type WebPushReadiness struct {
	Ready         bool   `json:"ready"`
	PublicKey     string `json:"public_key,omitempty"`
	ConnectionKey string `json:"connection_key,omitempty"`
	Status        string `json:"status"`
	Reason        string `json:"reason,omitempty"`
}
