package integrationmodel

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	ProviderCallPersistenceStandard  = "standard"
	ProviderCallPersistenceSensitive = "sensitive"
)

type Invocation struct {
	ID                  string         `json:"id"`
	WorkspaceID         string         `json:"workspace_id,omitempty"`
	ConnectorKey        string         `json:"connector_key"`
	ProviderKey         string         `json:"provider_key,omitempty"`
	ConnectionKey       string         `json:"connection_key,omitempty"`
	Operation           string         `json:"operation"`
	Status              string         `json:"status"`
	DurationMS          int64          `json:"duration_ms,omitempty"`
	RequestRef          string         `json:"request_ref,omitempty"`
	ResponseRef         string         `json:"response_ref,omitempty"`
	Error               string         `json:"error,omitempty"`
	EventID             string         `json:"event_id,omitempty"`
	ObjectKey           string         `json:"object_key,omitempty"`
	RecordID            string         `json:"record_id,omitempty"`
	WorkflowExecutionID string         `json:"workflow_execution_id,omitempty"`
	Metadata            map[string]any `json:"metadata,omitempty"`
	CreatedAt           string         `json:"created_at,omitempty"`
	UpdatedAt           string         `json:"updated_at,omitempty"`
}

type InvocationQuery struct {
	WorkspaceID   string `json:"workspace_id"`
	ConnectorKey  string `json:"connector_key,omitempty"`
	ConnectionKey string `json:"connection_key,omitempty"`
	Operation     string `json:"operation,omitempty"`
	Status        string `json:"status,omitempty"`
	CreatedFrom   string `json:"created_from,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

type ProviderCallRequest struct {
	Source InvocationSource `json:"source,omitempty"`
	// ReadExpectation is set only by owner-side account-read orchestration.
	// JSON callers cannot supply or override this pre-I/O consistency guard.
	ReadExpectation   *ProviderReadExpectation `json:"-"`
	RequestID         string                   `json:"request_id"`
	WorkspaceID       string                   `json:"workspace_id"`
	ConnectorKey      string                   `json:"connector_key"`
	ConnectionKey     string                   `json:"connection_key,omitempty"`
	Operation         string                   `json:"operation"`
	Payload           json.RawMessage          `json:"payload"`
	PersistenceMode   string                   `json:"persistence_mode,omitempty"`
	MaskedDestination string                   `json:"masked_destination,omitempty"`
	ActorID           string                   `json:"actor_id,omitempty"`
	RoleKey           string                   `json:"role_key,omitempty"`
}

type InvocationSource struct {
	ExecutionID string `json:"execution_id,omitempty"`
	ObjectKey   string `json:"object_key,omitempty"`
	RecordID    string `json:"record_id,omitempty"`
}

func (s InvocationSource) Validate() error {
	for _, value := range []string{s.ExecutionID, s.ObjectKey, s.RecordID} {
		if value != "" && !boundedAccountWriteValue(value, 255) {
			return fmt.Errorf("Integration invocation source invalid")
		}
	}
	if s.RecordID != "" && (s.ExecutionID == "" || s.ObjectKey == "") {
		return fmt.Errorf("Integration invocation source incomplete")
	}
	return nil
}

type ProviderReadExpectation struct {
	ConnectionUpdatedAt string
	ProviderKey         string
	ContractSHA256      string
}

type ProviderCallResult struct {
	Invocation Invocation      `json:"invocation"`
	Response   json.RawMessage `json:"response,omitempty"`
}

type Event struct {
	ID            string                   `json:"id"`
	WorkspaceID   string                   `json:"workspace_id,omitempty"`
	ConnectorKey  string                   `json:"connector_key,omitempty"`
	ConnectionKey string                   `json:"connection_key,omitempty"`
	Provider      string                   `json:"provider"`
	EventType     string                   `json:"event_type"`
	ExternalID    string                   `json:"external_id"`
	Status        string                   `json:"status"`
	Payload       json.RawMessage          `json:"payload,omitempty"`
	Error         string                   `json:"error,omitempty"`
	AttemptCount  int                      `json:"attempt_count"`
	NextRetryAt   string                   `json:"next_retry_at,omitempty"`
	LastAttemptAt string                   `json:"last_attempt_at,omitempty"`
	ReceivedAt    string                   `json:"received_at"`
	UpdatedAt     string                   `json:"updated_at"`
	Execution     *RuntimeExecutionReceipt `json:"execution,omitempty"`
}

type EventQuery struct {
	WorkspaceID string `json:"workspace_id"`
	Provider    string `json:"provider,omitempty"`
	EventType   string `json:"event_type,omitempty"`
	Status      string `json:"status,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type WebhookRequest struct {
	WorkspaceID   string              `json:"workspace_id"`
	ConnectorKey  string              `json:"connector_key"`
	ConnectionKey string              `json:"connection_key"`
	Headers       map[string][]string `json:"headers,omitempty"`
	Query         map[string][]string `json:"query,omitempty"`
	Body          []byte              `json:"body"`
	ReceivedAt    time.Time           `json:"received_at"`
}

type WebhookReceipt struct {
	Event     Event  `json:"event"`
	Challenge string `json:"challenge,omitempty"`
	Format    string `json:"challenge_format,omitempty"`
}

type RuntimeExecutionReceipt struct {
	EventID     string `json:"event_id"`
	MappingKey  string `json:"mapping_key"`
	ExecutionID string `json:"execution_id"`
	TargetType  string `json:"target_type"`
	Status      string `json:"status"`
	ErrorCode   string `json:"error_code,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
}

type EventMappingRequirement struct {
	Key               string                             `json:"key"`
	WorkspaceID       string                             `json:"workspace_id"`
	Provider          string                             `json:"provider"`
	ConnectionKey     string                             `json:"connection_key,omitempty"`
	EventType         string                             `json:"event_type,omitempty"`
	CommandPrefix     string                             `json:"command_prefix,omitempty"`
	TargetType        string                             `json:"target_type"`
	WorkflowKey       string                             `json:"workflow_key,omitempty"`
	ObjectKey         string                             `json:"object_key,omitempty"`
	ObjectKeyPath     string                             `json:"object_key_path,omitempty"`
	RecordID          string                             `json:"record_id,omitempty"`
	RecordIDPath      string                             `json:"record_id_path,omitempty"`
	ActionKey         string                             `json:"action_key,omitempty"`
	ActionKeyPath     string                             `json:"action_key_path,omitempty"`
	ActionInput       map[string]string                  `json:"action_input,omitempty"`
	WorkflowInput     map[string]string                  `json:"workflow_input,omitempty"`
	AgentID           string                             `json:"agent_id,omitempty"`
	ConversationID    string                             `json:"conversation_id,omitempty"`
	AgentTaskMode     string                             `json:"agent_task_mode,omitempty"`
	RelatedTaskID     string                             `json:"related_task_id,omitempty"`
	RelatedTaskIDPath string                             `json:"related_task_id_path,omitempty"`
	AgentInput        map[string]string                  `json:"agent_input,omitempty"`
	EventFields       []EventFieldRequirement            `json:"event_fields,omitempty"`
	ExternalIdentity  ExternalIdentityMappingRequirement `json:"external_identity,omitempty"`
	Payload           map[string]any                     `json:"payload,omitempty"`
	Enabled           bool                               `json:"enabled"`
}

type EventFieldRequirement struct {
	Path     string   `json:"path"`
	Type     string   `json:"type"`
	Options  []string `json:"options,omitempty"`
	Required bool     `json:"required,omitempty"`
}

type ExternalIdentityMappingRequirement struct {
	Provider    string `json:"provider,omitempty"`
	SubjectPath string `json:"subject_path,omitempty"`
	SubjectType string `json:"subject_type,omitempty"`
	NamePath    string `json:"name_path,omitempty"`
	OnUnmapped  string `json:"on_unmapped,omitempty"`
}
