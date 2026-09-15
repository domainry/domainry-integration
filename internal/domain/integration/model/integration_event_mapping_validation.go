package integrationmodel

import (
	"fmt"
	"strings"
)

// Validate closes the Integration-owned half of an inbound mapping contract.
// Runtime separately verifies that the finite Action or Workflow target exists
// and that the declared input bindings match that target's authored contract.
func (r EventMappingRequirement) Validate() error {
	for name, value := range map[string]string{"key": r.Key, "workspace_id": r.WorkspaceID, "provider": r.Provider, "target_type": r.TargetType} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("Integration event mapping %s is required", name)
		}
	}
	switch strings.TrimSpace(r.TargetType) {
	case "action":
		if strings.TrimSpace(r.ObjectKeyPath) != "" || strings.TrimSpace(r.ActionKeyPath) != "" {
			return fmt.Errorf("Integration action event mapping requires finite static object_key and action_key targets")
		}
		if strings.TrimSpace(r.ObjectKey) == "" || strings.TrimSpace(r.ActionKey) == "" {
			return fmt.Errorf("Integration action event mapping requires object_key and action_key")
		}
		if strings.TrimSpace(r.RecordID) == "" && strings.TrimSpace(r.RecordIDPath) == "" {
			return fmt.Errorf("Integration action event mapping requires record_id or record_id_path")
		}
	case "workflow":
		if strings.TrimSpace(r.WorkflowKey) == "" {
			return fmt.Errorf("Integration workflow event mapping requires workflow_key")
		}
	case "agent_task":
		if strings.TrimSpace(r.AgentID) == "" || strings.TrimSpace(r.ConversationID) == "" {
			return fmt.Errorf("Integration Agent event mapping requires agent_id and conversation_id")
		}
		if strings.TrimSpace(r.ExternalIdentity.SubjectPath) == "" {
			return fmt.Errorf("Integration Agent event mapping requires external_identity.subject_path")
		}
		switch strings.TrimSpace(r.AgentTaskMode) {
		case "start":
			if strings.TrimSpace(r.RelatedTaskID) != "" || strings.TrimSpace(r.RelatedTaskIDPath) != "" {
				return fmt.Errorf("Integration Agent start event mapping cannot declare related_task_id")
			}
		case "wake":
			if strings.TrimSpace(r.RelatedTaskID) == "" && strings.TrimSpace(r.RelatedTaskIDPath) == "" {
				return fmt.Errorf("Integration Agent wake event mapping requires related_task_id or related_task_id_path")
			}
		default:
			return fmt.Errorf("Integration Agent event mapping agent_task_mode must be start or wake")
		}
	default:
		return fmt.Errorf("Integration event mapping target_type %q is unsupported", r.TargetType)
	}

	declared := map[string]EventFieldRequirement{}
	for index, field := range r.EventFields {
		path := strings.TrimSpace(field.Path)
		if !validEventPath(path) {
			return fmt.Errorf("Integration event mapping event_fields[%d].path is invalid", index)
		}
		if _, exists := declared[path]; exists {
			return fmt.Errorf("Integration event mapping event field path %q is duplicated", path)
		}
		if !supportedEventFieldType(field.Type) {
			return fmt.Errorf("Integration event mapping event field %q has unsupported type %q", path, field.Type)
		}
		declared[path] = field
	}
	requireDeclared := func(name, path string) error {
		path = strings.TrimSpace(path)
		if path == "" {
			return nil
		}
		if _, exists := declared[path]; !exists {
			return fmt.Errorf("Integration event mapping %s path %q is not declared in event_fields", name, path)
		}
		return nil
	}
	for key, path := range r.ActionInput {
		if err := requireDeclared("action_input."+key, path); err != nil {
			return err
		}
	}
	for key, path := range r.WorkflowInput {
		if err := requireDeclared("workflow_input."+key, path); err != nil {
			return err
		}
	}
	for key, path := range r.AgentInput {
		if err := requireDeclared("agent_input."+key, path); err != nil {
			return err
		}
	}
	for name, path := range map[string]string{
		"record_id_path":                 r.RecordIDPath,
		"related_task_id_path":           r.RelatedTaskIDPath,
		"external_identity.subject_path": r.ExternalIdentity.SubjectPath,
		"external_identity.name_path":    r.ExternalIdentity.NamePath,
	} {
		if err := requireDeclared(name, path); err != nil {
			return err
		}
	}
	if strings.TrimSpace(r.CommandPrefix) != "" {
		if err := requireDeclared("command_prefix", "command"); err != nil {
			return err
		}
	}
	return nil
}

func validEventPath(path string) bool {
	if path == "" {
		return false
	}
	for _, segment := range strings.Split(path, ".") {
		if strings.TrimSpace(segment) == "" || strings.TrimSpace(segment) != segment {
			return false
		}
	}
	return true
}

func supportedEventFieldType(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "text", "string", "long_text", "email", "url", "date", "datetime", "relation", "user", "file",
		"number", "decimal", "currency", "percent", "integer", "boolean", "bool", "object", "map", "array", "list", "json":
		return true
	default:
		return false
	}
}
