package integration

import (
	"context"
	"encoding/json"
	connector "github.com/domainry/domainry-connector-sdk"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"strings"
)

func (s *OperationsStore) guardInboundSubject(ctx context.Context, request model.WebhookRequest, provider string, verified connector.VerifiedWebhook) error {
	event := model.Event{WorkspaceID: request.WorkspaceID, Provider: provider, EventType: verified.EventType, Payload: verified.Payload}
	mapping, found, err := s.eventMapping(ctx, event)
	if err != nil {
		return err
	}
	subjectProvider, subject := provider, ""
	if found {
		subjectProvider, subject = mappedSubject(mapping, event)
	}
	if verified.ExternalIdentity != nil {
		subject = strings.TrimSpace(verified.ExternalIdentity.Subject)
	}
	refs := []subjectFenceReference{{"connection", "", request.ConnectionKey}}
	if subject != "" {
		refs = append(refs, subjectFenceReference{"external", subjectProvider, oauthHash(subject)})
	}
	return guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, request.WorkspaceID, refs...)
}
func verifiedInboundPayload(raw json.RawMessage, external *connector.WebhookExternalIdentity) (json.RawMessage, error) {
	if external == nil || strings.TrimSpace(external.Subject) == "" {
		return raw, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	contextValue, _ := payload["_integration_context"].(map[string]any)
	if contextValue == nil {
		contextValue = map[string]any{}
	}
	contextValue["verified_external_subject"] = strings.TrimSpace(external.Subject)
	payload["_integration_context"] = contextValue
	return json.Marshal(payload)
}
