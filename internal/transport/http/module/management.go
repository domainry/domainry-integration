package module

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

func (h *handler) managementHandlers() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		integrationsdk.ActionIntegrationCatalogRead:                 h.integrationCatalog,
		integrationsdk.ActionIntegrationConnectorsList:              h.integrationCatalog,
		integrationsdk.ActionIntegrationConnectionsList:             h.listConnections,
		integrationsdk.ActionIntegrationConnectionsGet:              h.getConnection,
		integrationsdk.ActionIntegrationConnectionsValidate:         h.validateConnection,
		integrationsdk.ActionIntegrationConnectionsUpsert:           h.upsertConnection,
		integrationsdk.ActionIntegrationConnectionsDelete:           h.deleteConnection,
		integrationsdk.ActionIntegrationConnectionsDisable:          h.disableConnection,
		integrationsdk.ActionIntegrationConnectionsTestOperation:    h.testConnection,
		integrationsdk.ActionIntegrationSecretsList:                 h.listSecrets,
		integrationsdk.ActionIntegrationSecretsUpsert:               h.upsertSecret,
		integrationsdk.ActionIntegrationSecretsDisable:              h.transitionSecret("disable"),
		integrationsdk.ActionIntegrationSecretsRotate:               h.rotateSecret,
		integrationsdk.ActionIntegrationSecretsExpire:               h.transitionSecret("expire"),
		integrationsdk.ActionIntegrationSecretsRevoke:               h.transitionSecret("revoke"),
		integrationsdk.ActionIntegrationAPIKeysList:                 h.listAPIKeys,
		integrationsdk.ActionIntegrationAPIKeysCreate:               h.createAPIKey,
		integrationsdk.ActionIntegrationAPIKeysDisable:              h.disableAPIKey,
		integrationsdk.ActionIntegrationAPIKeysRotate:               h.rotateAPIKey,
		integrationsdk.ActionIntegrationExternalIdentitiesList:      h.listExternalIdentities,
		integrationsdk.ActionIntegrationExternalIdentitiesUpsert:    h.upsertExternalIdentity,
		integrationsdk.ActionIntegrationExternalIdentitiesDisable:   h.disableExternalIdentity,
		integrationsdk.ActionIntegrationExternalIdentitiesResolve:   h.resolveExternalIdentity,
		integrationsdk.ActionIntegrationWebhookSubscriptionsList:    h.listWebhookSubscriptions,
		integrationsdk.ActionIntegrationWebhookSubscriptionsUpsert:  h.upsertWebhookSubscription,
		integrationsdk.ActionIntegrationWebhookSubscriptionsDelete:  h.deleteWebhookSubscription,
		integrationsdk.ActionIntegrationWebhookSubscriptionsDisable: h.disableWebhookSubscription,
	}
}

func managementPrincipal(w http.ResponseWriter, r *http.Request) (workspaceID, actorID string, ok bool) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return "", "", false
	}
	return p.WorkspaceID, p.UserID, true
}

func writeManagementError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]string{"code": "backend.integration.management_failed", "message": err.Error()})
}

func (h *handler) integrationCatalog(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	definitions, err := h.catalog.ListConnectorDefinitions(r.Context())
	if err != nil {
		writeManagementError(w, err)
		return
	}
	connections, err := h.management.ListConnections(r.Context(), workspaceID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	connectors := make([]any, 0, len(definitions))
	for _, definition := range definitions {
		var projected any
		if len(definition.Definition) != 0 && json.Unmarshal(definition.Definition, &projected) == nil {
			connectors = append(connectors, projected)
			continue
		}
		connectors = append(connectors, definition)
	}
	writeJSON(w, http.StatusOK, map[string]any{"connectors": connectors, "connections": connections, "connections_available": true, "count": len(connectors)})
}

func (h *handler) listConnections(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	values, err := h.management.ListConnections(r.Context(), workspaceID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"connections": values, "count": len(values)})
}

func connectionHash(value integrationsdk.Connection) string {
	payload, _ := json.Marshal(value)
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func (h *handler) getConnection(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	value, err := h.management.GetConnection(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("connectionKey")))
	if err != nil {
		writeManagementError(w, err)
		return
	}
	w.Header().Set("X-Resource-Hash", connectionHash(value))
	writeJSON(w, http.StatusOK, value)
}

func decodeConnectionInput(w http.ResponseWriter, r *http.Request) (integrationsdk.ConnectionInput, bool) {
	var input integrationsdk.ConnectionInput
	if !decodeJSON(w, r, &input) {
		return input, false
	}
	if strings.TrimSpace(input.ConnectorKey) == "" || strings.TrimSpace(input.ProviderKey) == "" {
		writeError(w, http.StatusBadRequest, "backend.integration.connection_invalid")
		return input, false
	}
	return input, true
}

func (h *handler) validateConnection(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := managementPrincipal(w, r); !ok {
		return
	}
	input, ok := decodeConnectionInput(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"valid": true, "connection_key": strings.TrimSpace(r.PathValue("connectionKey")), "normalized": input})
}

func (h *handler) upsertConnection(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	input, ok := decodeConnectionInput(w, r)
	if !ok {
		return
	}
	value, err := h.management.UpsertConnection(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("connectionKey")), actorID, input)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) deleteConnection(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	if err := h.management.DeleteConnection(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("connectionKey"))); err != nil {
		writeManagementError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) disableConnection(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	value, err := h.management.SetConnectionStatus(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("connectionKey")), "disabled", actorID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) testConnection(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	var input integrationsdk.ConnectionTestRequest
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.management.TestConnection(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("connectionKey")), input)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) listSecrets(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	values, err := h.management.ListSecrets(r.Context(), workspaceID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": values, "count": len(values)})
}

func (h *handler) upsertSecret(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	var input integrationsdk.SecretInput
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.management.UpsertSecret(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("secretKey")), actorID, input)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) transitionSecret(transition string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		workspaceID, actorID, ok := managementPrincipal(w, r)
		if !ok {
			return
		}
		value, err := h.management.TransitionSecret(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("secretKey")), transition, actorID)
		if err != nil {
			writeManagementError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, value)
	}
}

func (h *handler) rotateSecret(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	var input integrationsdk.SecretInput
	if !decodeJSON(w, r, &input) {
		return
	}
	rotator, ok := h.management.(interface {
		RotateSecret(context.Context, string, string, string, integrationsdk.SecretInput) (integrationsdk.Secret, error)
	})
	if !ok {
		writeManagementError(w, fmt.Errorf("Integration secret rotation requires atomic owner persistence"))
		return
	}
	value, err := rotator.RotateSecret(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("secretKey")), actorID, input)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	values, err := h.management.ListAPIKeys(r.Context(), workspaceID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"api_keys": values, "count": len(values)})
}

func (h *handler) createAPIKey(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	var input integrationsdk.APIKeyInput
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.management.CreateAPIKey(r.Context(), workspaceID, actorID, input)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, value)
}

func (h *handler) disableAPIKey(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	value, err := h.management.DisableAPIKey(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("apiKey")), actorID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) rotateAPIKey(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	value, err := h.management.RotateAPIKey(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("apiKey")), actorID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) listExternalIdentities(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	values, err := h.management.ListExternalIdentities(r.Context(), workspaceID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"identities": values, "count": len(values)})
}

func (h *handler) upsertExternalIdentity(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	var input integrationsdk.ExternalIdentityInput
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.management.UpsertExternalIdentity(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("identityKey")), actorID, input)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) disableExternalIdentity(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	value, err := h.management.DisableExternalIdentity(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("identityKey")), actorID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) resolveExternalIdentity(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	var input struct {
		Provider        string `json:"provider"`
		ExternalSubject string `json:"external_subject"`
	}
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.management.ResolveExternalIdentity(r.Context(), workspaceID, input.Provider, input.ExternalSubject)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) listWebhookSubscriptions(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	values, err := h.management.ListWebhookSubscriptions(r.Context(), workspaceID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": values, "count": len(values)})
}

func (h *handler) upsertWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	var input integrationsdk.WebhookSubscriptionInput
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.management.UpsertWebhookSubscription(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("subscriptionKey")), actorID, input)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) deleteWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	if err := h.management.DeleteWebhookSubscription(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("subscriptionKey"))); err != nil {
		writeManagementError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) disableWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	workspaceID, actorID, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	value, err := h.management.DisableWebhookSubscription(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("subscriptionKey")), actorID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
