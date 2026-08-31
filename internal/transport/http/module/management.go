package module

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

func (h *handler) registerManagement() {
	h.mux.HandleFunc("GET /tenant-admin/integrations/catalog", h.integrationCatalog)
	h.mux.HandleFunc("GET /tenant-admin/integrations/connectors", h.integrationCatalog)
	h.mux.HandleFunc("GET /tenant-admin/integrations/connections", h.listConnections)
	h.mux.HandleFunc("GET /tenant-admin/integrations/connections/{connectionKey}", h.getConnection)
	h.mux.HandleFunc("POST /tenant-admin/integrations/connections/{connectionKey}/validate", h.validateConnection)
	h.mux.HandleFunc("PUT /tenant-admin/integrations/connections/{connectionKey}", h.upsertConnection)
	h.mux.HandleFunc("DELETE /tenant-admin/integrations/connections/{connectionKey}", h.deleteConnection)
	h.mux.HandleFunc("POST /tenant-admin/integrations/connections/{connectionKey}/disable", h.disableConnection)
	h.mux.HandleFunc("POST /tenant-admin/integrations/connections/{connectionKey}/test-operation", h.testConnection)
	h.mux.HandleFunc("GET /tenant-admin/integrations/secrets", h.listSecrets)
	h.mux.HandleFunc("PUT /tenant-admin/integrations/secrets/{secretKey}", h.upsertSecret)
	h.mux.HandleFunc("POST /tenant-admin/integrations/secrets/{secretKey}/disable", h.transitionSecret("disable"))
	h.mux.HandleFunc("POST /tenant-admin/integrations/secrets/{secretKey}/rotate", h.rotateSecret)
	h.mux.HandleFunc("POST /tenant-admin/integrations/secrets/{secretKey}/expire", h.transitionSecret("expire"))
	h.mux.HandleFunc("POST /tenant-admin/integrations/secrets/{secretKey}/revoke", h.transitionSecret("revoke"))
	h.mux.HandleFunc("GET /tenant-admin/integrations/api-keys", h.listAPIKeys)
	h.mux.HandleFunc("POST /tenant-admin/integrations/api-keys", h.createAPIKey)
	h.mux.HandleFunc("POST /tenant-admin/integrations/api-keys/{apiKey}/disable", h.disableAPIKey)
	h.mux.HandleFunc("POST /tenant-admin/integrations/api-keys/{apiKey}/rotate", h.rotateAPIKey)
	h.mux.HandleFunc("GET /tenant-admin/integrations/external-identities", h.listExternalIdentities)
	h.mux.HandleFunc("PUT /tenant-admin/integrations/external-identities/{identityKey}", h.upsertExternalIdentity)
	h.mux.HandleFunc("POST /tenant-admin/integrations/external-identities/{identityKey}/disable", h.disableExternalIdentity)
	h.mux.HandleFunc("POST /tenant-admin/integrations/external-identities/resolve", h.resolveExternalIdentity)
	h.mux.HandleFunc("GET /tenant-admin/integrations/webhook-subscriptions", h.listWebhookSubscriptions)
	h.mux.HandleFunc("PUT /tenant-admin/integrations/webhook-subscriptions/{subscriptionKey}", h.upsertWebhookSubscription)
	h.mux.HandleFunc("DELETE /tenant-admin/integrations/webhook-subscriptions/{subscriptionKey}", h.deleteWebhookSubscription)
	h.mux.HandleFunc("POST /tenant-admin/integrations/webhook-subscriptions/{subscriptionKey}/disable", h.disableWebhookSubscription)
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
	value, err := h.management.UpsertSecret(r.Context(), workspaceID, strings.TrimSpace(r.PathValue("secretKey")), actorID, input)
	if err == nil {
		value, err = h.management.TransitionSecret(r.Context(), workspaceID, value.Key, "rotate", actorID)
	}
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
