package saas

import (
	"net/http"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

func (h *handler) registerManagement() {
	h.mux.HandleFunc("GET /v1/management/connections", h.listConnections)
	h.mux.HandleFunc("GET /v1/management/connections/{key}", h.getConnection)
	h.mux.HandleFunc("PUT /v1/management/connections/{key}", h.upsertConnection)
	h.mux.HandleFunc("DELETE /v1/management/connections/{key}", h.deleteConnection)
	h.mux.HandleFunc("POST /v1/management/connections/{key}/status", h.setConnectionStatus)
	h.mux.HandleFunc("POST /v1/management/connections/{key}/test", h.testConnection)
	h.mux.HandleFunc("GET /v1/management/secrets", h.listSecrets)
	h.mux.HandleFunc("PUT /v1/management/secrets/{key}", h.upsertSecret)
	h.mux.HandleFunc("POST /v1/management/secrets/{key}/transition", h.transitionSecret)
	h.mux.HandleFunc("GET /v1/management/api-keys", h.listAPIKeys)
	h.mux.HandleFunc("POST /v1/management/api-keys", h.createAPIKey)
	h.mux.HandleFunc("POST /v1/management/api-keys/{key}/disable", h.disableAPIKey)
	h.mux.HandleFunc("POST /v1/management/api-keys/{key}/rotate", h.rotateAPIKey)
	h.mux.HandleFunc("GET /v1/management/external-identities", h.listExternalIdentities)
	h.mux.HandleFunc("PUT /v1/management/external-identities/{key}", h.upsertExternalIdentity)
	h.mux.HandleFunc("POST /v1/management/external-identities/{key}/disable", h.disableExternalIdentity)
	h.mux.HandleFunc("POST /v1/management/external-identities/resolve", h.resolveExternalIdentity)
	h.mux.HandleFunc("GET /v1/management/webhook-subscriptions", h.listWebhookSubscriptions)
	h.mux.HandleFunc("PUT /v1/management/webhook-subscriptions/{key}", h.upsertWebhookSubscription)
	h.mux.HandleFunc("DELETE /v1/management/webhook-subscriptions/{key}", h.deleteWebhookSubscription)
	h.mux.HandleFunc("POST /v1/management/webhook-subscriptions/{key}/disable", h.disableWebhookSubscription)
}

func workspace(r *http.Request) string { return r.URL.Query().Get("workspace_id") }
func actor(r *http.Request) string     { return r.URL.Query().Get("actor_id") }

func (h *handler) listConnections(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.ListConnections(r.Context(), workspace(r))
	respond(w, map[string]any{"items": v}, e)
}
func (h *handler) getConnection(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.GetConnection(r.Context(), workspace(r), r.PathValue("key"))
	respond(w, v, e)
}
func (h *handler) upsertConnection(w http.ResponseWriter, r *http.Request) {
	var input integrationsdk.ConnectionInput
	if !decode(w, r, &input) {
		return
	}
	v, e := h.management.UpsertConnection(r.Context(), workspace(r), r.PathValue("key"), actor(r), input)
	respond(w, v, e)
}
func (h *handler) deleteConnection(w http.ResponseWriter, r *http.Request) {
	if e := h.management.DeleteConnection(r.Context(), workspace(r), r.PathValue("key")); e != nil {
		writeError(w, e)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *handler) setConnectionStatus(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Status string `json:"status"`
	}
	if !decode(w, r, &input) {
		return
	}
	v, e := h.management.SetConnectionStatus(r.Context(), workspace(r), r.PathValue("key"), input.Status, actor(r))
	respond(w, v, e)
}
func (h *handler) testConnection(w http.ResponseWriter, r *http.Request) {
	var input integrationsdk.ConnectionTestRequest
	if !decode(w, r, &input) {
		return
	}
	v, e := h.management.TestConnection(r.Context(), workspace(r), r.PathValue("key"), input)
	respond(w, v, e)
}
func (h *handler) listSecrets(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.ListSecrets(r.Context(), workspace(r))
	respond(w, map[string]any{"items": v}, e)
}
func (h *handler) upsertSecret(w http.ResponseWriter, r *http.Request) {
	var input integrationsdk.SecretInput
	if !decode(w, r, &input) {
		return
	}
	v, e := h.management.UpsertSecret(r.Context(), workspace(r), r.PathValue("key"), actor(r), input)
	respond(w, v, e)
}
func (h *handler) transitionSecret(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Transition string `json:"transition"`
	}
	if !decode(w, r, &input) {
		return
	}
	v, e := h.management.TransitionSecret(r.Context(), workspace(r), r.PathValue("key"), input.Transition, actor(r))
	respond(w, v, e)
}
func (h *handler) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.ListAPIKeys(r.Context(), workspace(r))
	respond(w, map[string]any{"items": v}, e)
}
func (h *handler) createAPIKey(w http.ResponseWriter, r *http.Request) {
	var input integrationsdk.APIKeyInput
	if !decode(w, r, &input) {
		return
	}
	v, e := h.management.CreateAPIKey(r.Context(), workspace(r), actor(r), input)
	respond(w, v, e)
}
func (h *handler) disableAPIKey(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.DisableAPIKey(r.Context(), workspace(r), r.PathValue("key"), actor(r))
	respond(w, v, e)
}
func (h *handler) rotateAPIKey(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.RotateAPIKey(r.Context(), workspace(r), r.PathValue("key"), actor(r))
	respond(w, v, e)
}
func (h *handler) listExternalIdentities(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.ListExternalIdentities(r.Context(), workspace(r))
	respond(w, map[string]any{"items": v}, e)
}
func (h *handler) upsertExternalIdentity(w http.ResponseWriter, r *http.Request) {
	var input integrationsdk.ExternalIdentityInput
	if !decode(w, r, &input) {
		return
	}
	v, e := h.management.UpsertExternalIdentity(r.Context(), workspace(r), r.PathValue("key"), actor(r), input)
	respond(w, v, e)
}
func (h *handler) disableExternalIdentity(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.DisableExternalIdentity(r.Context(), workspace(r), r.PathValue("key"), actor(r))
	respond(w, v, e)
}
func (h *handler) resolveExternalIdentity(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.ResolveExternalIdentity(r.Context(), workspace(r), r.URL.Query().Get("provider"), r.URL.Query().Get("external_subject"))
	respond(w, v, e)
}
func (h *handler) listWebhookSubscriptions(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.ListWebhookSubscriptions(r.Context(), workspace(r))
	respond(w, map[string]any{"items": v}, e)
}
func (h *handler) upsertWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	var input integrationsdk.WebhookSubscriptionInput
	if !decode(w, r, &input) {
		return
	}
	v, e := h.management.UpsertWebhookSubscription(r.Context(), workspace(r), r.PathValue("key"), actor(r), input)
	respond(w, v, e)
}
func (h *handler) deleteWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	if e := h.management.DeleteWebhookSubscription(r.Context(), workspace(r), r.PathValue("key")); e != nil {
		writeError(w, e)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *handler) disableWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	v, e := h.management.DisableWebhookSubscription(r.Context(), workspace(r), r.PathValue("key"), actor(r))
	respond(w, v, e)
}
