package module

import (
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"net/http"
)

func (h *handler) oauthHandlers() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		integrationsdk.ActionIntegrationOAuthApplicationsList: h.listOAuthApplications, integrationsdk.ActionIntegrationOAuthApplicationsUpsert: h.upsertOAuthApplication,
		integrationsdk.ActionIntegrationOAuthAuthorizationsOptions: h.oauthOptions, integrationsdk.ActionIntegrationOAuthAuthorizationsStart: h.startOAuth,
		integrationsdk.ActionIntegrationOAuthAuthorizationsGet: h.getOAuth, integrationsdk.ActionIntegrationOAuthAuthorizationsComplete: h.completeOAuth,
	}
}
func (h *handler) oauthReady(w http.ResponseWriter) bool {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if h.oauthApplications == nil || h.oauthAuthorizations == nil {
		writeError(w, 503, "backend.integration.oauth_unavailable")
		return false
	}
	return true
}
func oauthResponse(w http.ResponseWriter, value any, err error) {
	if err != nil {
		writeError(w, 400, "backend.integration.oauth_failed")
		return
	}
	writeJSON(w, 200, value)
}
func (h *handler) listOAuthApplications(w http.ResponseWriter, r *http.Request) {
	if !h.oauthReady(w) {
		return
	}
	workspace, _, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	value, err := h.oauthApplications.ListOAuthApplications(r.Context(), workspace)
	oauthResponse(w, map[string]any{"applications": value}, err)
}
func (h *handler) upsertOAuthApplication(w http.ResponseWriter, r *http.Request) {
	if !h.oauthReady(w) {
		return
	}
	workspace, actor, ok := managementPrincipal(w, r)
	if !ok {
		return
	}
	var input integrationsdk.OAuthApplicationInput
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.oauthApplications.UpsertOAuthApplication(r.Context(), workspace, r.PathValue("applicationKey"), actor, input)
	oauthResponse(w, value, err)
}
func (h *handler) oauthOptions(w http.ResponseWriter, r *http.Request) {
	if !h.oauthReady(w) {
		return
	}
	subject, ok := connectionAccountSubject(w, r)
	if !ok {
		return
	}
	value, err := h.oauthAuthorizations.ListOAuthAuthorizationOptions(r.Context(), subject)
	oauthResponse(w, map[string]any{"options": value}, err)
}
func (h *handler) startOAuth(w http.ResponseWriter, r *http.Request) {
	if !h.oauthReady(w) {
		return
	}
	subject, ok := connectionAccountSubject(w, r)
	if !ok {
		return
	}
	var input integrationsdk.OAuthAuthorizationInput
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.oauthAuthorizations.StartOAuthAuthorization(r.Context(), subject, input)
	oauthResponse(w, value, err)
}
func (h *handler) getOAuth(w http.ResponseWriter, r *http.Request) {
	if !h.oauthReady(w) {
		return
	}
	subject, ok := connectionAccountSubject(w, r)
	if !ok {
		return
	}
	value, err := h.oauthAuthorizations.GetOAuthAuthorization(r.Context(), subject, r.PathValue("sessionID"))
	oauthResponse(w, value, err)
}
func (h *handler) completeOAuth(w http.ResponseWriter, r *http.Request) {
	if !h.oauthReady(w) {
		return
	}
	subject, ok := connectionAccountSubject(w, r)
	if !ok {
		return
	}
	var input integrationsdk.OAuthAuthorizationCallback
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.oauthAuthorizations.CompleteOAuthAuthorization(r.Context(), subject, input)
	oauthResponse(w, value, err)
}
