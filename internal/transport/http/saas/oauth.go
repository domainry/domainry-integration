package saas

import (
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"net/http"
)

func (h *handler) registerOAuth() {
	h.mux.HandleFunc("GET /integration/v1/management/oauth-applications", func(w http.ResponseWriter, r *http.Request) {
		if !h.oauthReady(w) {
			return
		}
		value, err := h.oauthApplications.ListOAuthApplications(r.Context(), workspace(r))
		respond(w, map[string]any{"items": value}, err)
	})
	h.mux.HandleFunc("PUT /integration/v1/management/oauth-applications/{key}", func(w http.ResponseWriter, r *http.Request) {
		if !h.oauthReady(w) {
			return
		}
		var input integrationsdk.OAuthApplicationInput
		if !decode(w, r, &input) {
			return
		}
		value, err := h.oauthApplications.UpsertOAuthApplication(r.Context(), workspace(r), r.PathValue("key"), actor(r), input)
		respond(w, value, err)
	})
	h.mux.HandleFunc("GET /integration/v1/oauth-authorizations/options", func(w http.ResponseWriter, r *http.Request) {
		if !h.oauthReady(w) {
			return
		}
		value, err := h.oauthAuthorizations.ListOAuthAuthorizationOptions(r.Context(), accountSubject(r))
		respond(w, map[string]any{"items": value}, err)
	})
	h.mux.HandleFunc("POST /integration/v1/oauth-authorizations", func(w http.ResponseWriter, r *http.Request) {
		if !h.oauthReady(w) {
			return
		}
		var input integrationsdk.OAuthAuthorizationInput
		if !decode(w, r, &input) {
			return
		}
		value, err := h.oauthAuthorizations.StartOAuthAuthorization(r.Context(), accountSubject(r), input)
		respond(w, value, err)
	})
	h.mux.HandleFunc("GET /integration/v1/oauth-authorizations/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !h.oauthReady(w) {
			return
		}
		value, err := h.oauthAuthorizations.GetOAuthAuthorization(r.Context(), accountSubject(r), r.PathValue("id"))
		respond(w, value, err)
	})
	h.mux.HandleFunc("POST /integration/v1/oauth-authorizations/callback", func(w http.ResponseWriter, r *http.Request) {
		if !h.oauthReady(w) {
			return
		}
		var input integrationsdk.OAuthAuthorizationCallback
		if !decode(w, r, &input) {
			return
		}
		value, err := h.oauthAuthorizations.CompleteOAuthAuthorization(r.Context(), accountSubject(r), input)
		respond(w, value, err)
	})
}
func (h *handler) oauthReady(w http.ResponseWriter) bool {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if h.oauthApplications == nil || h.oauthAuthorizations == nil {
		writeJSON(w, 503, map[string]string{"code": "integration.oauth_unavailable"})
		return false
	}
	return true
}
