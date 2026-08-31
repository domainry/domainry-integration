package module

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

type surface struct {
	handler http.Handler
	routes  []modulehttp.Route
}

func (*surface) ContractVersion() string      { return modulehttp.ContractVersion }
func (*surface) Owner() string                { return "integration" }
func (*surface) Name() string                 { return "web_push_subscriptions" }
func (s *surface) Handler() http.Handler      { return s.handler }
func (s *surface) Routes() []modulehttp.Route { return append([]modulehttp.Route(nil), s.routes...) }

func NewSurface(binding integrationsdk.Binding) (modulehttp.Surface, error) {
	webPushBinding, ok := binding.(integrationsdk.WebPushBinding)
	if !ok || webPushBinding.WebPushSubscriptions() == nil {
		return nil, errors.New("Integration Web Push binding is unavailable")
	}
	h := &handler{subscriptions: webPushBinding.WebPushSubscriptions(), mux: http.NewServeMux()}
	h.register()
	user := func(pattern string) modulehttp.Route {
		return modulehttp.Route{Pattern: pattern, Exposures: []modulehttp.Exposure{modulehttp.ExposurePublic}, Authentication: modulehttp.AuthenticationAuthenticated, PrincipalOnly: true}
	}
	admin := func(pattern string) modulehttp.Route {
		return modulehttp.Route{Pattern: pattern, Exposures: []modulehttp.Exposure{modulehttp.ExposureTenantAdmin}, Authentication: modulehttp.AuthenticationAuthenticated, AnyPermissions: []string{"workspace.admin"}}
	}
	return &surface{handler: h.mux, routes: []modulehttp.Route{
		user("GET /business/notifications/web-push/readiness"),
		user("GET /business/notifications/web-push/subscriptions"),
		user("PUT /business/notifications/web-push/subscriptions/{subscriptionID}"),
		user("POST /business/notifications/web-push/subscriptions/{subscriptionID}/revoke"),
		admin("POST /integrations/web-push/subscriptions/cleanup-expired"),
	}}, nil
}

type handler struct {
	subscriptions integrationsdk.WebPushSubscriptions
	mux           *http.ServeMux
}

func (h *handler) register() {
	h.mux.HandleFunc("GET /business/notifications/web-push/readiness", h.readiness)
	h.mux.HandleFunc("GET /business/notifications/web-push/subscriptions", h.list)
	h.mux.HandleFunc("PUT /business/notifications/web-push/subscriptions/{subscriptionID}", h.upsert)
	h.mux.HandleFunc("POST /business/notifications/web-push/subscriptions/{subscriptionID}/revoke", h.revoke)
	h.mux.HandleFunc("POST /integrations/web-push/subscriptions/cleanup-expired", h.cleanup)
}

func requestPrincipal(r *http.Request) (identitysdk.Principal, bool) {
	principal, ok := identitysdk.PrincipalFromContext(r.Context())
	return principal, ok && principal.Known && strings.TrimSpace(principal.WorkspaceID) != "" && strings.TrimSpace(principal.UserID) != ""
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"code": code})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, value any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		writeError(w, http.StatusBadRequest, "backend.request.invalid_json")
		return false
	}
	return true
}

func (h *handler) readiness(w http.ResponseWriter, r *http.Request) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return
	}
	value, err := h.subscriptions.Readiness(r.Context(), p.WorkspaceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backend.integration.web_push_readiness_failed")
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return
	}
	values, err := h.subscriptions.List(r.Context(), p.WorkspaceID, p.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backend.integration.web_push_list_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": values, "count": len(values)})
}

func (h *handler) upsert(w http.ResponseWriter, r *http.Request) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return
	}
	var input integrationsdk.WebPushSubscriptionInput
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.subscriptions.Upsert(r.Context(), p.WorkspaceID, p.UserID, strings.TrimSpace(r.PathValue("subscriptionID")), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backend.integration.web_push_upsert_failed")
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) revoke(w http.ResponseWriter, r *http.Request) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return
	}
	if strings.TrimSpace(r.Header.Get("Idempotency-Key")) == "" {
		writeError(w, http.StatusBadRequest, "backend.idempotency.key_required")
		return
	}
	value, err := h.subscriptions.Revoke(r.Context(), p.WorkspaceID, p.UserID, strings.TrimSpace(r.PathValue("subscriptionID")))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backend.integration.web_push_revoke_failed")
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) cleanup(w http.ResponseWriter, r *http.Request) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return
	}
	count, err := h.subscriptions.CleanupExpired(r.Context(), p.WorkspaceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backend.integration.web_push_cleanup_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cleaned": count})
}

var _ modulehttp.Surface = (*surface)(nil)
