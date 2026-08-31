package module

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

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
func (*surface) Name() string                 { return "integration_product" }
func (s *surface) Handler() http.Handler      { return s.handler }
func (s *surface) Routes() []modulehttp.Route { return append([]modulehttp.Route(nil), s.routes...) }

func NewSurface(binding integrationsdk.Binding) (modulehttp.Surface, error) {
	webPushBinding, ok := binding.(integrationsdk.WebPushBinding)
	if !ok || webPushBinding.WebPushSubscriptions() == nil {
		return nil, errors.New("Integration Web Push binding is unavailable")
	}
	managementBinding, ok := binding.(integrationsdk.ManagementBinding)
	if !ok || managementBinding.Management() == nil {
		return nil, errors.New("Integration Management binding is unavailable")
	}
	operationsBinding, ok := binding.(integrationsdk.OperationsBinding)
	if !ok || operationsBinding.Operations() == nil {
		return nil, errors.New("Integration Operations binding is unavailable")
	}
	h := &handler{catalog: binding.Catalog(), management: managementBinding.Management(), operations: operationsBinding.Operations(), subscriptions: webPushBinding.WebPushSubscriptions(), mux: http.NewServeMux()}
	h.register()
	return &surface{handler: h.mux, routes: integrationRoutes()}, nil
}

func integrationRoutes() []modulehttp.Route {
	contract := integrationsdk.IntegrationHTTPSurfaceContract()
	routes := make([]modulehttp.Route, 0, len(contract.Routes))
	for _, route := range contract.Routes {
		exposures := make([]modulehttp.Exposure, len(route.Exposures))
		for index, exposure := range route.Exposures {
			exposures[index] = modulehttp.Exposure(exposure)
		}
		governance := &modulehttp.Governance{
			EffectClass:         modulehttp.EffectClass(route.EffectClass),
			HighRiskPolicy:      modulehttp.HighRiskPolicy(route.HighRiskPolicy),
			IdempotencyDecision: route.IdempotencyDecision,
			AuditClass:          route.AuditClass,
		}
		routes = append(routes, modulehttp.Route{
			Pattern: route.Pattern, Exposures: exposures, Authentication: modulehttp.Authentication(route.Authentication),
			Permission: route.Permission, AnyPermissions: append([]string(nil), route.AnyPermissions...), PrincipalOnly: route.PrincipalOnly,
			Governance: governance,
		})
	}
	return routes
}

type handler struct {
	catalog       integrationsdk.Catalog
	management    integrationsdk.Management
	operations    integrationsdk.Operations
	subscriptions integrationsdk.WebPushSubscriptions
	mux           *http.ServeMux
}

func (h *handler) register() {
	h.registerManagement()
	h.mux.HandleFunc("GET /business/notifications/web-push/readiness", h.readiness)
	h.mux.HandleFunc("GET /business/notifications/web-push/subscriptions", h.list)
	h.mux.HandleFunc("PUT /business/notifications/web-push/subscriptions/{subscriptionID}", h.upsert)
	h.mux.HandleFunc("POST /business/notifications/web-push/subscriptions/{subscriptionID}/revoke", h.revoke)
	h.mux.HandleFunc("POST /integrations/web-push/subscriptions/cleanup-expired", h.cleanup)
	h.mux.HandleFunc("GET /tenant-admin/integrations/invocations", h.listInvocations)
	h.mux.HandleFunc("GET /tenant-admin/integrations/invocations/{invocationID}", h.getInvocation)
	h.mux.HandleFunc("GET /tenant-admin/integrations/events", h.listEvents)
	h.mux.HandleFunc("GET /tenant-admin/integrations/events/{eventID}", h.getEvent)
	h.mux.HandleFunc("POST /tenant-admin/integrations/events/{eventID}/replay", h.replayEvent)
	h.mux.HandleFunc("POST /integrations/webhooks/{workspaceID}/{connectorKey}/{connectionKey}", h.webhook)
}

func (h *handler) listInvocations(w http.ResponseWriter, r *http.Request) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return
	}
	values, err := h.operations.ListInvocations(r.Context(), integrationsdk.InvocationQuery{WorkspaceID: p.WorkspaceID, ConnectorKey: r.URL.Query().Get("connector_key"), ConnectionKey: r.URL.Query().Get("connection_key"), Operation: r.URL.Query().Get("operation"), Status: r.URL.Query().Get("status"), CreatedFrom: r.URL.Query().Get("created_from")})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backend.integration.invocation_list_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invocations": values, "count": len(values)})
}

func (h *handler) getInvocation(w http.ResponseWriter, r *http.Request) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return
	}
	value, err := h.operations.GetInvocation(r.Context(), p.WorkspaceID, r.PathValue("invocationID"))
	if err != nil {
		writeError(w, http.StatusNotFound, "backend.integration.invocation_not_found")
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) listEvents(w http.ResponseWriter, r *http.Request) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return
	}
	values, err := h.operations.ListEvents(r.Context(), integrationsdk.EventQuery{WorkspaceID: p.WorkspaceID, Provider: r.URL.Query().Get("provider"), EventType: r.URL.Query().Get("event_type"), Status: r.URL.Query().Get("status")})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backend.integration.event_list_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": values, "count": len(values)})
}

func (h *handler) getEvent(w http.ResponseWriter, r *http.Request) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return
	}
	value, err := h.operations.GetEvent(r.Context(), p.WorkspaceID, r.PathValue("eventID"))
	if err != nil {
		writeError(w, http.StatusNotFound, "backend.integration.event_not_found")
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) replayEvent(w http.ResponseWriter, r *http.Request) {
	p, ok := requestPrincipal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "auth.principal_required")
		return
	}
	value, err := h.operations.ReplayEvent(r.Context(), p.WorkspaceID, r.PathValue("eventID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "backend.integration.event_replay_failed")
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) webhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "backend.integration.webhook_body_invalid")
		return
	}
	receipt, err := h.operations.AcceptWebhook(r.Context(), integrationsdk.WebhookRequest{WorkspaceID: r.PathValue("workspaceID"), ConnectorKey: r.PathValue("connectorKey"), ConnectionKey: r.PathValue("connectionKey"), Headers: r.Header.Clone(), Query: r.URL.Query(), Body: body, ReceivedAt: time.Now().UTC()})
	if err != nil {
		writeError(w, http.StatusUnauthorized, "backend.integration.webhook_rejected")
		return
	}
	if receipt.Challenge != "" && receipt.Format == "plain" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(receipt.Challenge))
		return
	}
	writeJSON(w, http.StatusAccepted, receipt)
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
