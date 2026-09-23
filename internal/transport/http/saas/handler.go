package saas

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

func NewHandler(binding integrationsdk.Binding, serviceToken string) (http.Handler, error) {
	if binding == nil {
		return nil, fmt.Errorf("Integration SaaS binding is required")
	}
	webPush, ok := binding.(integrationsdk.WebPushBinding)
	if !ok {
		return nil, fmt.Errorf("Integration SaaS Web Push binding is required")
	}
	management, ok := binding.(integrationsdk.ManagementBinding)
	if !ok || management.Management() == nil {
		return nil, fmt.Errorf("Integration SaaS Management binding is required")
	}
	accounts, ok := binding.(integrationsdk.ConnectionAccountsBinding)
	if !ok || accounts.ConnectionAccounts() == nil {
		return nil, fmt.Errorf("Integration SaaS connection accounts binding is required")
	}
	accountAdmin, ok := binding.(integrationsdk.ConnectionAccountAdministrationBinding)
	if !ok || accountAdmin.ConnectionAccountAdministration() == nil {
		return nil, fmt.Errorf("Integration SaaS connection account administration binding is required")
	}
	operations, ok := binding.(integrationsdk.OperationsBinding)
	if !ok || operations.Operations() == nil {
		return nil, fmt.Errorf("Integration SaaS Operations binding is required")
	}
	token := strings.TrimSpace(serviceToken)
	if token == "" {
		return nil, fmt.Errorf("Integration SaaS service token is required")
	}
	h := &handler{binding: binding, webPush: webPush.WebPushSubscriptions(), management: management.Management(), accounts: accounts.ConnectionAccounts(), accountAdmin: accountAdmin.ConnectionAccountAdministration(), operations: operations.Operations(), token: token, mux: http.NewServeMux()}
	if port, ok := binding.(integrationsdk.ConnectionAccountReadsBinding); ok {
		h.accountReads = port.ConnectionAccountReads()
	}
	if port, ok := binding.(integrationsdk.OAuthApplicationsBinding); ok {
		h.oauthApplications = port.OAuthApplications()
	}
	if port, ok := binding.(integrationsdk.OAuthAuthorizationsBinding); ok {
		h.oauthAuthorizations = port.OAuthAuthorizations()
	}
	if port, ok := binding.(integrationsdk.ConnectionAccountWritesBinding); ok {
		h.accountWrites = port.ConnectionAccountWrites()
	}
	h.registerAccountWrites()
	h.registerSubjectLifecycle()
	h.registerOAuth()
	h.register()
	return h, nil
}

type handler struct {
	oauthApplications   integrationsdk.OAuthApplications
	oauthAuthorizations integrationsdk.OAuthAuthorizations
	binding             integrationsdk.Binding
	webPush             integrationsdk.WebPushSubscriptions
	management          integrationsdk.Management
	accounts            integrationsdk.ConnectionAccounts
	accountReads        integrationsdk.ConnectionAccountReads
	accountWrites       integrationsdk.ConnectionAccountWrites
	accountAdmin        integrationsdk.ConnectionAccountAdministration
	operations          integrationsdk.Operations
	token               string
	mux                 *http.ServeMux
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/integration/v1/public/webhooks/") {
		h.mux.ServeHTTP(w, r)
		return
	}
	provided := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if subtle.ConstantTimeCompare([]byte(provided), []byte(h.token)) != 1 || strings.TrimSpace(r.Header.Get("X-Domainry-Runtime-ID")) == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "integration.unauthorized"})
		return
	}
	h.mux.ServeHTTP(w, r)
}
func (h *handler) register() {
	h.mux.HandleFunc("GET /integration/v1/descriptor", h.descriptor)
	h.mux.HandleFunc("GET /integration/v1/connector-definitions", h.catalog)
	h.mux.HandleFunc("PUT /integration/v1/application-requirements/connections", h.requirements)
	h.mux.HandleFunc("PUT /integration/v1/application-requirements/event-mappings", h.eventMappingRequirements)
	h.mux.HandleFunc("POST /integration/v1/deliveries", h.accept)
	h.mux.HandleFunc("GET /integration/v1/deliveries/{messageID}", h.query)
	h.mux.HandleFunc("GET /integration/v1/web-push/readiness", h.readiness)
	h.mux.HandleFunc("GET /integration/v1/web-push-subscriptions", h.listWebPush)
	h.mux.HandleFunc("PUT /integration/v1/web-push-subscriptions/{subscriptionID}", h.upsertWebPush)
	h.mux.HandleFunc("POST /integration/v1/web-push-subscriptions/{subscriptionID}/revoke", h.revokeWebPush)
	h.mux.HandleFunc("POST /integration/v1/web-push-subscriptions/cleanup-expired", h.cleanupWebPush)
	h.registerManagement()
	h.registerOperations()
}
func (h *handler) descriptor(w http.ResponseWriter, r *http.Request) {
	descriptor := h.binding.Descriptor()
	descriptor.Audience = strings.TrimSpace(r.Header.Get("X-Domainry-Runtime-ID"))
	respond(w, descriptor, nil)
}
func (h *handler) eventMappingRequirements(w http.ResponseWriter, r *http.Request) {
	var v struct {
		Items []integrationsdk.EventMappingRequirement `json:"items"`
	}
	if !decode(w, r, &v) {
		return
	}
	if err := h.binding.Requirements().SynchronizeEventMappings(r.Context(), v.Items); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *handler) catalog(w http.ResponseWriter, r *http.Request) {
	v, e := h.binding.Catalog().ListConnectorDefinitions(r.Context())
	respond(w, map[string]any{"items": v}, e)
}
func (h *handler) requirements(w http.ResponseWriter, r *http.Request) {
	var v struct {
		Items []integrationsdk.ConnectionRequirement `json:"items"`
	}
	if !decode(w, r, &v) {
		return
	}
	if err := h.binding.Requirements().SynchronizeConnections(r.Context(), v.Items); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *handler) accept(w http.ResponseWriter, r *http.Request) {
	var v integrationsdk.DeliveryRequest
	if !decode(w, r, &v) {
		return
	}
	o, e := h.binding.Delivery().Accept(r.Context(), v)
	respond(w, o, e)
}
func (h *handler) query(w http.ResponseWriter, r *http.Request) {
	v, e := h.binding.Delivery().Query(r.Context(), r.PathValue("messageID"))
	respond(w, v, e)
}
func (h *handler) readiness(w http.ResponseWriter, r *http.Request) {
	v, e := h.webPush.Readiness(r.Context(), r.URL.Query().Get("workspace_id"))
	respond(w, v, e)
}
func (h *handler) listWebPush(w http.ResponseWriter, r *http.Request) {
	v, e := h.webPush.List(r.Context(), r.URL.Query().Get("workspace_id"), r.URL.Query().Get("user_id"))
	respond(w, map[string]any{"items": v}, e)
}
func (h *handler) upsertWebPush(w http.ResponseWriter, r *http.Request) {
	var v integrationsdk.WebPushSubscriptionInput
	if !decode(w, r, &v) {
		return
	}
	o, e := h.webPush.Upsert(r.Context(), r.URL.Query().Get("workspace_id"), r.URL.Query().Get("user_id"), r.PathValue("subscriptionID"), v)
	respond(w, o, e)
}
func (h *handler) revokeWebPush(w http.ResponseWriter, r *http.Request) {
	v, e := h.webPush.Revoke(r.Context(), r.URL.Query().Get("workspace_id"), r.URL.Query().Get("user_id"), r.PathValue("subscriptionID"))
	respond(w, v, e)
}
func (h *handler) cleanupWebPush(w http.ResponseWriter, r *http.Request) {
	v, e := h.webPush.CleanupExpired(r.Context(), r.URL.Query().Get("workspace_id"))
	respond(w, map[string]int{"cleaned": v}, e)
}
func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		writeJSON(w, 400, map[string]string{"code": "integration.invalid_json"})
		return false
	}
	return true
}
func respond(w http.ResponseWriter, value any, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, value)
}
func writeError(w http.ResponseWriter, err error) {
	writeJSON(w, 500, map[string]string{"code": "integration.internal", "message": err.Error()})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
