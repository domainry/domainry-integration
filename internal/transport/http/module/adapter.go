package module

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationapplication "github.com/domainry/domainry-integration/internal/application/integration"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type adapter struct {
	handler    http.Handler
	routes     []modulehttp.Route
	operations map[string]map[string]any
}

func (*adapter) ContractVersion() string      { return modulehttp.ContractVersion }
func (*adapter) Owner() string                { return "integration" }
func (*adapter) Name() string                 { return "integration_product" }
func (s *adapter) Handler() http.Handler      { return s.handler }
func (s *adapter) Routes() []modulehttp.Route { return append([]modulehttp.Route(nil), s.routes...) }

func NewAdapter(binding integrationsdk.Binding) (modulehttp.Adapter, error) {
	webPushBinding, ok := binding.(integrationsdk.WebPushBinding)
	if !ok || webPushBinding.WebPushSubscriptions() == nil {
		return nil, errors.New("Integration Web Push binding is unavailable")
	}
	managementBinding, ok := binding.(integrationsdk.ManagementBinding)
	if !ok || managementBinding.Management() == nil {
		return nil, errors.New("Integration Management binding is unavailable")
	}
	accountsBinding, ok := binding.(integrationsdk.ConnectionAccountsBinding)
	if !ok || accountsBinding.ConnectionAccounts() == nil {
		return nil, errors.New("Integration connection accounts binding is unavailable")
	}
	accountAdminBinding, ok := binding.(integrationsdk.ConnectionAccountAdministrationBinding)
	if !ok || accountAdminBinding.ConnectionAccountAdministration() == nil {
		return nil, errors.New("Integration connection account administration binding is unavailable")
	}
	operationsBinding, ok := binding.(integrationsdk.OperationsBinding)
	if !ok || operationsBinding.Operations() == nil {
		return nil, errors.New("Integration Operations binding is unavailable")
	}
	h := &handler{catalog: binding.Catalog(), management: managementBinding.Management(), accounts: accountsBinding.ConnectionAccounts(), accountAdmin: accountAdminBinding.ConnectionAccountAdministration(), operations: operationsBinding.Operations(), subscriptions: webPushBinding.WebPushSubscriptions(), mux: http.NewServeMux()}
	if port, ok := binding.(integrationsdk.ConnectionAccountReadsBinding); ok {
		h.accountReads = port.ConnectionAccountReads()
	}
	if port, ok := binding.(integrationsdk.OAuthApplicationsBinding); ok {
		h.oauthApplications = port.OAuthApplications()
	}
	if port, ok := binding.(integrationsdk.OAuthAuthorizationsBinding); ok {
		h.oauthAuthorizations = port.OAuthAuthorizations()
	}
	routes, err := integrationRoutes()
	if err != nil {
		return nil, err
	}
	handlers := h.handlers()
	operations := integrationsdk.IntegrationHTTPAdapterContract().OpenAPI
	for _, route := range routes {
		key := strings.TrimSpace(route.Action.Key)
		implementation, found := handlers[key]
		if !found {
			return nil, fmt.Errorf("Integration Action %q has no HTTP handler", key)
		}
		if _, found := operations[route.Pattern()]; !found {
			return nil, fmt.Errorf("Integration Action %q has no OpenAPI operation", key)
		}
		if key != integrationsdk.ActionIntegrationWebhooksIngest {
			implementation = authorizeAction(key, implementation)
		}
		h.mux.HandleFunc(route.Pattern(), implementation)
		delete(handlers, key)
		delete(operations, route.Pattern())
	}
	if len(handlers) != 0 || len(operations) != 0 {
		keys := make([]string, 0, len(handlers)+len(operations))
		for key := range handlers {
			keys = append(keys, "handler:"+key)
		}
		for pattern := range operations {
			keys = append(keys, "openapi:"+pattern)
		}
		sort.Strings(keys)
		return nil, fmt.Errorf("Integration implementations have no Action manifest entries: %v", keys)
	}
	return &adapter{handler: h.mux, routes: routes, operations: integrationsdk.IntegrationHTTPAdapterContract().OpenAPI}, nil
}

func authorizeAction(permissionKey string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, ok := requestPrincipal(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "auth.principal_required")
			return
		}
		scope, err := integrationapplication.AuthorizeDataAccess(principal, permissionKey)
		if err != nil {
			writeError(w, http.StatusForbidden, "backend.integration.permission_denied")
			return
		}
		if requiresAllDataScope(permissionKey) && (!scope.Unrestricted || scope.DeniedAll || len(scope.DeniedUserIDs) != 0 || len(scope.DeniedOrgIDs) != 0) {
			writeError(w, http.StatusForbidden, "backend.integration.all_data_scope_required")
			return
		}
		next(w, r.WithContext(integrationmodel.WithAccessScope(r.Context(), scope)))
	}
}

func requiresAllDataScope(permissionKey string) bool {
	switch permissionKey {
	case integrationsdk.ActionIntegrationOAuthAuthorizationsOptions,
		integrationsdk.ActionIntegrationOAuthAuthorizationsStart,
		integrationsdk.ActionIntegrationOAuthAuthorizationsGet,
		integrationsdk.ActionIntegrationOAuthAuthorizationsComplete,
		integrationsdk.ActionIntegrationWebPushSubscriptionsList,
		integrationsdk.ActionIntegrationWebPushSubscriptionsUpsert,
		integrationsdk.ActionIntegrationWebPushSubscriptionsRevoke,
		integrationsdk.ActionIntegrationConnectionAccountsList,
		integrationsdk.ActionIntegrationConnectionAccountsGet,
		integrationsdk.ActionIntegrationConnectionAccountsTest,
		integrationsdk.ActionIntegrationConnectionAccountsReadAccess,
		integrationsdk.ActionIntegrationConnectionAccountsRead,
		integrationsdk.ActionIntegrationConnectionAccountsRevoke:
		return false
	default:
		return true
	}
}

func integrationRoutes() ([]modulehttp.Route, error) {
	contract := integrationsdk.IntegrationHTTPAdapterContract()
	routes := make([]modulehttp.Route, 0, len(contract.Routes))
	for _, declared := range contract.Routes {
		route, err := modulehttp.RouteFromAction(declared.Action)
		if err != nil {
			return nil, fmt.Errorf("project Integration Action %q: %w", declared.Action.Key, err)
		}
		routes = append(routes, route)
	}
	return routes, nil
}

// CapabilityRoutes exposes the immutable source-owned route facts used by the
// public capability contract without exporting the executable HTTP adapter.
func CapabilityRoutes() ([]modulehttp.Route, error) {
	return integrationRoutes()
}

type handler struct {
	oauthApplications   integrationsdk.OAuthApplications
	oauthAuthorizations integrationsdk.OAuthAuthorizations
	catalog             integrationsdk.Catalog
	management          integrationsdk.Management
	accounts            integrationsdk.ConnectionAccounts
	accountReads        integrationsdk.ConnectionAccountReads
	accountAdmin        integrationsdk.ConnectionAccountAdministration
	operations          integrationsdk.Operations
	subscriptions       integrationsdk.WebPushSubscriptions
	mux                 *http.ServeMux
}

func (h *handler) handlers() map[string]http.HandlerFunc {
	handlers := h.managementHandlers()
	for key, handler := range h.oauthHandlers() {
		handlers[key] = handler
	}
	handlers[integrationsdk.ActionIntegrationWebPushReadiness] = h.readiness
	handlers[integrationsdk.ActionIntegrationWebPushSubscriptionsList] = h.list
	handlers[integrationsdk.ActionIntegrationWebPushSubscriptionsUpsert] = h.upsert
	handlers[integrationsdk.ActionIntegrationWebPushSubscriptionsRevoke] = h.revoke
	handlers[integrationsdk.ActionIntegrationWebPushSubscriptionsCleanupExpired] = h.cleanup
	handlers[integrationsdk.ActionIntegrationInvocationsList] = h.listInvocations
	handlers[integrationsdk.ActionIntegrationInvocationsGet] = h.getInvocation
	handlers[integrationsdk.ActionIntegrationEventsList] = h.listEvents
	handlers[integrationsdk.ActionIntegrationEventsGet] = h.getEvent
	handlers[integrationsdk.ActionIntegrationEventsReplay] = h.replayEvent
	handlers[integrationsdk.ActionIntegrationWebhooksIngest] = h.webhook
	return handlers
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

var _ modulehttp.Adapter = (*adapter)(nil)
