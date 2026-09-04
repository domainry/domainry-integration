package saas

import (
	"io"
	"net/http"
	"strconv"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

func (h *handler) registerOperations() {
	h.mux.HandleFunc("POST /integration/v1/operations/call", h.callProvider)
	h.mux.HandleFunc("GET /integration/v1/operations/invocations", h.listInvocations)
	h.mux.HandleFunc("GET /integration/v1/operations/invocations/{id}", h.getInvocation)
	h.mux.HandleFunc("POST /integration/v1/inbound/webhooks", h.acceptWebhook)
	h.mux.HandleFunc("POST /integration/v1/public/webhooks/{workspaceID}/{connectorKey}/{connectionKey}", h.acceptPublicWebhook)
	h.mux.HandleFunc("GET /integration/v1/inbound/events", h.listEvents)
	h.mux.HandleFunc("GET /integration/v1/inbound/events/{id}", h.getEvent)
	h.mux.HandleFunc("POST /integration/v1/inbound/events/{id}/replay", h.replayEvent)
}

func (h *handler) callProvider(w http.ResponseWriter, r *http.Request) {
	var request integrationsdk.ProviderCallRequest
	if !decode(w, r, &request) {
		return
	}
	value, err := h.operations.Call(r.Context(), request)
	respond(w, value, err)
}

func (h *handler) listInvocations(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	values, err := h.operations.ListInvocations(r.Context(), integrationsdk.InvocationQuery{WorkspaceID: r.URL.Query().Get("workspace_id"), ConnectorKey: r.URL.Query().Get("connector_key"), ConnectionKey: r.URL.Query().Get("connection_key"), Operation: r.URL.Query().Get("operation"), Status: r.URL.Query().Get("status"), CreatedFrom: r.URL.Query().Get("created_from"), Limit: limit})
	respond(w, map[string]any{"items": values}, err)
}

func (h *handler) getInvocation(w http.ResponseWriter, r *http.Request) {
	value, err := h.operations.GetInvocation(r.Context(), r.URL.Query().Get("workspace_id"), r.PathValue("id"))
	respond(w, value, err)
}

func (h *handler) acceptWebhook(w http.ResponseWriter, r *http.Request) {
	var request integrationsdk.WebhookRequest
	if !decode(w, r, &request) {
		return
	}
	value, err := h.operations.AcceptWebhook(r.Context(), request)
	respond(w, value, err)
}

func (h *handler) acceptPublicWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "integration.invalid_body"})
		return
	}
	value, err := h.operations.AcceptWebhook(r.Context(), integrationsdk.WebhookRequest{WorkspaceID: r.PathValue("workspaceID"), ConnectorKey: r.PathValue("connectorKey"), ConnectionKey: r.PathValue("connectionKey"), Headers: r.Header.Clone(), Query: r.URL.Query(), Body: body, ReceivedAt: time.Now().UTC()})
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "integration.webhook_rejected"})
		return
	}
	if value.Challenge != "" && value.Format == "plain" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(value.Challenge))
		return
	}
	writeJSON(w, http.StatusAccepted, value)
}

func (h *handler) listEvents(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	values, err := h.operations.ListEvents(r.Context(), integrationsdk.EventQuery{WorkspaceID: r.URL.Query().Get("workspace_id"), Provider: r.URL.Query().Get("provider"), EventType: r.URL.Query().Get("event_type"), Status: r.URL.Query().Get("status"), Limit: limit})
	respond(w, map[string]any{"items": values}, err)
}

func (h *handler) getEvent(w http.ResponseWriter, r *http.Request) {
	value, err := h.operations.GetEvent(r.Context(), r.URL.Query().Get("workspace_id"), r.PathValue("id"))
	respond(w, value, err)
}

func (h *handler) replayEvent(w http.ResponseWriter, r *http.Request) {
	value, err := h.operations.ReplayEvent(r.Context(), r.URL.Query().Get("workspace_id"), r.PathValue("id"))
	respond(w, value, err)
}
