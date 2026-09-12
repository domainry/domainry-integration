package module

import (
	sdk "github.com/domainry/domainry-integration-sdk"
	"net/http"
	"strings"
)

func (h *handler) authorizeConnectionAccountRead(w http.ResponseWriter, r *http.Request) {
	subject, ok := connectionAccountSubject(w, r)
	if !ok {
		return
	}
	if h.accountReads == nil {
		writeError(w, http.StatusServiceUnavailable, "backend.integration.account_reads_unavailable")
		return
	}
	var input sdk.ConnectionAccountReadOperation
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.accountReads.AuthorizeConnectionAccountRead(r.Context(), subject, strings.TrimSpace(r.PathValue("connectionKey")), input)
	if err != nil {
		writeConnectionAccountError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) readConnectionAccount(w http.ResponseWriter, r *http.Request) {
	subject, ok := connectionAccountSubject(w, r)
	if !ok {
		return
	}
	if h.accountReads == nil {
		writeError(w, http.StatusServiceUnavailable, "backend.integration.account_reads_unavailable")
		return
	}
	var input sdk.ConnectionAccountReadRequest
	if !decodeJSON(w, r, &input) {
		return
	}
	value, err := h.accountReads.ReadConnectionAccount(r.Context(), subject, strings.TrimSpace(r.PathValue("connectionKey")), input)
	if err != nil {
		writeConnectionAccountError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, value)
}
