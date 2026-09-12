package saas

import (
	sdk "github.com/domainry/domainry-integration-sdk"
	"net/http"
)

func (h *handler) authorizeConnectionAccountRead(w http.ResponseWriter, r *http.Request) {
	if h.accountReads == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "integration.account_reads_unavailable"})
		return
	}
	var input sdk.ConnectionAccountReadOperation
	if !decode(w, r, &input) {
		return
	}
	value, err := h.accountReads.AuthorizeConnectionAccountRead(r.Context(), accountSubject(r), r.PathValue("key"), input)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "integration.account_read_failed"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) readConnectionAccount(w http.ResponseWriter, r *http.Request) {
	if h.accountReads == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "integration.account_reads_unavailable"})
		return
	}
	var input sdk.ConnectionAccountReadRequest
	if !decode(w, r, &input) {
		return
	}
	value, err := h.accountReads.ReadConnectionAccount(r.Context(), accountSubject(r), r.PathValue("key"), input)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "integration.account_read_failed"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, value)
}
