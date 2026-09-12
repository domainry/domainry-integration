package saas

import (
	"encoding/json"
	"io"
	"net/http"

	sdk "github.com/domainry/domainry-integration-sdk"
)

// This is the authenticated host-to-owner service surface. The public product
// Module HTTP adapter deliberately has no equivalent mutation route: the host
// execution/confirmation flow consumes the Go SDK (or this service binding).
func (h *handler) registerAccountWrites() {
	h.mux.HandleFunc("POST /integration/v1/connection-accounts/{key}/write-access", h.authorizeConnectionAccountWrite)
	h.mux.HandleFunc("POST /integration/v1/connection-accounts/{key}/write", h.writeConnectionAccount)
	h.mux.HandleFunc("POST /integration/v1/connection-accounts/{key}/write-receipt", h.readConnectionAccountWriteReceipt)
}

func decodeAccountWrite(w http.ResponseWriter, r *http.Request, value any) bool {
	w.Header().Set("Cache-Control", "no-store")
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	d.DisallowUnknownFields()
	if d.Decode(value) != nil || d.Decode(new(any)) != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "integration.invalid_json"})
		return false
	}
	return true
}

func (h *handler) authorizeConnectionAccountWrite(w http.ResponseWriter, r *http.Request) {
	if h.accountWrites == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "integration.account_writes_unavailable"})
		return
	}
	var input sdk.ConnectionAccountWriteOperation
	if !decodeAccountWrite(w, r, &input) {
		return
	}
	value, err := h.accountWrites.AuthorizeConnectionAccountWrite(r.Context(), accountSubject(r), r.PathValue("key"), input)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "integration.account_write_denied"})
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *handler) writeConnectionAccount(w http.ResponseWriter, r *http.Request) {
	h.accountWrite(w, r, false)
}
func (h *handler) readConnectionAccountWriteReceipt(w http.ResponseWriter, r *http.Request) {
	h.accountWrite(w, r, true)
}

func (h *handler) accountWrite(w http.ResponseWriter, r *http.Request, receiptOnly bool) {
	if h.accountWrites == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "integration.account_writes_unavailable"})
		return
	}
	var input sdk.ConnectionAccountWriteRequest
	if !decodeAccountWrite(w, r, &input) {
		return
	}
	var value sdk.ConnectionAccountWriteResult
	var err error
	if receiptOnly {
		value, err = h.accountWrites.ReadConnectionAccountWriteReceipt(r.Context(), accountSubject(r), r.PathValue("key"), input)
	} else {
		value, err = h.accountWrites.WriteConnectionAccount(r.Context(), accountSubject(r), r.PathValue("key"), input)
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "integration.account_write_failed"})
		return
	}
	writeJSON(w, http.StatusOK, value)
}
