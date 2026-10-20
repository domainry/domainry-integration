package module

import (
	"net/http/httptest"
	"testing"
)

type classificationCapture struct {
	*httptest.ResponseRecorder
	kind, code string
}

func (capture *classificationCapture) RecordError(kind, code string) {
	capture.kind, capture.code = kind, code
}

func TestIntegrationErrorClassificationMatrix(t *testing.T) {
	tests := []struct {
		status int
		kind   string
		code   string
	}{
		{503, "unavailable", "backend.integration.database_unavailable"},
		{502, "unavailable", "backend.integration.provider_unavailable"},
		{409, "conflict", "backend.integration.delivery_fence_rejected"},
		{400, "bad_request", "backend.integration.request_invalid"},
		{403, "forbidden", "backend.integration.permission_denied"},
	}
	for _, test := range tests {
		capture := &classificationCapture{ResponseRecorder: httptest.NewRecorder()}
		writeError(capture, test.status, test.code)
		if capture.kind != test.kind || capture.code != test.code {
			t.Fatalf("status=%d kind=%q code=%q", test.status, capture.kind, capture.code)
		}
	}
}
