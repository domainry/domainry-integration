package saas

import (
	"encoding/json"
	"github.com/domainry/domainry-foundation/requestcontext"
	sdk "github.com/domainry/domainry-integration-sdk"
	"net/http"
)

// These routes are inside ServeHTTP's service-token/runtime authentication;
// the public product adapter deliberately never registers them.
func (h *handler) registerSubjectLifecycle() {
	port, ok := h.binding.(sdk.SubjectLifecycleBinding)
	if !ok || port.SubjectLifecycle() == nil {
		return
	}
	for _, operation := range []string{"preview", "export", "prepare", "erase"} {
		h.mux.HandleFunc("POST /integration/v1/subjects/"+operation, func(w http.ResponseWriter, r *http.Request) {
			var value struct {
				Request sdk.SubjectErasureRequest `json:"request"`
				Plan    json.RawMessage           `json:"plan,omitempty"`
			}
			if !decode(w, r, &value) {
				return
			}
			if err := value.Request.Validate(operation == "prepare" || operation == "erase"); err != nil {
				writeError(w, err)
				return
			}
			ctx := requestcontext.WithWorkspaceID(r.Context(), value.Request.WorkspaceID)
			var out json.RawMessage
			var err error
			switch operation {
			case "preview":
				out, err = port.SubjectLifecycle().PreviewSubject(ctx, value.Request)
			case "export":
				out, err = port.SubjectLifecycle().ExportSubject(ctx, value.Request)
			case "prepare":
				out, err = port.SubjectLifecycle().PrepareSubjectErasure(ctx, value.Request)
			case "erase":
				out, err = port.SubjectLifecycle().ErasePreparedSubject(ctx, value.Request, value.Plan)
			}
			if err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, out)
		})
	}
}
