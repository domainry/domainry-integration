package integrationmodel

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

type SubjectRecordReference struct {
	ObjectKey string `json:"object_key"`
	RecordID  string `json:"record_id"`
}
type SubjectErasureRequest struct {
	WorkspaceID           string                   `json:"workspace_id"`
	SubjectID             string                   `json:"subject_id"`
	RequestID             string                   `json:"request_id,omitempty"`
	Resources             []SubjectRecordReference `json:"resources,omitempty"`
	PublicationMessageIDs []string                 `json:"publication_message_ids,omitempty"`
	EventIDs              []string                 `json:"event_ids,omitempty"`
	LegalHolds            json.RawMessage          `json:"legal_holds,omitempty"`
}

func (r SubjectErasureRequest) Validate(erasure bool) error {
	valid := func(v string, max int) bool {
		return v != "" && len(v) <= max && strings.TrimSpace(v) == v && strings.IndexFunc(v, unicode.IsControl) < 0
	}
	if !valid(r.WorkspaceID, 191) || !valid(r.SubjectID, 255) || erasure && !valid(r.RequestID, 255) {
		return fmt.Errorf("Integration subject scope invalid")
	}
	if len(r.Resources) > 10001 || len(r.PublicationMessageIDs) > 10000 || len(r.EventIDs) > 10000 {
		return fmt.Errorf("Integration subject provenance exceeds limit")
	}
	for _, ref := range r.Resources {
		if !valid(ref.ObjectKey, 255) || !valid(ref.RecordID, 255) {
			return fmt.Errorf("Integration subject resource invalid")
		}
	}
	for _, ids := range [][]string{r.PublicationMessageIDs, r.EventIDs} {
		for _, id := range ids {
			if !valid(id, 255) {
				return fmt.Errorf("Integration subject provenance invalid")
			}
		}
	}
	if erasure && len(r.LegalHolds) > 0 {
		var holds []json.RawMessage
		if json.Unmarshal(r.LegalHolds, &holds) != nil || len(holds) > 0 {
			return fmt.Errorf("Integration erasure blocked by legal hold")
		}
	}
	return nil
}
