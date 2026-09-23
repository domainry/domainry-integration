package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
	metadatamodulehost "github.com/domainry/domainry-metadata-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
)

type subjectExternalFence struct {
	Provider      string `json:"provider"`
	SubjectSHA256 string `json:"subject_sha256"`
}

func eventMappingMatches(mapping model.EventMappingRequirement, event model.Event) bool {
	if !mapping.Enabled || strings.TrimSpace(mapping.Provider) != event.Provider || strings.TrimSpace(mapping.EventType) != "" && strings.TrimSpace(mapping.EventType) != event.EventType {
		return false
	}
	var payload map[string]any
	_ = json.Unmarshal(event.Payload, &payload)
	if prefix := strings.TrimSpace(mapping.CommandPrefix); prefix != "" {
		value, _ := integrationPayloadPath(payload, "command")
		if !strings.HasPrefix(strings.TrimSpace(fmt.Sprint(value)), prefix) {
			return false
		}
	}
	if connection := strings.TrimSpace(mapping.ConnectionKey); connection != "" {
		value, _ := integrationPayloadPath(payload, "_integration_context.connection_key")
		if strings.TrimSpace(fmt.Sprint(value)) != connection {
			return false
		}
	}
	return true
}
func mappedSubject(mapping model.EventMappingRequirement, event model.Event) (string, string) {
	provider := strings.TrimSpace(mapping.ExternalIdentity.Provider)
	if provider == "" {
		provider = event.Provider
	}
	var payload map[string]any
	_ = json.Unmarshal(event.Payload, &payload)
	value, present := integrationPayloadPath(payload, mapping.ExternalIdentity.SubjectPath)
	if mapping.ExternalIdentity.SubjectPath == "" || !present {
		return provider, ""
	}
	return provider, strings.TrimSpace(fmt.Sprint(value))
}
func (s *SubjectLifecycleStore) subjectEvents(ctx context.Context, tx *sql.Tx, r model.SubjectErasureRequest, connections []string) ([]string, []subjectExternalFence, error) {
	owned := map[subjectExternalFence]bool{}
	external := []subjectExternalFence{}
	stmt, args, err := query.NewWorkspaceSelectBuilder(s.dialect, "_integration_external_identities", r.WorkspaceID).Columns("provider", "external_subject").Where(query.Equal("actor_id", r.SubjectID)).Build()
	if err != nil {
		return nil, nil, err
	}
	rows, err := tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var provider, subject string
		if err = rows.Scan(&provider, &subject); err != nil {
			rows.Close()
			return nil, nil, err
		}
		ref := subjectExternalFence{provider, oauthHash(strings.TrimSpace(subject))}
		owned[ref] = true
		external = append(external, ref)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	sort.Slice(external, func(i, j int) bool {
		if external[i].Provider != external[j].Provider {
			return external[i].Provider < external[j].Provider
		}
		return external[i].SubjectSHA256 < external[j].SubjectSHA256
	})
	external = slices.Compact(external)
	mappings, err := listEventMappingDefinitions(metadatamodulehost.WithExecutor(ctx, tx), s.definitions, r.WorkspaceID)
	if err != nil {
		return nil, nil, err
	}
	ids := slices.Clone(r.EventIDs)
	seen := map[string]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	stmt, args, err = query.NewWorkspaceSelectBuilder(s.dialect, "_integration_events", r.WorkspaceID).Columns("id", "provider", "event_type", "payload_json").Build()
	if err != nil {
		return nil, nil, err
	}
	rows, err = tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var event model.Event
		var raw string
		if err = rows.Scan(&event.ID, &event.Provider, &event.EventType, &raw); err != nil {
			return nil, nil, err
		}
		event.Payload = json.RawMessage(raw)
		var payload map[string]any
		if json.Unmarshal(event.Payload, &payload) != nil {
			continue
		}
		connection, _ := integrationPayloadPath(payload, "_integration_context.connection_key")
		covered := slices.Contains(connections, strings.TrimSpace(fmt.Sprint(connection)))
		verifiedSubject, _ := integrationPayloadPath(payload, "_integration_context.verified_external_subject")
		if subject, ok := verifiedSubject.(string); ok && subject != "" {
			covered = covered || owned[subjectExternalFence{event.Provider, oauthHash(strings.TrimSpace(subject))}]
		}
		for _, mapping := range mappings {
			if !eventMappingMatches(mapping, event) {
				continue
			}
			provider, subject := mappedSubject(mapping, event)
			if verified, ok := verifiedSubject.(string); ok && strings.TrimSpace(verified) != "" {
				subject = strings.TrimSpace(verified)
			}
			if subject != "" {
				covered = covered || owned[subjectExternalFence{provider, oauthHash(subject)}]
			}
			object, record := mapping.ObjectKey, mapping.RecordID
			if object == "" && mapping.ObjectKeyPath != "" {
				v, present := integrationPayloadPath(payload, mapping.ObjectKeyPath)
				if present {
					object = strings.TrimSpace(fmt.Sprint(v))
				}
			}
			if record == "" && mapping.RecordIDPath != "" {
				v, present := integrationPayloadPath(payload, mapping.RecordIDPath)
				if present {
					record = strings.TrimSpace(fmt.Sprint(v))
				}
			}
			for _, ref := range r.Resources {
				covered = covered || ref.ObjectKey == object && ref.RecordID == record
			}
			break
		}
		if covered && !seen[event.ID] {
			seen[event.ID] = true
			ids = append(ids, event.ID)
			if len(ids) > 10000 {
				return nil, nil, fmt.Errorf("Integration subject events exceed limit")
			}
		}
	}
	if err = rows.Err(); err != nil {
		return nil, nil, err
	}
	sort.Strings(ids)
	return slices.Compact(ids), external, nil
}
