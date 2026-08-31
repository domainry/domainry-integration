package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

func (s *RequirementsStore) SynchronizeEventMappings(ctx context.Context, requirements []integrationmodel.EventMappingRequirement) error {
	for _, requirement := range requirements {
		if err := requirement.Validate(); err != nil {
			return err
		}
		if err := s.synchronizeEventMapping(ctx, requirement); err != nil {
			return err
		}
	}
	return nil
}

func (s *RequirementsStore) synchronizeEventMapping(ctx context.Context, requirement integrationmodel.EventMappingRequirement) error {
	resourceKey := strings.TrimSpace(requirement.WorkspaceID) + ":" + strings.TrimSpace(requirement.Key)
	payload, err := json.Marshal(requirement)
	if err != nil {
		return fmt.Errorf("encode Integration event mapping %q: %w", requirement.Key, err)
	}
	digest := sha256.Sum256(payload)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	lookup, args, err := query.NewSelectBuilder(s.dialect, "_integration_event_mapping_definitions").Columns("id").Where(query.Equal("resource_key", resourceKey)).Build()
	if err != nil {
		return err
	}
	var id string
	err = s.database.QueryRowContext(ctx, lookup, args...).Scan(&id)
	disabledAt := any(nil)
	if !requirement.Enabled {
		disabledAt = now
	}
	if err == sql.ErrNoRows {
		id = "event-mapping:" + hex.EncodeToString(digest[:16])
		statement, values, buildErr := query.NewInsertBuilder(s.dialect, "_integration_event_mapping_definitions").Columns("id", "resource_key", "object_key", "name", "payload_json", "schema_version", "schema_hash", "source_kind", "source_id", "disabled_at", "created_at", "updated_at").Values(id, resourceKey, requirement.WorkspaceID, requirement.Key, string(payload), "1", hex.EncodeToString(digest[:]), "manifest_requirement", requirement.Key, disabledAt, now, now).Build()
		if buildErr != nil {
			return buildErr
		}
		if _, execErr := s.database.ExecContext(ctx, statement, values...); execErr != nil {
			return fmt.Errorf("insert Integration event mapping %q: %w", requirement.Key, execErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("lookup Integration event mapping %q: %w", requirement.Key, err)
	}
	statement, values, err := query.NewUpdateBuilder(s.dialect, "_integration_event_mapping_definitions").Set("object_key", requirement.WorkspaceID).Set("name", requirement.Key).Set("payload_json", string(payload)).Set("schema_hash", hex.EncodeToString(digest[:])).Set("source_kind", "manifest_requirement").Set("source_id", requirement.Key).Set("disabled_at", disabledAt).Set("updated_at", now).Where(query.Equal("id", id)).Build()
	if err != nil {
		return err
	}
	if _, err := s.database.ExecContext(ctx, statement, values...); err != nil {
		return fmt.Errorf("update Integration event mapping %q: %w", requirement.Key, err)
	}
	return nil
}
