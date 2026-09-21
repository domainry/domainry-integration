package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	connectorscatalog "github.com/domainry/domainry-connectors/catalog"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
)

// SyncBuiltinCatalog materializes the Connectors-owned product definitions in
// Integration's query index before registered provider descriptors overlay
// connectors that have an executable provider in this host.
func SyncBuiltinCatalog(ctx context.Context, database modulehost.Database, dialect modulehost.Dialect, definitions []connectorscatalog.ConnectorDefinition) error {
	existing, err := loadBuiltinConnectors(ctx, database, dialect)
	if err != nil {
		return err
	}
	for _, definition := range definitions {
		if err := syncBuiltinConnector(ctx, database, dialect, definition, existing[definition.Key]); err != nil {
			return err
		}
	}
	return nil
}

type persistedBuiltinConnector struct {
	id         string
	payload    string
	schemaHash string
	sourceKind string
}

func loadBuiltinConnectors(ctx context.Context, database modulehost.Database, dialect modulehost.Dialect) (map[string]persistedBuiltinConnector, error) {
	statement, args, err := query.NewSelectBuilder(dialect, "_integration_connector_definitions").Columns("resource_key", "id", "payload_json", "schema_hash", "source_kind").Build()
	if err != nil {
		return nil, err
	}
	rows, err := database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("load Integration connectors: %w", err)
	}
	defer rows.Close()
	existing := map[string]persistedBuiltinConnector{}
	for rows.Next() {
		var key string
		var connector persistedBuiltinConnector
		if err := rows.Scan(&key, &connector.id, &connector.payload, &connector.schemaHash, &connector.sourceKind); err != nil {
			return nil, fmt.Errorf("scan Integration connector: %w", err)
		}
		existing[key] = connector
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Integration connectors: %w", err)
	}
	return existing, nil
}

func syncBuiltinConnector(ctx context.Context, database modulehost.Database, dialect modulehost.Dialect, definition connectorscatalog.ConnectorDefinition, existing persistedBuiltinConnector) error {
	// Provider descriptors are overlaid immediately after this source-owned
	// definition on every startup; the Connector document remains the base.
	now := time.Now().UTC().Format(time.RFC3339Nano)
	hash := sha256.Sum256(definition.Payload)
	hashText := hex.EncodeToString(hash[:])
	if existing.id == "" {
		id := "connector:" + definition.Key
		statement, values, buildErr := query.NewInsertBuilder(dialect, "_integration_connector_definitions").Columns("id", "resource_key", "object_key", "name", "payload_json", "schema_version", "schema_hash", "source_kind", "source_id", "disabled_at", "created_at", "updated_at").Values(id, definition.Key, "", definition.Name, string(definition.Payload), "1", hashText, "connectors", "connectors", nil, now, now).Build()
		if buildErr != nil {
			return buildErr
		}
		if _, execErr := database.ExecContext(ctx, statement, values...); execErr != nil {
			return fmt.Errorf("insert Integration connector %s: %w", definition.Key, execErr)
		}
		return nil
	}
	if builtinConnectorIsCurrent(existing, definition.Payload, hashText) {
		return nil
	}
	statement, values, err := query.NewUpdateBuilder(dialect, "_integration_connector_definitions").Set("name", definition.Name).Set("payload_json", string(definition.Payload)).Set("schema_hash", hashText).Set("source_kind", "connectors").Set("source_id", "connectors").Set("disabled_at", nil).Set("updated_at", now).Where(query.Equal("id", existing.id)).Build()
	if err != nil {
		return err
	}
	if _, err := database.ExecContext(ctx, statement, values...); err != nil {
		return fmt.Errorf("update Integration connector %s: %w", definition.Key, err)
	}
	return nil
}

func builtinConnectorIsCurrent(existing persistedBuiltinConnector, desiredPayload []byte, desiredHash string) bool {
	switch strings.TrimSpace(existing.sourceKind) {
	case "connectors":
		return existing.schemaHash == desiredHash
	case "connectors+provider":
		var existingDocument, desiredDocument map[string]any
		if json.Unmarshal([]byte(existing.payload), &existingDocument) != nil || json.Unmarshal(desiredPayload, &desiredDocument) != nil {
			return false
		}
		// Registered runtime providers own these two projections. All other
		// fields remain the Connectors-owned definition and can be compared
		// without destroying an unchanged provider overlay on every startup.
		delete(existingDocument, "providers")
		delete(existingDocument, "operations")
		delete(desiredDocument, "providers")
		delete(desiredDocument, "operations")
		for _, key := range []string{"type", "source", "lifecycle_status"} {
			if _, definedByConnectors := desiredDocument[key]; !definedByConnectors {
				delete(existingDocument, key)
			}
		}
		return reflect.DeepEqual(existingDocument, desiredDocument)
	default:
		return false
	}
}
