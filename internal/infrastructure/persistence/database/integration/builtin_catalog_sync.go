package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	connectorscatalog "github.com/domainry/domainry-connectors/catalog"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
)

// SyncBuiltinCatalog materializes the Connectors-owned product definitions in
// Integration's query index before registered provider descriptors overlay
// connectors that have an executable provider in this host.
func SyncBuiltinCatalog(ctx context.Context, database modulehost.Database, dialect modulehost.Dialect, definitions []connectorscatalog.ConnectorDefinition) error {
	for _, definition := range definitions {
		if err := syncBuiltinConnector(ctx, database, dialect, definition); err != nil {
			return err
		}
	}
	return nil
}

func syncBuiltinConnector(ctx context.Context, database modulehost.Database, dialect modulehost.Dialect, definition connectorscatalog.ConnectorDefinition) error {
	lookup, args, err := query.NewSelectBuilder(dialect, "_integration_connector_definitions").Columns("id").Where(query.Equal("resource_key", definition.Key)).Build()
	if err != nil {
		return err
	}
	var id string
	err = database.QueryRowContext(ctx, lookup, args...).Scan(&id)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("lookup Integration connector %s: %w", definition.Key, err)
	}
	// Provider descriptors are overlaid immediately after this source-owned
	// definition on every startup; the Connector document remains the base.
	now := time.Now().UTC().Format(time.RFC3339Nano)
	hash := sha256.Sum256(definition.Payload)
	hashText := hex.EncodeToString(hash[:])
	if err == sql.ErrNoRows {
		id = "connector:" + definition.Key
		statement, values, buildErr := query.NewInsertBuilder(dialect, "_integration_connector_definitions").Columns("id", "resource_key", "object_key", "name", "payload_json", "schema_version", "schema_hash", "source_kind", "source_id", "disabled_at", "created_at", "updated_at").Values(id, definition.Key, "", definition.Name, string(definition.Payload), "1", hashText, "connectors", "connectors", nil, now, now).Build()
		if buildErr != nil {
			return buildErr
		}
		if _, execErr := database.ExecContext(ctx, statement, values...); execErr != nil {
			return fmt.Errorf("insert Integration connector %s: %w", definition.Key, execErr)
		}
		return nil
	}
	statement, values, err := query.NewUpdateBuilder(dialect, "_integration_connector_definitions").Set("name", definition.Name).Set("payload_json", string(definition.Payload)).Set("schema_hash", hashText).Set("source_kind", "connectors").Set("source_id", "connectors").Set("disabled_at", nil).Set("updated_at", now).Where(query.Equal("id", id)).Build()
	if err != nil {
		return err
	}
	if _, err := database.ExecContext(ctx, statement, values...); err != nil {
		return fmt.Errorf("update Integration connector %s: %w", definition.Key, err)
	}
	return nil
}
