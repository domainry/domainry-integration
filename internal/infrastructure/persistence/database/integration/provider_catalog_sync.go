package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormbuilder "github.com/domainry/domainry-orm/query"
)

// SyncProviderCatalog projects source-owned Provider descriptors into the
// Integration catalog. Application manifest requirements are deliberately not
// accepted here, so required contracts and available implementations remain
// separate facts.
func SyncProviderCatalog(ctx context.Context, database modulehost.Database, dialect modulehost.Dialect, descriptors []connector.ProviderDescriptor) error {
	grouped := map[string][]connector.ProviderDescriptor{}
	for _, descriptor := range descriptors {
		if err := descriptor.Validate(); err != nil {
			return fmt.Errorf("validate Integration provider descriptor: %w", err)
		}
		grouped[descriptor.ConnectorKey] = append(grouped[descriptor.ConnectorKey], descriptor)
	}
	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := syncProviderConnector(ctx, database, dialect, key, grouped[key]); err != nil {
			return err
		}
	}
	return nil
}

func syncProviderConnector(ctx context.Context, database modulehost.Database, dialect modulehost.Dialect, key string, descriptors []connector.ProviderDescriptor) error {
	sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].ProviderKey < descriptors[j].ProviderKey })
	providers := make([]map[string]any, 0, len(descriptors))
	operations := map[string]map[string]any{}
	revisions := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		operationKeys := make([]string, 0, len(descriptor.Operations))
		for _, operation := range descriptor.Operations {
			operationKeys = append(operationKeys, operation.Key)
			if _, exists := operations[operation.Key]; !exists {
				operations[operation.Key] = map[string]any{"key": operation.Key, "execution_mode": string(operation.Mode), "side_effect": string(operation.Reliability.Effect), "idempotency_supported": operation.Reliability.Idempotency.Strategy != connector.IdempotencyNone}
			}
		}
		sort.Strings(operationKeys)
		providers = append(providers, map[string]any{"key": descriptor.ProviderKey, "provider_revision": descriptor.ProviderRevision, "config_fields": descriptor.ConfigFields, "secret_fields": descriptor.SecretFields, "operation_keys": operationKeys, "startup_activation": descriptor.StartupActivation})
		revisions = append(revisions, descriptor.ProviderKey+":"+descriptor.ProviderRevision)
	}
	operationValues := make([]map[string]any, 0, len(operations))
	operationKeys := make([]string, 0, len(operations))
	for operation := range operations {
		operationKeys = append(operationKeys, operation)
	}
	sort.Strings(operationKeys)
	for _, operation := range operationKeys {
		operationValues = append(operationValues, operations[operation])
	}
	payload, err := json.Marshal(map[string]any{"key": key, "type": "connector", "name": key, "source": "integration-provider", "lifecycle_status": "active", "providers": providers, "operations": operationValues})
	if err != nil {
		return fmt.Errorf("encode Integration connector %s: %w", key, err)
	}
	hash := sha256.Sum256(payload)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	lookup, args, err := ormbuilder.NewSelectBuilder(dialect, "connector_definitions").Columns("id").Where(ormbuilder.Equal("resource_key", key)).Build()
	if err != nil {
		return err
	}
	var id string
	err = database.QueryRowContext(ctx, lookup, args...).Scan(&id)
	if err == sql.ErrNoRows {
		id = "connector:" + key
		insert, insertArgs, buildErr := ormbuilder.NewInsertBuilder(dialect, "connector_definitions").Columns("id", "resource_key", "object_key", "name", "payload_json", "schema_version", "schema_hash", "source_kind", "source_id", "disabled_at", "created_at", "updated_at").Values(id, key, "", key, string(payload), "1", hex.EncodeToString(hash[:]), "provider", strings.Join(revisions, ","), nil, now, now).Build()
		if buildErr != nil {
			return buildErr
		}
		if _, execErr := database.ExecContext(ctx, insert, insertArgs...); execErr != nil {
			return fmt.Errorf("insert Integration connector %s: %w", key, execErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("lookup Integration connector %s: %w", key, err)
	}
	update, updateArgs, err := ormbuilder.NewUpdateBuilder(dialect, "connector_definitions").Set("name", key).Set("payload_json", string(payload)).Set("schema_hash", hex.EncodeToString(hash[:])).Set("source_kind", "provider").Set("source_id", strings.Join(revisions, ",")).Set("disabled_at", nil).Set("updated_at", now).Where(ormbuilder.Equal("id", id)).Build()
	if err != nil {
		return err
	}
	if _, err := database.ExecContext(ctx, update, updateArgs...); err != nil {
		return fmt.Errorf("update Integration connector %s: %w", key, err)
	}
	return nil
}
