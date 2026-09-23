package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	connectorscatalog "github.com/domainry/domainry-connectors/catalog"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
)

const (
	integrationConnectorDefinitionKind    = "integration_connector"
	integrationEventMappingDefinitionKind = "integration_event_mapping"
	integrationConnectorSchemaVersion     = "domainry-integration-connector-v1"
	integrationEventMappingSchemaVersion  = "domainry-integration-event-mapping-v1"
	integrationConnectorSource            = "integration_connector_catalog"
)

// SyncConnectorCatalog publishes the complete Connectors catalog with the
// executable provider overlay as one source-owned shared Definition snapshot.
func SyncConnectorCatalog(ctx context.Context, definitions metadatasdk.DefinitionStore, catalog []connectorscatalog.ConnectorDefinition, descriptors []connector.ProviderDescriptor) error {
	if definitions == nil {
		return fmt.Errorf("Integration shared Definition store is unavailable")
	}
	type connectorDocument struct {
		name    string
		payload map[string]any
	}
	documents := make(map[string]connectorDocument, len(catalog))
	for _, definition := range catalog {
		key := strings.TrimSpace(definition.Key)
		if key == "" || !json.Valid(definition.Payload) {
			return fmt.Errorf("Integration connector catalog contains an invalid definition")
		}
		var payload map[string]any
		if err := json.Unmarshal(definition.Payload, &payload); err != nil {
			return fmt.Errorf("decode Integration connector %s: %w", key, err)
		}
		documents[key] = connectorDocument{name: strings.TrimSpace(definition.Name), payload: payload}
	}
	grouped := map[string][]connector.ProviderDescriptor{}
	for _, descriptor := range descriptors {
		if err := descriptor.Validate(); err != nil {
			return fmt.Errorf("validate Integration provider descriptor: %w", err)
		}
		grouped[descriptor.ConnectorKey] = append(grouped[descriptor.ConnectorKey], descriptor)
	}
	for key, values := range grouped {
		document, found := documents[key]
		if !found {
			document = connectorDocument{name: key, payload: map[string]any{"key": key, "type": "connector", "name": key, "source": "integration-provider", "lifecycle_status": "active"}}
		}
		providers, operations := providerCatalogOverlay(values)
		document.payload["providers"], document.payload["operations"] = providers, operations
		documents[key] = document
	}
	keys := make([]string, 0, len(documents))
	for key := range documents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	snapshot := metadatasdk.ProjectionSnapshot{
		Owner: metadatasdk.DefinitionOwnerIntegration, SchemaVersion: integrationConnectorSchemaVersion,
		SourceKind: integrationConnectorSource, SourceID: integrationConnectorSource, Name: "Integration connector catalog",
		Definitions: make([]metadatasdk.Definition, 0, len(keys)),
	}
	for _, key := range keys {
		document := documents[key]
		payload, err := json.Marshal(document.payload)
		if err != nil {
			return fmt.Errorf("encode Integration connector %s: %w", key, err)
		}
		digest := sha256.Sum256(payload)
		name := strings.TrimSpace(document.name)
		if name == "" {
			name = key
		}
		snapshot.Definitions = append(snapshot.Definitions, metadatasdk.Definition{
			Owner: metadatasdk.DefinitionOwnerIntegration, ResourceType: integrationConnectorDefinitionKind, ResourceKey: key,
			Name: name, Payload: payload, SchemaVersion: integrationConnectorSchemaVersion, SchemaHash: hex.EncodeToString(digest[:]),
			SourceKind: integrationConnectorSource, SourceID: integrationConnectorSource, PublishedBy: "integration_catalog",
		})
	}
	revisionPayload, err := json.Marshal(snapshot.Definitions)
	if err != nil {
		return fmt.Errorf("encode Integration connector catalog revision: %w", err)
	}
	revision := sha256.Sum256(revisionPayload)
	snapshot.SchemaVersion = integrationConnectorSchemaVersion + "-" + hex.EncodeToString(revision[:16])
	for index := range snapshot.Definitions {
		snapshot.Definitions[index].SchemaVersion = snapshot.SchemaVersion
	}
	return definitions.ReplaceSourceSnapshot(ctx, snapshot)
}

func providerCatalogOverlay(descriptors []connector.ProviderDescriptor) ([]map[string]any, []map[string]any) {
	sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].ProviderKey < descriptors[j].ProviderKey })
	providers := make([]map[string]any, 0, len(descriptors))
	operationsByKey := map[string]map[string]any{}
	for _, descriptor := range descriptors {
		operationKeys := make([]string, 0, len(descriptor.Operations))
		for _, operation := range descriptor.Operations {
			operationKeys = append(operationKeys, operation.Key)
			if _, found := operationsByKey[operation.Key]; !found {
				operationsByKey[operation.Key] = map[string]any{
					"key": operation.Key, "execution_mode": string(operation.Mode), "side_effect": string(operation.Reliability.Effect),
					"idempotency_supported": operation.Reliability.Idempotency.Strategy != connector.IdempotencyNone,
				}
			}
		}
		sort.Strings(operationKeys)
		providers = append(providers, map[string]any{
			"key": descriptor.ProviderKey, "provider_revision": descriptor.ProviderRevision, "config_fields": descriptor.ConfigFields,
			"secret_fields": descriptor.SecretFields, "operation_keys": operationKeys, "startup_activation": descriptor.StartupActivation,
		})
	}
	operationKeys := make([]string, 0, len(operationsByKey))
	for key := range operationsByKey {
		operationKeys = append(operationKeys, key)
	}
	sort.Strings(operationKeys)
	operations := make([]map[string]any, 0, len(operationKeys))
	for _, key := range operationKeys {
		operations = append(operations, operationsByKey[key])
	}
	return providers, operations
}

func SynchronizeEventMappingDefinitions(ctx context.Context, definitions metadatasdk.DefinitionStore, requirements []integrationmodel.EventMappingRequirement) error {
	if definitions == nil {
		return fmt.Errorf("Integration shared Definition store is unavailable")
	}
	byWorkspace := map[string][]integrationmodel.EventMappingRequirement{}
	for _, requirement := range requirements {
		if err := requirement.Validate(); err != nil {
			return err
		}
		workspaceID := strings.TrimSpace(requirement.WorkspaceID)
		byWorkspace[workspaceID] = append(byWorkspace[workspaceID], requirement)
	}
	for workspaceID, workspaceRequirements := range byWorkspace {
		sort.Slice(workspaceRequirements, func(i, j int) bool { return workspaceRequirements[i].Key < workspaceRequirements[j].Key })
		snapshot := metadatasdk.ProjectionSnapshot{
			Owner: metadatasdk.DefinitionOwnerIntegration, SchemaVersion: integrationEventMappingSchemaVersion,
			SourceKind: "manifest_requirement", SourceID: workspaceID, Name: "Integration event mappings",
		}
		for _, requirement := range workspaceRequirements {
			if !requirement.Enabled {
				continue
			}
			payload, err := json.Marshal(requirement)
			if err != nil {
				return fmt.Errorf("encode Integration event mapping %q: %w", requirement.Key, err)
			}
			digest := sha256.Sum256(payload)
			snapshot.Definitions = append(snapshot.Definitions, metadatasdk.Definition{
				Owner: metadatasdk.DefinitionOwnerIntegration, ResourceType: integrationEventMappingDefinitionKind,
				ResourceKey: integrationEventMappingDefinitionKey(workspaceID, requirement.Key), ObjectKey: workspaceID, Name: requirement.Key,
				Payload: payload, SchemaVersion: integrationEventMappingSchemaVersion, SchemaHash: hex.EncodeToString(digest[:]),
				SourceKind: "manifest_requirement", SourceID: workspaceID, PublishedBy: "integration_requirements",
			})
		}
		if err := definitions.ReplaceSourceSnapshot(ctx, snapshot); err != nil {
			return fmt.Errorf("replace Integration event mappings for workspace %q: %w", workspaceID, err)
		}
	}
	return nil
}

func integrationEventMappingDefinitionKey(workspaceID, key string) string {
	workspaceHash := sha256.Sum256([]byte(strings.TrimSpace(workspaceID)))
	keyHash := sha256.Sum256([]byte(strings.TrimSpace(key)))
	return "workspace:" + hex.EncodeToString(workspaceHash[:8]) + ":event-mapping:" + hex.EncodeToString(keyHash[:16])
}

func listEventMappingDefinitions(ctx context.Context, definitions metadatasdk.DefinitionStore, workspaceID string) ([]integrationmodel.EventMappingRequirement, error) {
	if definitions == nil {
		return nil, fmt.Errorf("Integration shared Definition store is unavailable")
	}
	values, err := definitions.List(ctx, metadatasdk.DefinitionQuery{
		Owner: metadatasdk.DefinitionOwnerIntegration, ResourceType: integrationEventMappingDefinitionKind, SourceID: strings.TrimSpace(workspaceID),
	})
	if err != nil {
		return nil, err
	}
	result := make([]integrationmodel.EventMappingRequirement, 0, len(values))
	for _, value := range values {
		var mapping integrationmodel.EventMappingRequirement
		if err := json.Unmarshal(value.Payload, &mapping); err != nil {
			return nil, fmt.Errorf("decode Integration event mapping Definition %q: %w", value.ResourceKey, err)
		}
		if mapping.WorkspaceID != strings.TrimSpace(workspaceID) || !mapping.Enabled {
			return nil, fmt.Errorf("Integration event mapping Definition %q identity is invalid", value.ResourceKey)
		}
		result = append(result, mapping)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}
