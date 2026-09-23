package integration

import (
	"context"
	"encoding/json"
	"fmt"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
)

type CatalogStore struct {
	definitions metadatasdk.DefinitionStore
}

func NewCatalogStore(definitions metadatasdk.DefinitionStore) *CatalogStore {
	return &CatalogStore{definitions: definitions}
}

func (s *CatalogStore) ListConnectorDefinitions(ctx context.Context) ([]integrationmodel.ConnectorDefinition, error) {
	if s == nil || s.definitions == nil {
		return nil, fmt.Errorf("Integration shared Definition store is unavailable")
	}
	definitions, err := s.definitions.List(ctx, metadatasdk.DefinitionQuery{Owner: metadatasdk.DefinitionOwnerIntegration, ResourceType: integrationConnectorDefinitionKind, SourceID: integrationConnectorSource})
	if err != nil {
		return nil, fmt.Errorf("query Integration connector catalog Definitions: %w", err)
	}
	items := make([]integrationmodel.ConnectorDefinition, 0, len(definitions))
	for _, definition := range definitions {
		item := integrationmodel.ConnectorDefinition{Key: definition.ResourceKey, DisplayName: definition.Name, Definition: append(json.RawMessage(nil), definition.Payload...)}
		if !json.Valid(item.Definition) {
			return nil, fmt.Errorf("Integration connector %q contains invalid definition JSON", item.Key)
		}
		items = append(items, item)
	}
	return items, nil
}
