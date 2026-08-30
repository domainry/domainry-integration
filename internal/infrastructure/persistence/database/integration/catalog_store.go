package integration

import (
	"context"
	"encoding/json"
	"fmt"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormbuilder "github.com/domainry/domainry-orm/query"
)

type CatalogStore struct {
	database modulehost.Database
	dialect  modulehost.Dialect
}

func NewCatalogStore(database modulehost.Database, dialect modulehost.Dialect) *CatalogStore {
	return &CatalogStore{database: database, dialect: dialect}
}

func (s *CatalogStore) ListConnectorDefinitions(ctx context.Context) ([]integrationsdk.ConnectorDefinition, error) {
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "_integration_connector_definitions").
		Columns("resource_key", "name", "payload_json").
		Where(ormbuilder.IsNull("disabled_at")).
		OrderBy(ormbuilder.Ascending("resource_key")).Build()
	if err != nil {
		return nil, fmt.Errorf("build Integration connector catalog query: %w", err)
	}
	rows, err := s.database.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query Integration connector catalog: %w", err)
	}
	defer rows.Close()
	items := make([]integrationsdk.ConnectorDefinition, 0)
	for rows.Next() {
		var item integrationsdk.ConnectorDefinition
		var payload string
		if err := rows.Scan(&item.Key, &item.DisplayName, &payload); err != nil {
			return nil, fmt.Errorf("scan Integration connector definition: %w", err)
		}
		item.Definition = json.RawMessage(payload)
		if !json.Valid(item.Definition) {
			return nil, fmt.Errorf("Integration connector %q contains invalid definition JSON", item.Key)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Integration connector catalog: %w", err)
	}
	return items, nil
}

var _ integrationsdk.Catalog = (*CatalogStore)(nil)
