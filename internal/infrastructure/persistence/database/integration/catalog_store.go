package integration

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

type CatalogStore struct {
	database modulehost.Database
	dialect  modulehost.Dialect
}

func NewCatalogStore(database modulehost.Database, dialect modulehost.Dialect) *CatalogStore {
	return &CatalogStore{database: database, dialect: dialect}
}

func (s *CatalogStore) ListConnectorDefinitions(ctx context.Context) ([]integrationmodel.ConnectorDefinition, error) {
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_integration_connector_definitions").
		Columns("resource_key", "name", "payload_json").
		Where(query.IsNull("disabled_at")).
		OrderBy(query.Ascending("resource_key")).Build()
	if err != nil {
		return nil, fmt.Errorf("build Integration connector catalog query: %w", err)
	}
	rows, err := s.database.QueryContext(ctx, queryValue, args...)
	if err != nil {
		return nil, fmt.Errorf("query Integration connector catalog: %w", err)
	}
	defer rows.Close()
	items := make([]integrationmodel.ConnectorDefinition, 0)
	for rows.Next() {
		var item integrationmodel.ConnectorDefinition
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
