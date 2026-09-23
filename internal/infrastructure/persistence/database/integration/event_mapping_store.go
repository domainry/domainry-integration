package integration

import (
	"context"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

func (s *RequirementsStore) SynchronizeEventMappings(ctx context.Context, requirements []integrationmodel.EventMappingRequirement) error {
	return SynchronizeEventMappingDefinitions(ctx, s.definitions, requirements)
}
