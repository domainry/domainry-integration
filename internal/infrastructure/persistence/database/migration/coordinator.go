package migration

import (
	"context"
	"fmt"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	databaseschema "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/schema"
)

const Owner = databaseschema.MigrationOwner

func Register(ctx context.Context, registrar modulehost.MigrationRegistrar) error {
	if registrar == nil {
		return fmt.Errorf("Integration migration registrar is required")
	}
	migrations, err := databaseschema.SchemaMigrations(registrar.Driver(), registrar.Schema())
	if err != nil {
		return err
	}
	if err := registrar.ApplyOwnedMigrations(ctx, Owner, migrations); err != nil {
		return fmt.Errorf("apply Integration migrations: %w", err)
	}
	return nil
}
