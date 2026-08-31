package persistence

import (
	"github.com/domainry/domainry-integration-sdk/modulehost"
	databaseschema "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/schema"
)

const SchemaVersion = databaseschema.SchemaVersion

func SchemaMigrations(driver, schema string) ([]modulehost.SchemaMigration, error) {
	return databaseschema.SchemaMigrations(driver, schema)
}
