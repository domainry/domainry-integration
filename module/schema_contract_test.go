package module

import (
	"slices"
	"testing"

	"github.com/domainry/domainry-foundation/schemaownership"
)

func TestPublicSchemaContractMatchesIntegrationInventory(t *testing.T) {
	tables := SchemaOwnership()
	if err := schemaownership.ValidateAll(tables); err != nil {
		t.Fatal(err)
	}
	if len(tables) != 13 || !slices.Equal(OwnedTables(), schemaownership.Names(tables)) {
		t.Fatalf("Integration public schema inventory=%+v", tables)
	}
	if MigrationOwner != "integration" {
		t.Fatalf("Integration migration owner=%q", MigrationOwner)
	}
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 1 || migrations[0].Baseline != nil {
		t.Fatalf("Integration public migrations=%+v", migrations)
	}

	tables[0].PrimaryKey[0] = "changed"
	if SchemaOwnership()[0].PrimaryKey[0] == "changed" {
		t.Fatal("Integration public schema contract shares mutable primary-key storage")
	}
}
