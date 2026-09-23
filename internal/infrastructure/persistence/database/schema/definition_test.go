package schema

import (
	"slices"
	"strings"
	"testing"

	"github.com/domainry/domainry-foundation/schemaownership"
)

func TestMySQLConnectionAccountOwnerIndexUsesBoundedColumns(t *testing.T) {
	migrations, err := SchemaMigrations("mysql", "")
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(migrations[0].Statements, "\n")
	for _, want := range []string{"`scope` VARCHAR(32) NOT NULL", "`owner_user_id` VARCHAR(191) NOT NULL", "idx_integration_connection_account_owner"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("MySQL connection-account migration is missing %q: %s", want, joined)
		}
	}
}

func TestSchemaOwnershipMatchesCanonicalFinalDDL(t *testing.T) {
	ownership := SchemaOwnership()
	if err := schemaownership.ValidateAll(ownership); err != nil {
		t.Fatal(err)
	}
	if len(ownership) != 13 || !slices.Equal(OwnedTables(), schemaownership.Names(ownership)) {
		t.Fatalf("Integration schema ownership=%+v", ownership)
	}
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 1 || migrations[0].Baseline != nil || len(migrations[0].Statements) != len(ownership)+len(indexes()) {
		t.Fatalf("Integration final migrations=%+v", migrations)
	}
	for _, table := range ownership {
		var create string
		for _, statement := range migrations[0].Statements {
			if strings.Contains(statement, `CREATE TABLE "`+table.Name+`"`) {
				create = statement
				break
			}
		}
		if create == "" {
			t.Fatalf("Integration table %s has ownership but no canonical DDL", table.Name)
		}
		quoted := make([]string, len(table.PrimaryKey))
		for index, column := range table.PrimaryKey {
			quoted[index] = `"` + column + `"`
		}
		if primaryKey := "PRIMARY KEY (" + strings.Join(quoted, ", ") + ")"; !strings.Contains(create, primaryKey) {
			t.Fatalf("Integration table %s physical primary key does not match %v: %s", table.Name, table.PrimaryKey, create)
		}
	}
	joined := strings.Join(migrations[0].Statements, "\n")
	for _, retired := range []string{"_integration_connection_account_secrets", "_integration_connection_grants", "information_schema.statistics", "PREPARE "} {
		if strings.Contains(joined, retired) {
			t.Fatalf("Integration final schema retained compatibility or retired object %q", retired)
		}
	}
}

func TestSchemaOwnershipReturnsIndependentValues(t *testing.T) {
	first, second := SchemaOwnership(), SchemaOwnership()
	first[0].PrimaryKey[0] = "changed"
	if second[0].PrimaryKey[0] == "changed" {
		t.Fatal("Integration schema ownership shares mutable primary-key storage")
	}
}
