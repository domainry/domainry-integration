package integration

import "testing"

func TestSchemaMigrationsCoverOwnerTables(t *testing.T) {
	for _, driver := range []string{"sqlite", "postgres", "mysql"} {
		migrations, err := SchemaMigrations(driver, "")
		if err != nil {
			t.Fatalf("%s schema: %v", driver, err)
		}
		if len(migrations) != 3 || len(migrations[0].Statements) != 16 || len(migrations[1].Statements) == 0 || len(migrations[2].Statements) != 1 {
			t.Fatalf("%s migrations=%d foundation=%d indexes=%d", driver, len(migrations), len(migrations[0].Statements), len(migrations[1].Statements))
		}
	}
}
