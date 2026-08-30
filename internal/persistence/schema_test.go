package persistence

import "testing"

func TestSchemaMigrationsCoverOwnerTables(t *testing.T) {
	for _, driver := range []string{"sqlite", "postgres", "mysql"} {
		migrations, err := SchemaMigrations(driver, "")
		if err != nil {
			t.Fatalf("%s schema: %v", driver, err)
		}
		if len(migrations) != 2 || len(migrations[0].Statements) != 15 || len(migrations[1].Statements) == 0 {
			t.Fatalf("%s migrations=%d foundation=%d indexes=%d", driver, len(migrations), len(migrations[0].Statements), len(migrations[1].Statements))
		}
	}
}
