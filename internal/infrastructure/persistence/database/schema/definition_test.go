package schema

import (
	"strings"
	"testing"
)

func TestMySQLConnectionAccountOwnerIndexUsesBoundedColumns(t *testing.T) {
	migrations, err := SchemaMigrations("mysql", "")
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(migrations[3].Statements, "\n")
	for _, want := range []string{"`scope` VARCHAR(32) NOT NULL", "`owner_user_id` VARCHAR(191) NOT NULL", "idx_integration_connection_account_owner"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("MySQL connection-account migration is missing %q: %s", want, joined)
		}
	}
}
