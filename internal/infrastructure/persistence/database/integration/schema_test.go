package integration

import (
	"database/sql"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestSchemaMigrationsCoverOwnerTables(t *testing.T) {
	for _, driver := range []string{"sqlite", "postgres", "mysql"} {
		migrations, err := SchemaMigrations(driver, "")
		if err != nil {
			t.Fatalf("%s schema: %v", driver, err)
		}
		if len(migrations) != 1 || len(migrations[0].Statements) != 22 || migrations[0].Baseline != nil {
			t.Fatalf("%s migrations=%#v", driver, migrations)
		}
		for _, migration := range migrations {
			for _, statement := range migration.Statements {
				if strings.Contains(statement, "_integration_subject_erasure_fences") || strings.Contains(statement, "_integration_subject_erasure_receipts") ||
					strings.Contains(statement, "_integration_connector_definitions") || strings.Contains(statement, "_integration_event_mapping_definitions") ||
					strings.Contains(statement, "_integration_connector_provider_states") || strings.Contains(statement, "_integration_connector_provider_commits") ||
					strings.Contains(statement, "_integration_event_mapping_intents") || strings.Contains(statement, "_integration_api_keys") ||
					strings.Contains(statement, "_integration_credential_refresh_leases") || strings.Contains(statement, "_integration_connection_account_secrets") ||
					strings.Contains(statement, "_integration_connection_grants") {
					t.Fatalf("%s still owns a retired private table: %s", driver, statement)
				}
			}
		}
	}
}

func TestFreshSchemaUsesOneTypedCredentialTable(t *testing.T) {
	database, err := sql.Open("sqlite", t.TempDir()+"/credentials.db")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := database.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, table := range []string{"_integration_api_keys", "_integration_credential_refresh_leases"} {
		var retired int
		if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&retired); err != nil || retired != 0 {
			t.Fatalf("retired credential table %s count=%d err=%v", table, retired, err)
		}
	}
	rows, err := database.QueryContext(t.Context(), `PRAGMA table_info('_integration_secrets')`)
	if err != nil {
		t.Fatal(err)
	}
	columns := map[string]bool{}
	for rows.Next() {
		var ordinal, required, primary int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&ordinal, &name, &kind, &required, &defaultValue, &primary); err != nil {
			t.Fatal(err)
		}
		columns[name] = true
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"credential_type", "connection_key", "display_prefix", "lookup_hash", "actor_id", "role_key", "scopes_json", "last_used_at"} {
		if !columns[required] {
			t.Errorf("typed credential column %s is missing", required)
		}
	}
	if columns["ciphertext"] {
		t.Error("credential metadata table contains encrypted material")
	}
	rows, err = database.QueryContext(t.Context(), `PRAGMA table_info('_integration_secret_materials')`)
	if err != nil {
		t.Fatal(err)
	}
	materialColumns := map[string]bool{}
	for rows.Next() {
		var ordinal, required, primary int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&ordinal, &name, &kind, &required, &defaultValue, &primary); err != nil {
			t.Fatal(err)
		}
		materialColumns[name] = true
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !materialColumns["ciphertext"] || materialColumns["actor_id"] || materialColumns["scopes_json"] {
		t.Fatalf("secret material boundary is not minimal: %#v", materialColumns)
	}
}

func TestFreshSchemaFoldsExecutionReceiptIntoEvent(t *testing.T) {
	database, err := sql.Open("sqlite", t.TempDir()+"/events.db")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := database.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	var retired int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='_integration_event_mapping_intents'`).Scan(&retired); err != nil || retired != 0 {
		t.Fatalf("retired mapping intent table count=%d err=%v", retired, err)
	}
	rows, err := database.QueryContext(t.Context(), `PRAGMA table_info('_integration_events')`)
	if err != nil {
		t.Fatal(err)
	}
	columns := map[string]bool{}
	for rows.Next() {
		var ordinal, required, primary int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&ordinal, &name, &kind, &required, &defaultValue, &primary); err != nil {
			t.Fatal(err)
		}
		columns[name] = true
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"mapping_key", "target_type", "execution_json"} {
		if !columns[required] {
			t.Errorf("event execution column %s is missing", required)
		}
	}
}

func TestFreshSchemaOwnsOneTypedProviderRunTable(t *testing.T) {
	database, err := sql.Open("sqlite", t.TempDir()+"/provider-runs.db")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := database.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, retired := range []string{"_integration_connector_provider_states", "_integration_connector_provider_commits"} {
		var count int
		if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, retired).Scan(&count); err != nil || count != 0 {
			t.Fatalf("retired provider table %s count=%d err=%v", retired, count, err)
		}
	}
	rows, err := database.QueryContext(t.Context(), `PRAGMA table_info('_integration_provider_runs')`)
	if err != nil {
		t.Fatal(err)
	}
	columns := map[string]bool{}
	for rows.Next() {
		var ordinal, required, primary int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&ordinal, &name, &kind, &required, &defaultValue, &primary); err != nil {
			t.Fatal(err)
		}
		columns[name] = true
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"run_kind", "run_key", "state_version", "operation_key", "contract_sha256", "status", "due_at", "lease_owner", "lease_expires_at", "fencing_token"} {
		if !columns[required] {
			t.Errorf("typed provider run column %s is missing", required)
		}
	}
}
