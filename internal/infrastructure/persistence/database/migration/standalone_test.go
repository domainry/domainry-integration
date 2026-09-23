package migration

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	ormmigration "github.com/domainry/domainry-orm/migration"
	_ "modernc.org/sqlite"
)

func TestStandaloneOwnersShareOneLedgerWithoutVersionCollision(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-owner-ledger?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	dialect, _ := ormdialect.New(ormdialect.SQLite)
	renderer := dialect.WithSchema("")
	for _, item := range []struct {
		owner, table string
	}{
		{"integration", "integration_probe"},
		{"metadata", "metadata_probe"},
	} {
		migration := modulehost.SchemaMigration{Version: 1, Name: "foundation", Statements: []string{"CREATE TABLE " + item.table + " (id TEXT PRIMARY KEY)"}}
		if err := ApplyOwnedMigrations(t.Context(), database, renderer, item.owner, []modulehost.SchemaMigration{migration}); err != nil {
			t.Fatal(err)
		}
	}
	var ledgers, rows, dirty int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE '%schema_migrations%'`).Scan(&ledgers); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*), COALESCE(SUM(CASE WHEN dirty THEN 1 ELSE 0 END), 0) FROM _schema_migrations`).Scan(&rows, &dirty); err != nil {
		t.Fatal(err)
	}
	if ledgers != 1 || rows != 2 || dirty != 0 {
		t.Fatalf("ledgers=%d rows=%d dirty=%d", ledgers, rows, dirty)
	}
}

func TestStandaloneLedgerRejectsChecksumDriftAndDirtyRestart(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-ledger-failure?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	dialect, _ := ormdialect.New(ormdialect.SQLite)
	renderer := dialect.WithSchema("")
	base := modulehost.SchemaMigration{Version: 1, Name: "foundation", Statements: []string{"CREATE TABLE stable_probe (id TEXT PRIMARY KEY)"}}
	if err := ApplyOwnedMigrations(t.Context(), database, renderer, "integration", []modulehost.SchemaMigration{base}); err != nil {
		t.Fatal(err)
	}
	drift := base
	drift.Statements = []string{"CREATE TABLE changed_probe (id TEXT PRIMARY KEY)"}
	var migrationErr *ormmigration.Error
	if err := ApplyOwnedMigrations(t.Context(), database, renderer, "integration", []modulehost.SchemaMigration{drift}); !errors.As(err, &migrationErr) || migrationErr.Code != ormmigration.CodeChecksumDrift {
		t.Fatalf("checksum drift error=%v", err)
	}
	broken := modulehost.SchemaMigration{Version: 2, Name: "broken", Statements: []string{"CREATE TABLE"}}
	if err := ApplyOwnedMigrations(t.Context(), database, renderer, "integration", []modulehost.SchemaMigration{broken}); err == nil {
		t.Fatal("broken migration unexpectedly succeeded")
	}
	migrationErr = nil
	if err := ApplyOwnedMigrations(t.Context(), database, renderer, "integration", []modulehost.SchemaMigration{{Version: 2, Name: "broken", Statements: []string{"CREATE TABLE repaired_probe (id TEXT PRIMARY KEY)"}}}); !errors.As(err, &migrationErr) || migrationErr.Code != ormmigration.CodeDirty {
		t.Fatalf("dirty restart error=%v", err)
	}
}
