package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormmigration "github.com/domainry/domainry-orm/migration"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/schema"
	"github.com/domainry/domainry-orm/sqlhost"
)

const LedgerTable = "_schema_migrations"

// ApplyOwnedMigrations gives the standalone Integration service the same
// owner-qualified identity, checksum and dirty-failure semantics used by its
// embedded modules while retaining one physical migration ledger.
func ApplyOwnedMigrations(ctx context.Context, database sqlhost.Database, renderer modulehost.Dialect, owner string, migrations []modulehost.SchemaMigration) error {
	owner = strings.TrimSpace(owner)
	if database == nil || renderer == nil || owner == "" {
		return fmt.Errorf("Integration migration coordinator is incomplete")
	}
	if err := ensureStandaloneLedger(ctx, database, renderer); err != nil {
		return err
	}
	for _, migration := range migrations {
		if err := applyStandaloneMigration(ctx, database, renderer, owner, migration); err != nil {
			return err
		}
	}
	return nil
}

func ensureStandaloneLedger(ctx context.Context, database sqlhost.Database, renderer modulehost.Dialect) error {
	statement, args, err := schema.NewTable(renderer, LedgerTable).IfNotExists().Columns(
		schema.Column("owner", schema.TextKey(191)).NotNull(), schema.Column("version", schema.BigInt()).NotNull(),
		schema.Column("name", schema.TextKey(191)).NotNull(), schema.Column("checksum", schema.TextKey(64)).NotNull(),
		schema.Column("dirty", schema.Boolean()).NotNull(), schema.Column("applied_at", schema.TextKey(40)).NotNull(),
	).PrimaryKey("owner", "version").Build()
	if err != nil {
		return fmt.Errorf("build Integration migration ledger: %w", err)
	}
	if _, err := database.ExecContext(ctx, statement, args...); err != nil {
		return fmt.Errorf("prepare Integration migration ledger: %w", err)
	}
	return nil
}

func applyStandaloneMigration(ctx context.Context, database sqlhost.Database, renderer modulehost.Dialect, owner string, migration modulehost.SchemaMigration) error {
	checksum := ormmigration.Checksum(migration)
	statement, args, err := standaloneLedgerQuery(renderer, owner, migration.Version)
	if err != nil {
		return err
	}
	applied, dirty, found, err := readStandaloneLedger(ctx, database, statement, args)
	if err != nil {
		return fmt.Errorf("inspect %s migration %d: %w", owner, migration.Version, err)
	}
	if found {
		return validateStandaloneLedger(migration, checksum, applied, dirty)
	}
	insert, insertArgs, err := query.NewInsertBuilder(renderer, LedgerTable).
		Columns("owner", "version", "name", "checksum", "dirty", "applied_at").
		Values(owner, migration.Version, strings.TrimSpace(migration.Name), checksum, true, "").Build()
	if err != nil {
		return err
	}
	if _, err := database.ExecContext(ctx, insert, insertArgs...); err != nil {
		if !isStandaloneConflict(err) {
			return fmt.Errorf("record dirty %s migration %d: %w", owner, migration.Version, err)
		}
		return waitForStandalonePeer(ctx, database, migration, checksum, statement, args)
	}
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin %s migration %d: %w", owner, migration.Version, err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, ddl := range migration.Statements {
		if _, err := tx.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("apply %s migration %d: %w", owner, migration.Version, err)
		}
	}
	complete, completeArgs, err := query.NewUpdateBuilder(renderer, LedgerTable).
		Set("dirty", false).Set("applied_at", time.Now().UTC().Format(time.RFC3339Nano)).
		Where(query.And(query.Equal("owner", owner), query.Equal("version", migration.Version), query.Equal("checksum", checksum), query.Equal("dirty", true))).Build()
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, complete, completeArgs...)
	if err != nil {
		return fmt.Errorf("complete %s migration %d ledger: %w", owner, migration.Version, err)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return fmt.Errorf("complete %s migration %d ledger: affected=%d err=%v", owner, migration.Version, affected, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s migration %d: %w", owner, migration.Version, err)
	}
	return nil
}

func standaloneLedgerQuery(renderer modulehost.Dialect, owner string, version uint) (string, []any, error) {
	return query.NewSelectBuilder(renderer, LedgerTable).Columns("checksum", "dirty").Where(query.And(
		query.Equal("owner", owner), query.Equal("version", version),
	)).Build()
}

func readStandaloneLedger(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, statement string, args []any) (string, bool, bool, error) {
	var checksum string
	var dirty bool
	err := queryer.QueryRowContext(ctx, statement, args...).Scan(&checksum, &dirty)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, false, nil
	}
	return checksum, dirty, err == nil, err
}

func validateStandaloneLedger(migration modulehost.SchemaMigration, expected, applied string, dirty bool) error {
	if dirty {
		return &ormmigration.Error{Code: ormmigration.CodeDirty, Version: migration.Version, Name: migration.Name}
	}
	if applied != expected {
		return &ormmigration.Error{Code: ormmigration.CodeChecksumDrift, Version: migration.Version, Name: migration.Name}
	}
	return nil
}

func waitForStandalonePeer(ctx context.Context, database sqlhost.Database, migration modulehost.SchemaMigration, checksum, statement string, args []any) error {
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		applied, dirty, found, err := readStandaloneLedger(ctx, database, statement, args)
		if err != nil {
			return err
		}
		if found && applied != checksum {
			return &ormmigration.Error{Code: ormmigration.CodeChecksumDrift, Version: migration.Version, Name: migration.Name}
		}
		if found && !dirty {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return &ormmigration.Error{Code: ormmigration.CodeWaitTimeout, Version: migration.Version, Name: migration.Name}
		case <-ticker.C:
		}
	}
}

func isStandaloneConflict(err error) bool {
	if err == nil {
		return false
	}
	value := strings.ToLower(err.Error())
	return strings.Contains(value, "unique") || strings.Contains(value, "duplicate") || strings.Contains(value, "constraint")
}
