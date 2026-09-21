package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	connectorscatalog "github.com/domainry/domainry-connectors/catalog"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	_ "modernc.org/sqlite"
)

func TestProviderOverlayPreservesConnectorsOwnedDefinition(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-catalog-overlay?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	rawDialect, _ := ormdialect.New(ormdialect.SQLite)
	dialect := rawDialect.WithSchema("")
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
	definition := connectorscatalog.ConnectorDefinition{
		Key: "crm", Name: "Customer CRM",
		Payload: json.RawMessage(`{"key":"crm","name":"Customer CRM","description":"Connectors owns this text","classification":"business"}`),
	}
	if err := SyncBuiltinCatalog(t.Context(), database, dialect, []connectorscatalog.ConnectorDefinition{definition}); err != nil {
		t.Fatal(err)
	}
	descriptor := connector.ProviderDescriptor{
		ConnectorKey: "crm", ProviderKey: "probe", ProviderRevision: "1.0.0", StartupActivation: connector.StartupActivationDefaultSafe,
		Operations: []connector.OperationDescriptor{{
			ConnectorKey: "crm", ProviderKey: "probe", Key: "lookup", Mode: connector.ModeCall,
			ContractSHA256: strings.Repeat("d", 64),
			Reliability: connector.ReliabilityContract{
				Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural},
				Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone},
			},
		}},
	}
	if err := SyncProviderCatalog(t.Context(), database, dialect, []connector.ProviderDescriptor{descriptor}); err != nil {
		t.Fatal(err)
	}
	existing, err := loadBuiltinConnectors(t.Context(), database, dialect)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(definition.Payload)
	if !builtinConnectorIsCurrent(existing[definition.Key], definition.Payload, hex.EncodeToString(hash[:])) {
		t.Fatalf("provider overlay was not recognized as current: %#v", existing[definition.Key])
	}
	if err := SyncBuiltinCatalog(t.Context(), database, dialect, []connectorscatalog.ConnectorDefinition{definition}); err != nil {
		t.Fatal(err)
	}
	var payload, sourceKind, sourceID string
	if err := database.QueryRowContext(t.Context(), "SELECT payload_json,source_kind,source_id FROM _integration_connector_definitions WHERE resource_key=?", "crm").Scan(&payload, &sourceKind, &sourceID); err != nil {
		t.Fatal(err)
	}
	var projected map[string]any
	if err := json.Unmarshal([]byte(payload), &projected); err != nil {
		t.Fatal(err)
	}
	if projected["name"] != "Customer CRM" || projected["description"] != "Connectors owns this text" || projected["classification"] != "business" {
		t.Fatalf("Connectors definition was overwritten: %#v", projected)
	}
	if sourceKind != "connectors+provider" || !strings.Contains(sourceID, "probe:1.0.0") {
		t.Fatalf("source_kind=%q source_id=%q", sourceKind, sourceID)
	}
}

func TestBuiltinCatalogLoadsExistingDefinitionsOnceAndSkipsUnchangedWrites(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-catalog-bulk-sync?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	rawDialect, _ := ormdialect.New(ormdialect.SQLite)
	dialect := rawDialect.WithSchema("")
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
	definitions := []connectorscatalog.ConnectorDefinition{
		{Key: "crm", Name: "CRM", Payload: json.RawMessage(`{"key":"crm","name":"CRM"}`)},
		{Key: "mail", Name: "Mail", Payload: json.RawMessage(`{"key":"mail","name":"Mail"}`)},
	}
	counting := &catalogCountingDatabase{DB: database}
	if err := SyncBuiltinCatalog(t.Context(), counting, dialect, definitions); err != nil {
		t.Fatal(err)
	}
	if counting.queryCount != 1 || counting.execCount != 2 {
		t.Fatalf("first sync queries=%d execs=%d, want 1 bulk query and 2 inserts", counting.queryCount, counting.execCount)
	}
	counting.queryCount, counting.execCount = 0, 0
	if err := SyncBuiltinCatalog(t.Context(), counting, dialect, definitions); err != nil {
		t.Fatal(err)
	}
	if counting.queryCount != 1 || counting.execCount != 0 {
		t.Fatalf("unchanged sync queries=%d execs=%d, want 1 bulk query and no writes", counting.queryCount, counting.execCount)
	}
}

type catalogCountingDatabase struct {
	*sql.DB
	queryCount int
	execCount  int
}

func (database *catalogCountingDatabase) QueryContext(ctx context.Context, statement string, args ...any) (*sql.Rows, error) {
	database.queryCount++
	return database.DB.QueryContext(ctx, statement, args...)
}

func (database *catalogCountingDatabase) QueryRowContext(ctx context.Context, statement string, args ...any) *sql.Row {
	database.queryCount++
	return database.DB.QueryRowContext(ctx, statement, args...)
}

func (database *catalogCountingDatabase) ExecContext(ctx context.Context, statement string, args ...any) (sql.Result, error) {
	database.execCount++
	return database.DB.ExecContext(ctx, statement, args...)
}
