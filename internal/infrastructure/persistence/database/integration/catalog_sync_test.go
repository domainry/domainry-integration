package integration

import (
	"database/sql"
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
