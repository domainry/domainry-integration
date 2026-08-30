package integration

import (
	"database/sql"
	"encoding/json"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	ormbuilder "github.com/domainry/domainry-orm/query"
	_ "modernc.org/sqlite"
)

func TestRequirementsStorePreservesManagedConnectionState(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-requirements?mode=memory&cache=shared")
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
	provider := &deliveryTestProvider{descriptor: connector.ProviderDescriptor{ConnectorKey: "crm", ProviderKey: "probe", StartupActivation: connector.StartupActivationDefaultSafe}}
	store := NewRequirementsStore(database, dialect, deliveryTestProviders{provider: provider})
	requirement := integrationsdk.ConnectionRequirement{Key: "primary", WorkspaceID: "default", ConnectorKey: "crm", ProviderKey: "probe", Name: "Manifest", Config: json.RawMessage(`{"region":"manifest"}`)}
	if err := store.SynchronizeConnections(t.Context(), []integrationsdk.ConnectionRequirement{requirement}); err != nil {
		t.Fatal(err)
	}
	update, args, err := ormbuilder.NewUpdateBuilder(dialect, "_integration_connections").Set("name", "Managed").Set("status", "inactive").Set("config_json", `{"region":"managed","timeout":30}`).Set("secret_refs_json", `{"token":"secret:managed"}`).Set("created_by", "operator").Where(ormbuilder.Equal("connection_key", "primary")).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), update, args...); err != nil {
		t.Fatal(err)
	}
	if err := store.SynchronizeConnections(t.Context(), []integrationsdk.ConnectionRequirement{requirement}); err != nil {
		t.Fatal(err)
	}
	var name, status, configJSON, secretsJSON, createdBy string
	if err := database.QueryRowContext(t.Context(), "SELECT name,status,config_json,secret_refs_json,created_by FROM _integration_connections WHERE connection_key=?", "primary").Scan(&name, &status, &configJSON, &secretsJSON, &createdBy); err != nil {
		t.Fatal(err)
	}
	if name != "Manifest" || status != "inactive" || secretsJSON != `{"token":"secret:managed"}` || createdBy != "operator" {
		t.Fatalf("managed state overwritten: name=%q status=%q secrets=%s created_by=%q", name, status, secretsJSON, createdBy)
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil || config["region"] != "managed" || config["timeout"] != float64(30) {
		t.Fatalf("config=%s err=%v", configJSON, err)
	}
}
