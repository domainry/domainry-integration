package integration

import (
	"database/sql"
	"testing"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	"github.com/domainry/domainry-orm/query"
	_ "modernc.org/sqlite"
)

func TestWebPushReadinessUsesIntegrationOwnedConnectionAndSecretState(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-web-push-readiness?mode=memory&cache=shared")
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
	connection, args, err := query.NewInsertBuilder(dialect, "_integration_connections").Columns("id", "connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").Values("push-connection", "push", "workspace-a", "notification", "web_push", "Push", "verified", `{"vapid_public_key":"public-vapid-key"}`, `{"vapid_private_key":"secret:vapid-private"}`, "admin", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z").Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), connection, args...); err != nil {
		t.Fatal(err)
	}
	secret, args, err := query.NewInsertBuilder(dialect, "_integration_secrets").Columns("id", "secret_key", "workspace_id", "kind", "status", "description", "value_ref", "fingerprint", "created_by", "created_at", "updated_at", "disabled_at", "expires_at", "rotated_at", "revoked_at", "last_tested_at", "last_test_status", "last_test_error").Values("secret-1", "vapid-private", "workspace-a", "private_key", "active", nil, nil, nil, "admin", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", nil, "", "", "", "", "", "").Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), secret, args...); err != nil {
		t.Fatal(err)
	}
	readiness, err := NewWebPushSubscriptionStore(database, dialect).Readiness(t.Context(), "workspace-a")
	if err != nil || !readiness.Ready || readiness.PublicKey != "public-vapid-key" || readiness.ConnectionKey != "push" || readiness.Status != "verified" || readiness.Reason != "" {
		t.Fatalf("readiness=%#v err=%v", readiness, err)
	}
	missing, err := NewWebPushSubscriptionStore(database, dialect).Readiness(t.Context(), "workspace-b")
	if err != nil || missing.Ready || missing.Status != "unconfigured" || missing.Reason != "connection_missing" {
		t.Fatalf("missing=%#v err=%v", missing, err)
	}
}

func TestWebPushSubscriptionsAreIntegrationOwnedAndEraseMaterialOnRevoke(t *testing.T) {
	database, err := sql.Open("sqlite", "file:integration-web-push?mode=memory&cache=shared")
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
	store := NewWebPushSubscriptionStore(database, dialect)
	created, err := store.Upsert(t.Context(), "workspace-a", "user-a", "browser-a", integrationmodel.WebPushSubscriptionInput{Endpoint: "https://push.example/subscription", P256DH: "public-key", Auth: "auth-secret"})
	if err != nil || created.Status != "active" || created.EndpointHash == "" {
		t.Fatalf("created=%#v err=%v", created, err)
	}
	listed, err := store.List(t.Context(), "workspace-a", "user-a")
	if err != nil || len(listed) != 1 || listed[0].ID != "browser-a" {
		t.Fatalf("listed=%#v err=%v", listed, err)
	}
	if other, err := store.List(t.Context(), "workspace-a", "user-b"); err != nil || len(other) != 0 {
		t.Fatalf("other=%#v err=%v", other, err)
	}
	revoked, err := store.Revoke(t.Context(), "workspace-a", "user-a", "browser-a")
	if err != nil || revoked.Status != "revoked" {
		t.Fatalf("revoked=%#v err=%v", revoked, err)
	}
	material, found, err := store.material(t.Context(), "workspace-a", "browser-a")
	if err != nil || !found || material.Endpoint != "" || material.P256DH != "" || material.Auth != "" {
		t.Fatalf("material=%#v found=%v err=%v", material, found, err)
	}
}
