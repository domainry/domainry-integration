package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/domainry/domainry-integration-sdk/modulehost"
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

func TestWebPushOwnerScopeCannotReadOrMutateAnotherUser(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "integration-web-push-scope")
	store := NewWebPushSubscriptionStore(database, dialect)
	input := integrationmodel.WebPushSubscriptionInput{Endpoint: "https://push.example/a", P256DH: "public-key", Auth: "auth-secret"}
	if _, err := store.Upsert(t.Context(), "workspace-a", "user-a", "browser-a", input); err != nil {
		t.Fatal(err)
	}
	input.Endpoint = "https://push.example/b"
	if _, err := store.Upsert(t.Context(), "workspace-a", "user-b", "browser-b", input); err != nil {
		t.Fatal(err)
	}

	ownerContext := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.web_push_subscriptions.list", ActorID: "user-a", AllowedUserIDs: []string{"user-a"},
	})
	values, err := store.List(ownerContext, "workspace-a", "ignored-caller-user")
	if err != nil || len(values) != 1 || values[0].ID != "browser-a" {
		t.Fatalf("owner values=%#v err=%v", values, err)
	}
	if _, err := store.Revoke(ownerContext, "workspace-a", "user-a", "browser-b"); err == nil {
		t.Fatal("owner scope revoked another user's subscription")
	}

	allContext := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.web_push_subscriptions.list", ActorID: "admin-a", Unrestricted: true,
	})
	values, err = store.List(allContext, "workspace-a", "ignored-caller-user")
	if err != nil || len(values) != 2 {
		t.Fatalf("all values=%#v err=%v", values, err)
	}

	orgContext := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.web_push_subscriptions.upsert", ActorID: "user-a", AllowedOrgIDs: []string{"org-a"},
	})
	if _, err := store.Upsert(orgContext, "workspace-a", "user-a", "browser-c", input); err == nil {
		t.Fatal("organization scope must fail closed because subscriptions have no organization ownership column")
	}
}

func TestWebPushCleanupRollsBackWholeCandidateSet(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "integration-web-push-cleanup")
	expired := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	for _, id := range []string{"browser-a", "browser-b"} {
		statement, args, err := query.NewInsertBuilder(dialect, "_integration_web_push_subscriptions").
			Columns("id", "workspace_id", "user_id", "endpoint_hash", "endpoint", "p256dh", "auth_secret", "status", "expires_at", "created_at", "updated_at", "revoked_at").
			Values(id, "workspace-a", "user-a", "hash-"+id, "https://push.example/"+id, "key", "secret", "active", expired, expired, expired, "").Build()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.ExecContext(t.Context(), statement, args...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := database.ExecContext(t.Context(), `CREATE TRIGGER fail_second_cleanup BEFORE UPDATE ON _integration_web_push_subscriptions WHEN OLD.id = 'browser-b' AND NEW.status = 'expired' BEGIN SELECT RAISE(ABORT, 'forced cleanup failure'); END`); err != nil {
		t.Fatal(err)
	}
	allContext := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.web_push_subscriptions.cleanup_expired", ActorID: "admin-a", Unrestricted: true,
	})
	if _, err := NewWebPushSubscriptionStore(database, dialect).CleanupExpired(allContext, "workspace-a"); err == nil {
		t.Fatal("expected cleanup to fail")
	}
	rows, err := database.QueryContext(t.Context(), `SELECT status FROM _integration_web_push_subscriptions ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "active" {
			t.Fatalf("cleanup was partial: status=%q", status)
		}
	}
}

func webPushTestDatabase(t *testing.T, name string) (*sql.DB, modulehost.Dialect) {
	t.Helper()
	database, err := sql.Open("sqlite", "file:"+name+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	rawDialect, err := ormdialect.New(ormdialect.SQLite)
	if err != nil {
		t.Fatal(err)
	}
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
	return database, dialect
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
