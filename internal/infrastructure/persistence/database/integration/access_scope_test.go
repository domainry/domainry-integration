package integration

import (
	"context"
	"strings"
	"testing"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	"github.com/domainry/domainry-orm/query"
)

func TestScopedWherePushesCanonicalScopeIntoSQL(t *testing.T) {
	rawDialect, err := ormdialect.New(ormdialect.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	dialect := rawDialect.WithSchema("")

	allContext := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.connections.list", ActorID: "user-a", Unrestricted: true,
	})
	allWhere, err := scopedWhere(allContext, "workspace-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	allSQL, allArgs, err := query.NewSelectBuilder(dialect, "_integration_connections").Columns("id").Where(allWhere).Build()
	if err != nil {
		t.Fatal(err)
	}
	if len(allArgs) != 1 || !strings.Contains(allSQL, "workspace_id") || strings.Contains(allSQL, "user_id") || strings.Contains(allSQL, "org_id") {
		t.Fatalf("all SQL=%q args=%#v", allSQL, allArgs)
	}

	ownerContext := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.web_push_subscriptions.list", ActorID: "user-a", AllowedUserIDs: []string{"user-a"},
	})
	ownerWhere, err := scopedWhere(ownerContext, "workspace-a", "user_id", "")
	if err != nil {
		t.Fatal(err)
	}
	ownerSQL, ownerArgs, err := query.NewSelectBuilder(dialect, "_integration_web_push_subscriptions").Columns("id").Where(ownerWhere).Build()
	if err != nil {
		t.Fatal(err)
	}
	if len(ownerArgs) != 2 || !strings.Contains(ownerSQL, "workspace_id") || !strings.Contains(ownerSQL, "user_id") {
		t.Fatalf("owner SQL=%q args=%#v", ownerSQL, ownerArgs)
	}

	adminWhere, err := scopedWhere(ownerContext, "workspace-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	adminSQL, _, err := query.NewSelectBuilder(dialect, "_integration_connections").Columns("id").Where(adminWhere).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(adminSQL, "1 = 0") && !strings.Contains(adminSQL, "FALSE") {
		t.Fatalf("restricted admin SQL must fail closed: %q", adminSQL)
	}

	deniedContext := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.connections.list", ActorID: "admin-a", Unrestricted: true, DeniedUserIDs: []string{"user-b"},
	})
	deniedWhere, err := scopedWhere(deniedContext, "workspace-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	deniedSQL, _, err := query.NewSelectBuilder(dialect, "_integration_connections").Columns("id").Where(deniedWhere).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(deniedSQL, "1 = 0") && !strings.Contains(deniedSQL, "FALSE") {
		t.Fatalf("untranslatable deny must fail closed: %q", deniedSQL)
	}
}

func TestRequireAllDataScopeRejectsRestrictedManagementMutation(t *testing.T) {
	ownerContext := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.connections.upsert", ActorID: "user-a", AllowedUserIDs: []string{"user-a"},
	})
	if err := requireAllDataScope(ownerContext, "workspace-a"); err == nil {
		t.Fatal("expected restricted management mutation to fail")
	}
	allContext := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.connections.upsert", ActorID: "user-a", Unrestricted: true,
	})
	if err := requireAllDataScope(allContext, "workspace-a"); err != nil {
		t.Fatal(err)
	}
}
