package integration

import (
	"strings"
	"testing"
	"time"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	"github.com/domainry/domainry-orm/query"
)

func TestTerminalEventFailureAssignsStatusOnceForPostgres(t *testing.T) {
	rawDialect, err := ormdialect.New(ormdialect.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	store := &OperationsStore{dialect: rawDialect.WithSchema("public")}
	statement, _, err := store.eventStatusMutation(integrationmodel.Event{AttemptCount: 4}, "failed", "backend.internal", time.UnixMilli(1_800_000_000_000).UTC()).Where(query.AlwaysTrue()).Build()
	if err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(statement, `"status" =`); count != 1 {
		t.Fatalf("PostgreSQL terminal event update assigned status %d times: %s", count, statement)
	}
	if !strings.Contains(statement, `"status" = $1`) {
		t.Fatalf("terminal event status is missing from update: %s", statement)
	}
}
