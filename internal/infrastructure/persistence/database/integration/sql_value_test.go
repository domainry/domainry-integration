package integration

import (
	"strings"
	"testing"

	ormdialect "github.com/domainry/domainry-orm/dialect"
	"github.com/domainry/domainry-orm/query"
)

func TestSQLIntegerValueRendersTypedCaseResultsForEveryDatabase(t *testing.T) {
	tests := []struct {
		name        ormdialect.Name
		cast        string
		placeholder string
	}{
		{name: ormdialect.SQLite, cast: "INTEGER", placeholder: "?"},
		{name: ormdialect.Postgres, cast: "BIGINT", placeholder: "$2"},
		{name: ormdialect.MySQL, cast: "SIGNED", placeholder: "?"},
	}
	for _, test := range tests {
		t.Run(string(test.name), func(t *testing.T) {
			dialect, err := ormdialect.New(test.name)
			if err != nil {
				t.Fatal(err)
			}
			renderer := dialect.WithSchema("")
			statement, args, err := query.NewSelectBuilder(renderer, "provider_runs").Projections(
				query.Project(query.CaseWhen(query.Equal("status", "ready"), sqlIntegerValue(renderer, 1)).Else(sqlIntegerValue(renderer, 0))),
			).Build()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(statement, "THEN CAST("+test.placeholder+" AS "+test.cast+")") || !strings.Contains(statement, "ELSE CAST(") || !strings.Contains(statement, " AS "+test.cast+") END") {
				t.Fatalf("untyped %s CASE statement: %s", test.name, statement)
			}
			if len(args) != 3 || args[0] != "ready" || args[1] != int64(1) || args[2] != int64(0) {
				t.Fatalf("unexpected %s CASE args: %#v", test.name, args)
			}
		})
	}
}
