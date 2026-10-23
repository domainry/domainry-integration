package integration

import (
	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	"github.com/domainry/domainry-orm/query"
)

type namedSQLDialect interface {
	Name() ormdialect.Name
}

// sqlIntegerValue gives CASE result parameters an explicit database type.
// PostgreSQL cannot infer numeric placeholder types when every CASE result is
// a bound value, while MySQL uses SIGNED rather than BIGINT as a CAST target.
func sqlIntegerValue(dialect modulehost.Dialect, value int64) query.Expression {
	typeName := "DECIMAL(20,0)"
	if named, ok := dialect.(namedSQLDialect); ok {
		switch named.Name() {
		case ormdialect.Postgres:
			typeName = "BIGINT"
		case ormdialect.MySQL:
			typeName = "SIGNED"
		case ormdialect.SQLite:
			typeName = "INTEGER"
		}
	}
	return query.Cast(query.Value(value), typeName)
}
