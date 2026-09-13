package integration

import (
	"context"
	"fmt"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

type subjectFenceReference struct{ kind, object, id string }

func guardSubjectWrite(ctx context.Context, db sqlhost.Queryer, dialect modulehost.Dialect, workspace string, refs ...subjectFenceReference) error {
	ps := []query.Predicate{query.AlwaysFalse()}
	for _, ref := range refs {
		if ref.id != "" {
			ps = append(ps, query.And(query.Equal("kind", ref.kind), query.Equal("object_key", ref.object), query.Equal("resource_id", ref.id)))
		}
	}
	stmt, args, err := query.NewWorkspaceSelectBuilder(dialect, "_integration_subject_erasure_fences", workspace).Columns("id").Where(query.Or(ps...)).Limit(1).Build()
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("integration.subject_erased")
	}
	return rows.Err()
}

// Correlated final predicates prevent a cached worker/manual retry from writing
// a row fenced after its earlier read, including cross-workspace same-ID rows.
func subjectRowWriteAllowed(table string) query.Predicate {
	return query.NotExists("_integration_subject_erasure_fences", query.And(
		query.EqualExpressions(query.Column("workspace_id"), query.QualifiedColumn(table, "workspace_id")),
		query.Equal("kind", "row"), query.Equal("object_key", table),
		query.EqualExpressions(query.Column("resource_id"), query.QualifiedColumn(table, "id")),
	))
}
