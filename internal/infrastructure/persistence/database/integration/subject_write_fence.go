package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

const (
	sharedSubjectRequestsTable       = "_subject_requests"
	sharedSubjectExecutionStepsTable = "_subject_steps"
	lifecycleSubjectOwner            = "lifecycle"
	subjectEraseFenceOperation       = "erase_fence"
	integrationSubjectOwner          = "integration"
	subjectErasePlanOperation        = "erase_plan"
	subjectEraseOperation            = "erase"
)

func sharedSubjectErasureRequestIDs(dialect modulehost.Dialect, workspace, subject string) *query.SelectBuilder {
	return query.NewWorkspaceSelectBuilder(dialect, sharedSubjectRequestsTable, workspace).Columns("id").Where(query.And(
		query.NotEqual("request_type", "external_erasure"),
		query.Equal("kind", "erase"),
		query.Equal("resolved_identity", subject),
	))
}

func sharedSubjectFenceRequests(dialect modulehost.Dialect, workspace, subject, request string) *query.SelectBuilder {
	predicates := []query.Predicate{
		query.Equal("owner", lifecycleSubjectOwner),
		query.Equal("operation", subjectEraseFenceOperation),
		query.InSubquery("request_id", sharedSubjectErasureRequestIDs(dialect, workspace, subject)),
	}
	if request != "" {
		predicates = append(predicates, query.Equal("request_id", request))
	}
	return query.NewWorkspaceSelectBuilder(dialect, sharedSubjectExecutionStepsTable, workspace).
		Columns("request_id").Where(query.And(predicates...))
}

type subjectFenceReference struct {
	Kind   string `json:"kind"`
	Object string `json:"object"`
	ID     string `json:"id"`
}

func subjectPlanReferencePattern(ref subjectFenceReference) string {
	raw, _ := json.Marshal(ref)
	escaped := strings.NewReplacer("~", "~~", "%", "~%", "_", "~_").Replace(string(raw))
	return "%" + escaped + "%"
}

func guardSubjectWrite(ctx context.Context, db sqlhost.Queryer, dialect modulehost.Dialect, persistence *SubjectLifecyclePersistence, workspace string, refs ...subjectFenceReference) error {
	if !persistence.Bound() {
		return nil
	}
	plans := []query.Predicate{query.AlwaysFalse()}
	checks := []*query.SelectBuilder{}
	for _, ref := range refs {
		if strings.TrimSpace(ref.ID) == "" {
			continue
		}
		if ref.Kind == "subject" {
			checks = append(checks, sharedSubjectFenceRequests(dialect, workspace, ref.ID, "").Limit(1))
		} else {
			plans = append(plans, query.LikeEscaped("payload_json", subjectPlanReferencePattern(ref)))
		}
	}
	checks = append(checks,
		query.NewWorkspaceSelectBuilder(dialect, sharedSubjectExecutionStepsTable, workspace).Columns("request_id").Where(query.And(
			query.Equal("owner", integrationSubjectOwner),
			query.Equal("operation", subjectErasePlanOperation),
			query.Or(plans...),
		)).Limit(1),
	)
	for _, check := range checks {
		stmt, args, err := check.Build()
		if err != nil {
			return err
		}
		rows, err := db.QueryContext(ctx, stmt, args...)
		if err != nil {
			return err
		}
		blocked := rows.Next()
		rowsErr := rows.Err()
		_ = rows.Close()
		if rowsErr != nil {
			return rowsErr
		}
		if blocked {
			return fmt.Errorf("integration.subject_erased")
		}
	}
	return nil
}

// subjectRowsWriteAllowed keeps the shared frozen plan in the final UPDATE
// predicate. This prevents a cached worker/manual retry from winning after the
// plan was committed, without restoring a module-owned row-fence table.
func subjectRowsWriteAllowed(persistence *SubjectLifecyclePersistence, dialect modulehost.Dialect, workspace, table string, ids ...string) query.Predicate {
	if !persistence.Bound() {
		return query.AlwaysTrue()
	}
	blocked := []query.Predicate{query.AlwaysFalse()}
	for _, id := range ids {
		if id = strings.TrimSpace(id); id == "" {
			continue
		}
		ref := subjectFenceReference{Kind: "row", Object: table, ID: id}
		blocked = append(blocked, query.ExistsSubquery(
			query.NewWorkspaceSelectBuilder(dialect, sharedSubjectExecutionStepsTable, workspace).
				Columns("request_id").Where(query.And(
				query.Equal("owner", integrationSubjectOwner),
				query.Equal("operation", subjectErasePlanOperation),
				query.LikeEscaped("payload_json", subjectPlanReferencePattern(ref)),
			)),
		))
	}
	return query.Not(query.Or(blocked...))
}
