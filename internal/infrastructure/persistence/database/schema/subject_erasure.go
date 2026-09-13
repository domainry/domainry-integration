package schema

import (
	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormschema "github.com/domainry/domainry-orm/schema"
)

func subjectErasureStatements(r modulehost.Dialect) ([]string, error) {
	fences, _, err := table(r, "_integration_subject_erasure_fences", key("id"), scope("workspace_id"), req("kind", ormschema.TextKey(32)), key("object_key"), key("resource_id"), key("request_id")).Unique("workspace_id", "kind", "object_key", "resource_id").Build()
	if err != nil {
		return nil, err
	}
	receipts, _, err := table(r, "_integration_subject_erasure_receipts", key("id"), scope("workspace_id"), key("request_id"), key("subject_id"), text("plan_json"), text("result_json")).Unique("workspace_id", "request_id").Build()
	if err != nil {
		return nil, err
	}
	return []string{fences, receipts}, nil
}
