package schema

import (
	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormschema "github.com/domainry/domainry-orm/schema"
)

func oauthStatements(r modulehost.Dialect) ([]string, error) {
	var result []string
	for _, builder := range []*ormschema.TableBuilder{
		table(r, IntegrationOAuthApplicationsTable, key("id"), scope("workspace_id"), scope("application_key"), text("application_json"), text("client_ciphertext"), key("updated_at")).Unique("workspace_id", "application_key"),
		table(r, IntegrationOAuthSessionsTable, key("id"), scope("workspace_id"), scope("user_id"), scope("application_key"), key("application_revision"), key("state_hash"), key("status"), text("session_json"), text("verifier_ciphertext"), key("expires_at"), key("exchange_deadline"), key("created_at"), key("updated_at")).Unique("state_hash"),
	} {
		statement, _, err := builder.Build()
		if err != nil {
			return nil, err
		}
		result = append(result, statement)
	}
	return result, nil
}
