package schema

import (
	"fmt"
	ormschema "github.com/domainry/domainry-orm/schema"
	"strings"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormdialect "github.com/domainry/domainry-orm/dialect"
)

const SchemaVersion uint = 5

func SchemaMigrations(driver, schema string) ([]modulehost.SchemaMigration, error) {
	parsed, err := ormdialect.Parse(driver)
	if err != nil {
		return nil, fmt.Errorf("Integration database driver %q is unsupported: %w", driver, err)
	}
	dialect, err := ormdialect.New(parsed.Name())
	if err != nil {
		return nil, err
	}
	renderer := dialect.WithSchema(schema)
	builders := []*ormschema.TableBuilder{
		connectionsTable(renderer), providerRunsTable(renderer),
		secretMaterialsTable(renderer), secretsTable(renderer), externalIdentitiesTable(renderer),
		invocationsTable(renderer), eventsTable(renderer),
		webhookNoncesTable(renderer), webhookSubscriptionsTable(renderer),
		webPushSubscriptionsTable(renderer),
	}
	statements := make([]string, 0, len(builders))
	for _, builder := range builders {
		statement, _, err := builder.Build()
		if err != nil {
			return nil, fmt.Errorf("build Integration schema: %w", err)
		}
		statements = append(statements, statement)
	}
	indexes, err := ownerIndexStatements(parsed.Name(), renderer)
	if err != nil {
		return nil, err
	}
	connectionAccounts, _, err := connectionAccountsTable(renderer).Build()
	if err != nil {
		return nil, fmt.Errorf("build Integration connection account migration: %w", err)
	}
	connectionAccountSecrets, _, err := connectionAccountSecretsTable(renderer).Build()
	if err != nil {
		return nil, fmt.Errorf("build Integration connection account secret migration: %w", err)
	}
	connectionAccountIndex, _, err := ormschema.NewIndex(renderer, "idx_integration_connection_account_owner", "_integration_connection_accounts").Columns("workspace_id", "scope", "owner_user_id", "connection_key").Build()
	if err != nil {
		return nil, fmt.Errorf("build Integration connection account index: %w", err)
	}
	connectionAccountSecretIndex, _, err := ormschema.NewIndex(renderer, "idx_integration_connection_account_secret", "_integration_connection_account_secrets").Columns("workspace_id", "connection_key", "secret_key").Build()
	if err != nil {
		return nil, fmt.Errorf("build Integration connection account secret index: %w", err)
	}
	oauth, err := oauthStatements(renderer)
	if err != nil {
		return nil, err
	}
	grant, _, err := table(renderer, "_integration_connection_grants", key("id"), scope("workspace_id"), scope("connection_key"), text("scopes_json")).Unique("workspace_id", "connection_key").Build()
	if err != nil {
		return nil, err
	}
	return []modulehost.SchemaMigration{
		{Version: 1, Name: "integration_foundation", Statements: statements},
		{Version: 2, Name: "integration_owner_indexes", Statements: indexes},
		{Version: 3, Name: "integration_connection_accounts", Statements: []string{connectionAccounts, connectionAccountSecrets, connectionAccountIndex, connectionAccountSecretIndex}},
		{Version: 4, Name: "integration_oauth_authorization", Statements: oauth},
		{Version: SchemaVersion, Name: "integration_connection_grants", Statements: []string{grant}},
	}, nil
}

func ownerIndexStatements(driver ormdialect.Name, renderer modulehost.Dialect) ([]string, error) {
	specs := []struct {
		name, table string
		columns     []string
	}{
		{"idx_integration_events_status", "_integration_events", []string{"workspace_id", "provider", "status"}},
		{"idx_integration_webhook_nonce_expiry", "_integration_webhook_nonces", []string{"expires_at"}},
		{"idx_integration_webhook_subscription_connection", "_integration_webhook_subscriptions", []string{"workspace_id", "connection_key"}},
		{"idx_integration_credentials_status", "_integration_secrets", []string{"workspace_id", "credential_type", "status"}},
		{"idx_integration_provider_run_due", "_integration_provider_runs", []string{"run_kind", "status", "due_at", "lease_expires_at"}},
		{"idx_integration_external_identities_actor", "_integration_external_identities", []string{"actor_id", "role_key"}},
		{"idx_web_push_subscription_user", "_integration_web_push_subscriptions", []string{"workspace_id", "user_id", "status"}},
	}
	statements := make([]string, 0, len(specs))
	for _, spec := range specs {
		builder := ormschema.NewIndex(renderer, spec.name, spec.table).Columns(spec.columns...)
		if driver != ormdialect.MySQL {
			statement, _, err := builder.IfNotExists().Build()
			if err != nil {
				return nil, fmt.Errorf("build Integration index %s: %w", spec.name, err)
			}
			statements = append(statements, statement)
			continue
		}
		statement, _, err := builder.Build()
		if err != nil {
			return nil, fmt.Errorf("build Integration index %s: %w", spec.name, err)
		}
		// MySQL has no portable CREATE INDEX IF NOT EXISTS. The ORM renders the
		// DDL; this information_schema guard only adopts legacy Runtime indexes.
		escaped := strings.ReplaceAll(statement, "'", "''")
		variable := "domainry_integration_" + strings.TrimPrefix(spec.name, "idx_")
		statements = append(statements,
			"SET @"+variable+" = (SELECT IF(COUNT(*) = 0, '"+escaped+"', 'SELECT 1') FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = '"+spec.table+"' AND index_name = '"+spec.name+"')",
			"PREPARE "+variable+"_stmt FROM @"+variable,
			"EXECUTE "+variable+"_stmt",
			"DEALLOCATE PREPARE "+variable+"_stmt",
		)
	}
	return statements, nil
}

func table(renderer modulehost.Dialect, name string, columns ...ormschema.ColumnDefinition) *ormschema.TableBuilder {
	return ormschema.NewTable(renderer, name).IfNotExists().Columns(columns...).PrimaryKey("id")
}

func req(name string, kind ormschema.ColumnType) ormschema.ColumnDefinition {
	return ormschema.Column(name, kind).NotNull()
}
func opt(name string, kind ormschema.ColumnType) ormschema.ColumnDefinition {
	return ormschema.Column(name, kind)
}
func key(name string) ormschema.ColumnDefinition     { return req(name, ormschema.TextKey(255)) }
func scope(name string) ormschema.ColumnDefinition   { return req(name, ormschema.TextKey(191)) }
func text(name string) ormschema.ColumnDefinition    { return req(name, ormschema.LongText()) }
func integer(name string) ormschema.ColumnDefinition { return req(name, ormschema.BigInt()) }

func connectionsTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_connections", key("id"), scope("connection_key"), scope("workspace_id"), scope("connector_key"), key("provider_key"), opt("name", ormschema.Text()), key("status"), text("config_json"), text("secret_refs_json"), opt("created_by", ormschema.TextKey(255)), key("created_at"), key("updated_at")).Unique("workspace_id", "connection_key")
}

func connectionAccountsTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_connection_accounts", key("id"), scope("workspace_id"), scope("connection_key"), req("scope", ormschema.TextKey(32)), req("owner_user_id", ormschema.TextKey(191)), key("created_by"), key("created_at"), key("updated_at")).Unique("workspace_id", "connection_key")
}

func connectionAccountSecretsTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_connection_account_secrets", key("id"), scope("workspace_id"), scope("connection_key"), scope("secret_key"), key("created_at")).Unique("workspace_id", "connection_key", "secret_key").Unique("workspace_id", "secret_key")
}

func providerRunsTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_provider_runs",
		key("id"), scope("workspace_id"), req("run_kind", ormschema.TextKey(32)), scope("run_key"),
		scope("connector_key"), scope("provider_key"), scope("connection_key"), scope("task_key"),
		integer("state_version"), key("operation_key"), key("contract_sha256"), text("payload_json"),
		key("status"), key("last_error_code"), integer("attempt_count"), key("due_at"),
		key("lease_owner"), key("lease_expires_at"), integer("fencing_token"), key("created_at"), key("updated_at"),
	).Unique("workspace_id", "run_kind", "connection_key", "run_key")
}

func secretMaterialsTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_secret_materials", key("id"), scope("workspace_id"), scope("secret_key"), text("ciphertext"), key("created_at"), key("updated_at")).Unique("workspace_id", "secret_key")
}

func secretsTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_secrets",
		key("id"), scope("secret_key"), scope("workspace_id"), req("credential_type", ormschema.TextKey(32)), key("kind"), key("status"),
		opt("name", ormschema.Text()), opt("description", ormschema.Text()), opt("value_ref", ormschema.Text()), opt("fingerprint", ormschema.TextKey(255)),
		opt("display_prefix", ormschema.TextKey(255)), opt("lookup_hash", ormschema.TextKey(255)), opt("actor_id", ormschema.TextKey(255)), opt("role_key", ormschema.TextKey(255)), text("scopes_json"),
		opt("created_by", ormschema.TextKey(255)), key("created_at"), key("updated_at"), opt("disabled_at", ormschema.TextKey(255)), key("expires_at"), key("rotated_at"), key("revoked_at"), key("last_used_at"), key("last_tested_at"), key("last_test_status"), text("last_test_error"),
	).Unique("workspace_id", "secret_key").Unique("lookup_hash")
}

func externalIdentitiesTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_external_identities", key("id"), scope("identity_key"), scope("workspace_id"), key("provider"), scope("external_subject"), key("external_subject_type"), opt("external_name", ormschema.Text()), opt("external_organization", ormschema.Text()), opt("external_department", ormschema.Text()), opt("external_group", ormschema.Text()), opt("external_bot_id", ormschema.Text()), key("actor_id"), key("role_key"), key("status"), opt("last_resolved_at", ormschema.TextKey(255)), key("created_by"), key("created_at"), key("updated_at"), opt("disabled_at", ormschema.TextKey(255))).Unique("workspace_id", "identity_key").Unique("workspace_id", "provider", "external_subject")
}

func invocationsTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_invocations", key("id"), scope("workspace_id"), scope("connector_key"), key("provider_key"), opt("connection_key", ormschema.TextKey(191)), key("operation"), key("status"), integer("duration_ms"), opt("request_ref", ormschema.Text()), opt("response_ref", ormschema.Text()), opt("error", ormschema.Text()), opt("event_id", ormschema.TextKey(255)), opt("object_key", ormschema.TextKey(255)), opt("record_id", ormschema.TextKey(255)), opt("workflow_execution_id", ormschema.TextKey(255)), text("metadata_json"), key("created_at"), key("updated_at")).Unique("workspace_id", "id")
}

func eventsTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_events", key("id"), scope("workspace_id"), scope("provider"), scope("event_type"), scope("external_id"), key("status"), text("payload_json"), key("mapping_key"), key("target_type"), text("execution_json"), opt("error", ormschema.Text()), integer("attempt_count"), key("next_retry_at"), key("last_attempt_at"), key("lease_owner"), key("lease_expires_at"), integer("fencing_token"), key("received_at"), key("updated_at")).Unique("workspace_id", "provider", "external_id")
}

func webhookNoncesTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_webhook_nonces", key("id"), scope("workspace_id"), scope("connector_key"), scope("nonce"), key("request_timestamp"), key("created_at"), key("expires_at")).Unique("workspace_id", "connector_key", "nonce")
}

func webhookSubscriptionsTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_webhook_subscriptions", key("id"), scope("subscription_key"), scope("workspace_id"), opt("name", ormschema.Text()), scope("connector_key"), scope("connection_key"), text("event_types_json"), key("status"), opt("description", ormschema.Text()), key("created_by"), key("created_at"), key("updated_at"), key("disabled_at")).Unique("workspace_id", "subscription_key")
}

func webPushSubscriptionsTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return table(r, "_integration_web_push_subscriptions", key("id"), scope("workspace_id"), scope("user_id"), key("endpoint_hash"), text("endpoint"), text("p256dh"), text("auth_secret"), key("status"), key("expires_at"), key("created_at"), key("updated_at"), key("revoked_at")).Unique("workspace_id", "id").Unique("workspace_id", "endpoint_hash")
}
