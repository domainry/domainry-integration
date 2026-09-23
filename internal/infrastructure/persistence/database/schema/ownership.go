package schema

import "github.com/domainry/domainry-foundation/schemaownership"

const (
	MigrationOwner                       = "integration"
	IntegrationConnectionAccountsTable   = "_integration_connection_accounts"
	IntegrationConnectionsTable          = "_integration_connections"
	IntegrationEventsTable               = "_integration_events"
	IntegrationExternalIdentitiesTable   = "_integration_external_identities"
	IntegrationInvocationsTable          = "_integration_invocations"
	IntegrationOAuthApplicationsTable    = "_integration_oauth_applications"
	IntegrationOAuthSessionsTable        = "_integration_oauth_sessions"
	IntegrationProviderRunsTable         = "_integration_provider_runs"
	IntegrationSecretMaterialsTable      = "_integration_secret_materials"
	IntegrationSecretsTable              = "_integration_secrets"
	IntegrationWebhookNoncesTable        = "_integration_webhook_nonces"
	IntegrationWebhookSubscriptionsTable = "_integration_webhook_subscriptions"
	IntegrationWebPushSubscriptionsTable = "_integration_web_push_subscriptions"
)

var tableOwnership = []schemaownership.Table{
	{
		Name: IntegrationConnectionAccountsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionUserErase, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "workspace/connection identity and workspace/scope/owner/connection account index",
		DeletionPolicy:   "account revocation preserves terminal ownership state; connection deletion and subject erasure remove or anonymize the owner-scoped account",
	},
	{
		Name: IntegrationConnectionsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionProduct, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "workspace/connection-key identity and bounded workspace connection listing",
		DeletionPolicy:   "explicit connection deletion removes the row; subject erasure revokes and redacts personal connection configuration and embedded OAuth grants",
	},
	{
		Name: IntegrationEventsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionTechnicalTTL, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "workspace event identity, provider/external idempotency identity, bounded status listing and fenced retry claims",
		DeletionPolicy:   "terminal inbound events follow operational retention; subject erasure redacts payload and cancels eligible subject-linked work",
	},
	{
		Name: IntegrationExternalIdentitiesTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionUserErase, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "workspace identity key or workspace/provider/external-subject identity plus actor index",
		DeletionPolicy:   "explicit disable preserves non-sensitive mapping state; subject erasure removes actor-owned external identities",
	},
	{
		Name: IntegrationInvocationsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionTechnicalTTL, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "workspace invocation identity and bounded connector/status/time, request, event or business-resource lookups",
		DeletionPolicy:   "terminal invocation evidence follows operational retention; subject erasure redacts references, errors and metadata after active work is fenced",
	},
	{
		Name: IntegrationOAuthApplicationsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionProduct, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "exact workspace/application-key identity",
		DeletionPolicy:   "OAuth application configuration follows explicit application replacement or removal and installation retention",
	},
	{
		Name: IntegrationOAuthSessionsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionTechnicalTTL, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "globally unique state hash or exact workspace/user/session identity with expiry checks",
		DeletionPolicy:   "consumed and expired authorization sessions are TTL-eligible; subject erasure removes user sessions after active exchange fencing",
	},
	{
		Name: IntegrationProviderRunsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionTechnicalTTL, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "workspace/run-kind/connection/run identity and bounded status/due/lease worker claims",
		DeletionPolicy:   "terminal provider runs follow operational replay retention; subject erasure cancels and redacts subject-linked active work",
	},
	{
		Name: IntegrationSecretMaterialsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionUserErase, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "exact workspace/secret-key identity",
		DeletionPolicy:   "rotation replaces ciphertext in place; credential deletion, connection-account erasure and subject erasure physically remove encrypted material",
	},
	{
		Name: IntegrationSecretsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionUserErase, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "workspace/secret-key identity, global lookup hash, credential status and workspace/connection ownership indexes",
		DeletionPolicy:   "revocation preserves non-secret credential status; explicit deletion and subject erasure remove owned metadata after active-use fencing",
	},
	{
		Name: IntegrationWebhookNoncesTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionTechnicalTTL, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "workspace/connector/nonce replay identity and expiry index",
		DeletionPolicy:   "expired replay nonces are physically purged after the webhook verification window",
	},
	{
		Name: IntegrationWebhookSubscriptionsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionProduct, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "workspace/subscription identity and workspace/connection index",
		DeletionPolicy:   "explicit subscription removal deletes the row; subject erasure disables and redacts subject-linked subscription metadata",
	},
	{
		Name: IntegrationWebPushSubscriptionsTable, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
		RetentionClass: schemaownership.RetentionUserErase, PrimaryKey: []string{"workspace_id", "id"},
		BoundedQueryPath: "workspace/subscription or endpoint-hash identity and workspace/user/status index",
		DeletionPolicy:   "revocation and expiry terminate delivery; explicit removal and subject erasure physically delete user subscriptions",
	},
}

func SchemaOwnership() []schemaownership.Table {
	return schemaownership.Clone(tableOwnership)
}

func OwnedTables() []string {
	return schemaownership.Names(SchemaOwnership())
}
