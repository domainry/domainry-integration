# Domainry Integration

`domainry-integration` owns Connector catalog, connection and credential
configuration, inbound webhook/event processing, event mapping, Provider task
state, Provider invocation evidence, and Web Push subscription/readiness state.

Runtime owns only its local `_publication_outbox` handoff because that
row must commit atomically with Runtime Record, Action and Workflow facts.
Web Push endpoint, `p256dh`, and auth material never enter that Runtime handoff;
Integration resolves the public `subscription_id` immediately before Provider
execution.

In Module mode Integration uses the host database, dialect, transaction
boundary, migration lock and the host's sole `_schema_migrations` ledger. In
SaaS mode the same SDK Binding is implemented remotely and Integration state is
not mirrored into the Runtime database.

## Architecture

The implementation follows an internal DDD boundary:

- `internal/domain/integration` owns deployment-neutral models, repository ports, and validation.
- `internal/application/integration` exposes the Integration use cases.
- `internal/adapter/integrationsdk` is the only conversion boundary to the public SDK.
- `internal/assembly/{module,saas}` composes the same application services for both deployment modes.
- `internal/infrastructure/persistence/database/{integration,migration,schema}` separates business DML, migration registration, and source-owned DDL.
- `module` remains a thin public facade over the internal Module assembly.

The standalone SQLite service requires `INTEGRATION_RUNTIME_ID` and
`INTEGRATION_SERVICE_TOKEN`; `INTEGRATION_SQLITE_PATH` and
`INTEGRATION_HTTP_ADDRESS` are optional. Provider adapters and a production
secret cipher should be supplied by the deployment composition.
