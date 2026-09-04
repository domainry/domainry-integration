# Domainry Integration

`domainry-connectors` owns Connector definitions, the official Provider release
catalog, Provider schemas, and Provider implementations. `domainry-integration`
consumes that source-owned catalog and materializes a query projection; it does
not author or redefine Connector product metadata.

Integration owns connection and credential configuration, inbound
webhook/event processing, event mapping, Provider task state, Provider
invocation/reconciliation evidence, and Web Push subscription/readiness state.

Runtime owns only its local `_publication_outbox` handoff because that
row must commit atomically with Runtime Record, Action and Workflow facts.
Web Push endpoint, `p256dh`, and auth material never enter that Runtime handoff;
Integration resolves the public `subscription_id` immediately before Provider
execution.

In Module mode Integration uses the host database, dialect, transaction
boundary, migration lock and the host's sole `_schema_migrations` ledger. Its
local workers execute through the Integration SDK boundary; Runtime starts the
loops but contains no Integration worker policy. In SaaS mode Integration owns
those loops in its own process, the same SDK Binding is implemented remotely,
and Integration state is not mirrored into the Runtime database.

## Architecture

The implementation follows an internal DDD boundary:

- `internal/domain/integration` owns deployment-neutral models, repository ports, and validation.
- `internal/application/integration` exposes the Integration use cases.
- `internal/adapter/integrationsdk` is the only conversion boundary to the public SDK.
- `internal/assembly/{module,saas}` composes the same application services for both deployment modes.
- `internal/infrastructure/persistence/database/{integration,migration,schema}` separates business DML, migration registration, and source-owned DDL.
- `internal/transport/http/module` implements the source-owned Module HTTP
  adapter and consumes the route, governance and OpenAPI contract published by
  `domainry-integration-sdk`.
- `module` remains a thin public facade over the internal Module assembly.

The standalone SQLite service requires `INTEGRATION_RUNTIME_ID` and
`INTEGRATION_SERVICE_TOKEN`; `INTEGRATION_SQLITE_PATH` and
`INTEGRATION_HTTP_ADDRESS` are optional. Provider adapters and a production
secret cipher should be supplied by the deployment composition.

## Published contracts

Integration publishes deployment-neutral contracts through
`domainry-integration-sdk`:

- capability disclosure and minimal, representative and repair examples;
- Connector-specialized authoring schemas and references;
- Module HTTP routes, listener exposure, authorization governance and OpenAPI;
- the `@domainry/integration-client` browser package.

Plane generates its admin disclosure and adapter inventory from those SDK
contracts. Runtime only hosts and aggregates the selected Binding; it does not
contain a second Integration capability catalog, OpenAPI implementation or
browser client.
