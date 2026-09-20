# Domainry Integration

Agent-facing question index and source-owned guides: [`capability/agent/index.json`](capability/agent/index.json).

The Agent index deliberately exposes Connector catalog selection and custom
Provider extension because those are product decisions. It does not expose
Connection or Secret persistence, encryption, credential injection, retry
workers, or operational status handling as project-authored behavior.
Deck derives its lightweight per-Connector purpose index from Integration's
locked capability projections, while executable availability comes only from
the official Provider release catalog rather than planned Provider definitions.

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

The standalone SQLite service requires `INTEGRATION_RUNTIME_ID`,
`INTEGRATION_SERVICE_TOKEN`, and `INTEGRATION_MASTER_KEY` (a base64-encoded
32-byte AES key kept in the deployment secret store). `INTEGRATION_SQLITE_PATH`
and `INTEGRATION_HTTP_ADDRESS` are optional. Keep the same master key across
restarts and backups; the service never generates an ephemeral replacement.
The executable composes Google Workspace and Microsoft 365 through the public
Connectors module. Its host transport permits only their official HTTPS API
domains, bounds responses, injects secret fields at dispatch, and refuses
redirects. Module hosts continue to provide their own cipher and transport.

Public web access is opt-in through `INTEGRATION_WEB_PROXY_ORIGIN`, an exact
llm-proxy origin (HTTPS, or explicitly configured loopback HTTP). This adds the
independent `web/llm_proxy` Provider with a separate transport permitting only
POST `/tool/web_search` and `/tool/web_fetch_jina`. It does not widen the work
account transport. Register a workspace service connection with the matching
`base_url`, `allowed_source_hosts` (1–16 exact public DNS hosts), optional
`processor` (`base` or `pro`) and `timeout_seconds` (1–60, default 40), and an
encrypted `api_token` containing a raw Passport token. Account registration and
current workspace list/read permissions remain required; an empty OAuth scope
rule is not anonymous access. The Provider has no free connection probe.
Proxy credentials never come from model arguments, cookies or browser sessions.
The host bounds request/response bytes, refuses redirects and disables POST
replay. The remote llm-proxy/Reader service owns its target DNS, rendering and
redirect policy; source URL validation is not proof of that remote network policy.

Sensitive synchronous calls claim their invocation identity with a unique insert
before Provider I/O. Failed, in-flight and interrupted calls cannot reclaim the
same identity, even after restart. Successful replay returns evidence without the
discarded sensitive payload. An explicit new request ID requires current owner
authorization and may incur a new service charge. Integration does not persist
the query, page text, source URL or credential in the sensitive invocation ledger.
Structured JSON configuration accepts equivalent native Go arrays/maps and HTTP
JSON arrays/objects; the Provider owns validation of their actual contents.

OAuth application configuration belongs to Integration. Authorized administrators
use `OAuthApplications.UpsertOAuthApplication` (or the Integration-owned
`PUT /integration/oauth-applications/{applicationKey}` product endpoint) to save
client ID, write-only client secret, exact product callback URI, allowed scopes,
Provider configuration and enabled state. Updates require `expected_updated_at`;
omitting the secret preserves the existing encrypted value. Ordinary users see
only enabled application labels and allowed scopes. Google uses
`google_workspace/google`; Microsoft uses `microsoft_365/microsoft` and requires
`connection_config.tenant_id`, with `offline_access` among its allowed scopes.
Client registration and consent in Google Cloud or Microsoft Entra still require
an administrator's actual external account; application code cannot manufacture
valid third-party client credentials.

The authenticated product starts `OAuthAuthorizations.StartOAuthAuthorization`
for the current user and personal/workspace scope, navigates to the one-time
`authorization_url`, and sends the returned `state` and `code` (or OAuth denial)
through `CompleteOAuthAuthorization`. Integration keeps the PKCE verifier
encrypted and state hashed, binds the callback to the current user/workspace,
and persists a one-time exchange claim before contacting the Provider. The
callback URI is a product page that forwards to the authenticated callback API;
the SaaS backend is a service-token API, not an anonymous OAuth callback page.
Use no-store/no-referrer for navigation and callbacks, scrub callback query
parameters before loading other page resources, and never put codes or URLs in
chat, telemetry or logs. Get-session responses contain no navigation URL.

A successful code exchange creates independently encrypted account credentials
and records actual granted scopes. Denials become `rejected`; ambiguous exchange
results and missing refresh tokens become `needs_reauthorization` with no
automatic code replay. Revocation prevents future local credential use and
refresh commits; it does not claim to delete the user's upstream OAuth consent.
Account pages and Agent tool preferences are product integration work tracked
under F01 in the Agent TODO.

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

### Account probe scope readiness

The safe account DTO separates local `readiness.available` from optional
`readiness.test`. The fixed probe's requirements come from the Connector SDK
capability, while Integration evaluates its own persisted OAuth grant.
`scope_required` and `scope_unverified` prevent the account test before provider
I/O; they do not invalidate credentials used by independently authorized business
operations. Invalid provider requirements fail closed. No requested scope is added
automatically. Providers without this declaration retain their previous test path.

## Host-authorized account writes

The optional public `ConnectionAccountWritesBinding` composes current account
and grant authorization, strict calendar/mail contracts, insert-only invocation
claims and bounded receipts. Application orchestration depends on internal
ports; the protocol codec imports only public Connector SDK write contracts.
Credential/configuration resolution and ORM writes remain owner infrastructure.
There is no Provider implementation import, account-store copy or new migration.

The invocation identity binds workspace, actor and stable host RequestID; the
fingerprint binds exact account revision, operation contract and canonical typed
payload. Claims use the existing `_integration_invocations` primary key. Terminal
updates match the original claim metadata and running state. Repeated, failed,
in-flight and crash-interrupted requests are never reset or automatically sent
again. This namespace is excluded before the generic reconciliation query limit.

Receipts preserve only validated provider acknowledgments, not outgoing text,
recipients, configuration, secrets or raw errors. Receipt persistence survives
caller cancellation with a bounded owner cleanup context. Credential rotation
failure is separately recorded and cannot erase a provider-confirmed success.
The host must authorize every execution and retained-result access; Integration
also checks current ownership, revision, state and actual grants. Its receipt
lookup never calls the vendor. Revocation can therefore hide existing evidence.

Embedded hosts consume the Go port; remote hosts use service-authenticated POST
`/integration/v1/connection-accounts/{key}/write-access`, `/write`, `/write-receipt`.
No corresponding mutation routes are added to the public product HTTP adapter.
The Agent/Tools host owns exact-content confirmation and execution IDs.
