# Subject erasure owner port

`SubjectLifecycleBinding` exposes privileged preview, export inventory,
prepare and erase ports. Product/browser management adapters never register
them. SaaS routes require the service token and Runtime identity header; the
owner establishes Foundation workspace only after that authentication.

Runtime supplies the account ID, request ID, published record references,
source event IDs and frozen publication message IDs. Lifecycle represents the
single subject fence with the root erase request's indexed `resolved_identity`
and its owner `lifecycle`, operation `erase_fence` execution step; Integration
atomically stores its frozen `erase_plan` and `erase` result as owner
`integration` entries in `_subject_steps`. Plans contain typed connection,
credential, message, resource, external-subject and row references, never
payloads, verifier material, browser keys, external names or encrypted secrets.
Reusing a request with different subject or provenance fails. A different plan
cannot execute against the shared step.

Cleanup includes personal connection configuration and exclusive encrypted
credentials, connection grants/worker captures, per-user OAuth sessions and
Web Push key material, account API keys, external bindings and invocation
payloads. Shared connections and credentials referenced by another connection
are preserved. Actual running delivery, reconciliation, provider effects,
OAuth exchanges and live Provider runs block preparation. Queued
effects are cancelled and their worker fencing tokens advanced.

Inbound ownership follows verified sender identities, the same published
mapping selection used for execution, exact mapping record targets and the
source-owned connection metadata. The owner overwrites incoming reserved
context so callers cannot forge those references. External subject digest
fences reject later private ingress and rebinding after raw external bindings
have been removed.

Each owner erase transaction clears the frozen rows and stores its shared result
together. Failure rolls back earlier changes; the original plan remains usable
for retries. Normal row updates include the frozen-plan row reference in their
final SQL predicate; late manual delivery retry must win a one-row claim before
calling the external provider. Ordinary standalone Integration operations do
not query Lifecycle tables. Subject cleanup fails closed until the embedding
host explicitly binds shared Lifecycle persistence after installing its schema.

Tests use public local and remote SDK ports, actual owner migrations and disk
SQLite with encrypted materials. They prove injected delete rollback, stable
retries, legal-hold and active-effect refusal, immutable plans, peer/workspace
isolation, verified webhook attribution and later ingress/rebinding refusal.
The CRM verifier/provider is a deterministic protocol fixture, not vendor I/O.

The Runtime connector gateway supplies execution, object and record references
from its immutable execution context. Standard and sensitive operation metadata
preserve these typed references, so an operation performed by another actor on
an owned record is included in cleanup. A later call against that erased record
is rejected before provider I/O. Tests cover an operator using a shared
connection for two members, preserving the other member's invocation. Delivery
requests carry the exact Runtime publication message ID.
