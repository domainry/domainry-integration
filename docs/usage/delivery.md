# Who owns an external provider call versus business intent?

## Problems solved

- Separates the domain decision to create an external effect from provider invocation, retry, receipt, and reconciliation.

## Business scenarios

- Notification decides to email a user while Integration invokes the configured email provider.
- A payment or CRM Handler records authorized intent while Integration performs the provider protocol and captures its outcome.
- A post-commit ERP write can finish as confirmed success, retryable failure, uncertain outcome, or permanent rejection, each with different recovery.
- Compensation is available only when the released Provider explicitly publishes an inverse Operation.

## Use when

Use Integration delivery/invocation when a Business Operation, Notification, or scheduled target must call an external provider.

## Do not use when

Do not put business validation or notification-recipient policy inside the Connector. Do not select Integration for in-app-only messages.

## How to use

The business owner emits typed intent. Integration resolves a governed connection and invokes the Connector/provider operation with idempotency and reconciliation evidence.

## Adaptation cookbook

| Business requirement | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| Notification decides to send email through a configured provider | Notification intent plus Integration delivery execution | Notification resolves recipient/template/channel; Integration selects the authorized connection, calls the provider, retries transport failures, and records provider receipt | Making Notification own SMTP/API credentials or letting Integration invent recipients and message policy |
| An approved refund must be sent to a payment provider | Domain-owned refund intent plus Integration invocation and reconciliation | The business Handler commits the authorized intent; after commit, Integration sends an idempotency key and maps the provider outcome back to a receipt/status | Calling the provider inside the same database transaction before business intent is durable |
| CRM synchronization gets an uncertain timeout | Integration retry/reconciliation using stable external identity | Reuse the same idempotency key or query provider status before retrying; record whether the remote effect is confirmed, rejected, or unresolved | Blindly issuing a second create call and assuming a timeout means nothing happened |
| A calculation is entirely internal | Runtime Operation or Handler only | Keep deterministic internal state change within the owning domain | Routing internal work through Integration merely because it is asynchronous |

## Example

For an approved invoice, the Handler commits local intent `erp.invoice.create:invoice-88:v3` and returns; after commit, Integration resolves `erp_primary`, invokes the typed Provider Operation with that stable identity, and stores the receipt. A confirmed success completes reconciliation. A provider-declared transient pre-effect failure retries with the same identity. A timeout after request acceptance is `uncertain`, so Integration queries provider status or waits for a webhook before retrying. A permanent validation rejection becomes terminal and exposes remediation. Only a published inverse Operation may compensate a confirmed external write; a local database rollback cannot undo it. For email, Notification still owns recipient/content/channel while Integration owns only connection, protocol, retry, and receipt.

## Permissions and scope

The caller receives the business permission. Integration’s service authority is limited to the chosen connection/provider operation and cannot read unrelated project data.

## Boundaries

Notification owns user communication, Scheduler owns time, business modules own intent, and Integration owns the external boundary.

## Publishing a small generated document

Use the existing `Operations.Call` with SDK `ProviderCallContent` and
`ProviderCallTarget` when a released synchronous Provider already accepts a
content stream. Content is bounded to 256 KiB; JSON operation metadata is also
bounded to 256 KiB. This is a bridge for small generated sources, not a blob
store, generic product upload endpoint or replacement for Data Exchange.

First commit the business owner's immutable approved document, content hash and
stable effect identity. Outside that transaction, read the current named
Connection, build `NewProviderCallTarget(connection, operation.Descriptor())`
and `NewProviderCallContent(bytes, contentType)`, then call the exact official
operation. The host supplies its authenticated actor/role and execution source.
Sensitive persistence is required. A JSON-only mutation such as a removal may
use the same target guard without content.

Integration verifies bytes, hash and MIME, checks the Provider contract and the
full connection snapshot again after secret resolution, and claims before I/O.
The snapshot includes configuration and secret references: changing the KB in
the same timestamp millisecond is still detected. Body and response never enter
Invocation evidence; only the bound request digest and safe routing metadata do.
The existing official Provider receives its content reader and exact identity.

The same effect ID cannot be reset after failure, during an in-flight call, or
after a process crash. A successful replay returns the original Invocation and
no sensitive body. Changed actor, source, target, metadata or bytes conflicts.
Do not convert a timeout or a failed invocation into permission to publish with
a new ID. Read the original Invocation and inspect the document through a
read-only Provider operation. An HTTP acceptance or PENDING state is not proof
that the document is indexed. Approval, applicability, indexing, update ordering
and removal remain business lifecycle decisions.

Module and SaaS public Binding tests run the official KB Provider through this
bridge and prove exact bytes, source identity, redaction, replay and changed-input
rejection. Additional owner tests cover concurrent claims, a lost response and
reopened owner, a changed target, and a same-timestamp scope change. These are
contract tests with a deterministic external transport, not a claim of a live
Platform deployment or a complete product publishing workflow.
