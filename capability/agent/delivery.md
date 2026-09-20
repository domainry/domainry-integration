# Who owns an external provider call versus business intent?

## Problems solved

- Separates the domain decision to create an external effect from provider invocation, retry, receipt, and reconciliation.

## Business scenarios

- Notification decides to email a user while Integration invokes the configured email provider.
- A payment or CRM Handler records authorized intent while Integration performs the provider protocol and captures its outcome.

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

Notification decides that a user should receive email; Integration invokes the configured email provider. A payment Handler decides the amount and eligibility; Integration performs the provider protocol.

## Permissions and scope

The caller receives the business permission. Integration’s service authority is limited to the chosen connection/provider operation and cannot read unrelated project data.

## Boundaries

Notification owns user communication, Scheduler owns time, business modules own intent, and Integration owns the external boundary.
