# How should a provider webhook enter Runtime safely?

## Problems solved

- Converts untrusted provider callbacks into verified, deduplicated, attributable Runtime events before business logic consumes them.

## Business scenarios

- A CRM contact-change webhook starts a mapped Workflow or Business Operation once.
- A payment-status or delivery-receipt callback is signature-checked, replay-protected, and reconciled.

## Use when

Use Integration events when a provider delivers signed callbacks that require verification, deduplication, durable receipt, mapping, and Runtime handoff.

## Do not use when

Do not create a public project Handler that trusts provider JSON directly. Internal domain events do not need Integration.

## How to use

Verify provider signature and subscription, persist the inbound event idempotently, map it to a typed Runtime target, and record handoff/reconciliation evidence.

## Adaptation cookbook

| Business requirement | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| CRM contact change should update local state exactly once | Verified webhook ingress plus dedupe and mapped Business Operation | Validate provider signature/timestamp, persist provider event ID, map external contact identity, and invoke an idempotent owner Operation after acceptance | Updating project tables directly from raw JSON before signature or replay checks |
| Payment callback may arrive before or after the synchronous response | Webhook reconciliation against the existing payment intent | Correlate provider object and idempotency key, accept repeated events safely, and transition only through valid owner states | Creating a new payment record for each callback or trusting event arrival order |
| Delivery provider posts a receipt | Integration receipt mapping followed by Notification delivery-state update | Preserve raw provider evidence within bounded retention and emit the normalized outcome to Notification | Moving message template or recipient preference decisions into the webhook handler |
| An internal domain event starts a Workflow | Runtime event/automation path, not webhook ingress | Publish the committed internal event and subscribe through the internal capability | Serializing an internal event to HTTP and calling the product's own public webhook endpoint |

## Example

A CRM webhook updates an external contact. Integration verifies and deduplicates it, then invokes a published workflow/operation mapping. The business owner validates whether the update is allowed.

## Permissions and scope

Provider admission uses connection/subscription authority, not an end-user Role. Operator event reads and replays require separate permissions and remain Workspace-scoped.

## Boundaries

Integration proves and transports the provider event; it does not own the resulting business state transition.
