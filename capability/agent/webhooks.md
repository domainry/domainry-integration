# How should a provider webhook enter Runtime safely?

## Problems solved

- Converts untrusted provider callbacks into verified, deduplicated, attributable Runtime events before business logic consumes them.

## Business scenarios

- A CRM contact-change webhook starts a mapped Workflow or Business Operation once.
- A payment-status or delivery-receipt callback is signature-checked, replay-protected, and reconciled.
- A provider repeats or reorders events; Integration returns the same receipt for a duplicate and the business owner rejects a stale state transition.
- A signature failure produces no business effect and logs bounded diagnostic metadata rather than secrets or an unredacted sensitive payload.

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

Payment Provider event `evt-8821` arrives for the published Subscription. Integration validates signature, timestamp, and connection; persists the Provider event ID and bounded receipt; maps the external payment identity; and only then hands the typed event to `payment.reconcile`. A replay of `evt-8821` returns the same receipt without another business invocation. If an older `authorized` event arrives after `captured`, the payment owner rejects the backward transition. An invalid signature creates no business effect and diagnostics omit the signing secret and raw sensitive body. Runtime-internal committed events use the internal event/Automation path, never this public ingress.

## Permissions and scope

Provider admission uses connection/subscription authority, not an end-user Role. Operator event reads and replays require separate permissions and remain Workspace-scoped.

## Boundaries

Integration proves and transports the provider event; it does not own the resulting business state transition.
