# Which Connector, Provider, and Operation satisfy an external-system requirement?

## Problems solved

- Selects a released Connector, Provider, and typed Operation from the locked Integration catalog without guessing provider support or loading the complete catalog into Agent context.

## Business scenarios

- An order workflow must create or update a record in an external CRM using a supported Provider operation.
- A scheduling product must read an authorized user's external calendar without implementing a second OAuth or HTTP stack.

## Use when

Use this guide when a confirmed product requirement crosses an external API, SaaS system, database, messaging provider, calendar, CRM, payment service, or other Connector boundary.

## Do not use when

Do not select a Connector for behavior that is entirely internal to Runtime. Do not treat connection persistence, secret encryption, credential injection, retries, or invocation evidence as project-authored business capabilities.

## How to use

1. Read `.domainry/framework/module-capabilities/integration/connectors/INDEX.md` to see what every Connector is for and whether it is `released`, `definition_only`, or `runtime_native`.
2. Select only a `released` Connector, then follow its source-details link. The generated index lists only Providers that exist in the locked official Provider release catalog; a declared or planned Provider is not executable support.
3. Confirm the same selection in `.domainry/framework/module-capabilities/integration/summary.json` through an exact `connector_definition.<connector>`, `connector.<connector>.<provider>`, or `connector.<connector>.<provider>.<operation>` capability.
4. Verify the Provider's configuration and secret requirements and the Operation's typed input, output, execution mode, side effect, timeout, idempotency, compensation, test, and dry-run declarations.
5. Record the selected Connector, Provider, Operation, and required connection scope in the product requirement. Keep user intent and authorization in the owning Business Operation.
6. Check `model.schema.json` and `effective-capabilities.json` before authoring. If the current compiler cannot express the connection requirement or generate an operation-specific client, report a Contract Gap; do not invent Model JSON or call a generic gateway.

## Adaptation cookbook

| Business requirement | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| Push an approved order update to a CRM | A released CRM Connector, matching Provider, and declared write Operation | Confirm the exact operation contract and side effect, keep approval in the Business Operation, and invoke only the generated operation-specific client after the connection requirement is authorable | Selecting a Provider by name alone or issuing ad hoc HTTP from the Handler |
| Read a user's calendar to find free time | A released calendar Provider read Operation plus an authorized connection account | Match required OAuth scopes and typed output, preserve current-user ownership, and use the declared read Operation | Requesting broad credentials or copying tokens into project configuration |
| Send an in-app notification with no external delivery | Notification only | Keep the message inside the Notification module and its inbox contract | Selecting an email or messaging Connector merely because the word "notification" appears |
| The catalog has no matching provider or operation | Evaluate the custom Provider extension guide | Prove the catalog gap using exact capability keys, then design only the missing Provider/Operation through the public Connector SDK | Pretending a nearby Provider supports an undeclared operation |

## Example

Suppose `summary.json` lists `connector.crm.acme.update_contact`. The Agent locates the category containing the `crm` projection, verifies that Provider `acme` declares `update_contact`, checks its typed fields and reliability contract, and records that exact selection. If the compiler exposes no authoring path for the connection and operation binding, the result is a Contract Gap rather than handwritten JSON.

## Permissions and scope

Connector discovery is read-only. Using a selected Provider still requires an explicitly authorized Workspace or current-user connection, the Provider's declared scopes, and the owning Business Operation's permission. Catalog visibility never grants credential or invocation access.

## Boundaries

The Connector catalog describes available external contracts; Integration owns governed connections and execution. It does not make every catalog entry authorable by the current compiler, and it does not move business policy into the Provider.
