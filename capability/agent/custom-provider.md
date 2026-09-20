# When should a project implement a custom Connector Provider?

## Problems solved

- Extends the external capability catalog for a proprietary or unsupported Provider without bypassing Runtime transport, credential governance, typed operation contracts, or startup validation.

## Business scenarios

- An enterprise product must call a private ERP API that has no released Domainry Provider.
- A supported Connector category needs one additional vendor-specific Operation that the released Provider does not declare.

## Use when

Use a custom Provider only after the locked Integration catalog proves that no released Provider and Operation satisfy the confirmed external-system requirement.

## Do not use when

Do not build a custom Provider when a released contract already fits. Do not use it to bypass missing Model authoring, permissions, connection governance, or operation-specific generated clients.

## How to use

1. Use the Connector selection guide to record the exact missing Connector, Provider, or Operation contract.
2. Implement `connector.Adapter` under `backend/connectors` with the public `github.com/domainry/domainry-connector-sdk` only.
3. Declare a stable Connector key, Provider key and revision, typed Operations, configuration fields, write-only secret fields, allowed execution modes, side effects, idempotency, reconciliation, compensation, timeout, test, and dry-run behavior.
4. Construct the Provider with the supplied `connector.Transport` and register it through `backend/connectors/registry.go` in `ProviderSet`. Never create a second HTTP, SQL, secret, or retry subsystem.
5. Test descriptor validation, duplicate registration, configuration validation, bounded transport, error classification, idempotency, uncertain outcomes, and reconciliation behavior.
6. Require an authorable connection requirement and an operation-specific generated client before a Business Handler uses the Provider. If either is absent from the current compiler contract, report a Contract Gap and leave the Handler free of direct network calls.

## Adaptation cookbook

| Business requirement | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| Call a private ERP that is absent from the released catalog | Project-owned Provider implementing the closest stable Connector contract, or a new contract when no semantic match exists | Declare typed read/write Operations, bounded configuration and secret fields, construct with Runtime transport, and register through `ProviderSet` | Embedding the ERP base URL and API token in a Business Handler |
| Add one vendor-specific lookup to an existing Connector category | Custom Provider Operation with a distinct stable key and read-only reliability contract | Model typed input/output, natural idempotency, timeout and error classification while reusing the public Connector SDK | Claiming an undeclared official Operation exists or returning untyped provider JSON |
| Use a released Provider with different Workspace credentials | A governed Integration connection, not a custom Provider | Select the released Provider and configure an authorized connection through Integration-owned surfaces | Forking Provider code just to store another credential |
| Compiler cannot bind the chosen Operation to a Handler | Contract Gap | Report the missing connection/operation binding and wait for compiler-generated authority | Calling the generic Connector gateway or direct HTTP to work around the missing binding |

## Example

A private ERP exposes `lookup_customer` and `create_invoice`. The project Provider declares separate typed read and write Operations, gives `create_invoice` an explicit idempotency and reconciliation contract, receives Runtime-owned transport, and registers through `ProviderSet`. The Business Handler may call it only after the compiler generates a client for that exact Operation.

## Permissions and scope

Provider registration grants no product permission. Connection administrators govern configuration and secret references; Runtime supplies only the authorized connection and principal for a declared Operation. The Provider must not access unrelated Workspace data or credentials.

## Boundaries

A custom Provider implements an external protocol. It does not own business validation, user authorization, Connection or Secret persistence, generic networking, scheduling, or the decision to create the external effect.
