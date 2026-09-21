# How should Integration credentials, API keys, accounts, and external identities be modeled?

## Problems solved

- Keeps secret material, provider account grants, application API keys, and external subject mappings inside governed Integration/Identity boundaries instead of project records or Handler input.

## Business scenarios

- A Workspace administrator authorizes one CRM account through OAuth while Handlers receive only a connection key and typed operation.
- A provider subject or application API key is mapped to a stable internal principal without exposing secret material to business code.
- A salesperson connects a personal calendar account that no other principal may reuse, while a Workspace-owned ERP service connection is shared only through exact Operations.
- A revoked or refresh-failed credential blocks new invocations until governed reauthorization; copying the token into a Record is never a recovery mechanism.

## Use when

Use these capabilities when an external provider requires OAuth/account ownership, secret references, application credentials, or external-to-internal identity mapping.

## Do not use when

Do not store provider tokens in project Objects, accept raw secrets in Business Operations, or use an external subject as an internal authorization claim without a governed mapping.

## How to use

Create a validated Connection requirement, complete account authorization through Integration, store only secret references, issue/rotate application keys through dedicated operations, and map external subjects to Identity principals with explicit issuer/provider scope.

## Adaptation cookbook

| Credential/identity requirement | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| User authorizes their CRM account | Current-user connection account plus OAuth session | Derive principal/Workspace from authenticated context, complete OAuth with state/PKCE, bind the account to the declared Connection, and expose typed reads | Sending refresh tokens to the browser or storing them on a customer Object |
| Workspace sends all mail through one service account | Workspace-owned service Connection | Administrators manage the Connection and secret reference; Handlers reference only connection/operation keys and never see plaintext | Sharing one user's personal OAuth token or embedding the provider secret in project settings |
| Server-to-server provider needs a secret | Integration secret reference | Resolve secret material only inside the Provider execution boundary; pass connection/operation keys to the Handler | Putting the API secret or vault path in Agent prompts or Operation input |
| External webhook identifies a provider user | External identity mapping | Scope mapping by provider/issuer and external subject, resolve one internal principal, and audit changes | Trusting email or arbitrary external ID as an internal user ID |
| Partner application calls an Integration endpoint | Managed application API key | Issue, rotate, revoke, hash/store, and scope the key through Integration operations | Reusing a human session token or placing a long-lived plaintext key in project config |

## Example

Salesperson `user-42` authorizes personal calendar Connection `my_calendar` with OAuth state/PKCE. Only that principal may invoke its read Operations. Workspace Connection `erp_primary` uses a service credential managed by administrators; an invoice Handler receives only `erp_primary + invoice.create`. Provider subject `acct-991` maps to the same internal principal even if its display name changes. On refresh failure or revocation, new invocations return an explicit reauthorization state and no stale token is exposed, copied into a project Record, or silently borrowed from another user.

## Permissions and scope

Connection administration, account authorization, secret administration, API-key administration, and external-identity mapping are separate permissions. No read operation returns secret material.

## Boundaries

Integration owns provider credential/account/mapping state; Identity owns internal principals; business owners receive typed results and stable connection identities only.
