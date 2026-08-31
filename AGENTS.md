# Domainry Integration development guide

- This repository is an independent Go module and must not import `domainry-runtime/internal/**` or Runtime implementation packages.
- Preserve the internal DDD layout: `internal/application`, `internal/domain/integration/{model,repository,service}`, `internal/adapter`, `internal/assembly`, `internal/transport`, and `internal/infrastructure`.
- Integration owns connector catalog, connection requirements, delivery evidence, secrets, provider state, inbound events, and Web Push subscriptions.
- Runtime owns only its transactional publication outbox and composes Integration through the public Integration SDK.
- Module mode uses the host database, dialect, transaction boundary, migration lock, and sole `_schema_migrations` ledger.
- Keep the public `module` package a thin facade over `internal/assembly/module`; do not expose domain, application, persistence, or transport implementation packages.

