# Domainry Integration

`domainry-integration` owns Connector catalog, connection and credential
configuration, inbound webhook/event processing, event mapping, Provider task
state, Provider invocation evidence, and Web Push subscription/readiness state.

Runtime owns only its local `_publication_outbox` handoff because that
row must commit atomically with Runtime Record, Action and Workflow facts.
Web Push endpoint, `p256dh`, and auth material never enter that Runtime handoff;
Integration resolves the public `subscription_id` immediately before Provider
execution.

In Module mode Integration uses the host database, dialect, transaction
boundary, migration lock and the host's sole `_schema_migrations` ledger. In
SaaS mode the same SDK Binding is implemented remotely and Integration state is
not mirrored into the Runtime database.
