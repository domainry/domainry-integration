# Provider account capacity model

This document defines the Integration-owned scheduler capacity boundary for
10,000 and 100,000 connected mail accounts. It covers durable state runs,
owner-scoped account reads, fair task selection, leases, internal rate
admission, Provider-call concurrency, result commits and installation-level
run snapshots. It does not claim vendor quota readiness or successful delivery
against real Google or Microsoft accounts.

## Workload model

The stricter current steady-state workload is Microsoft 365. One background
task alternates Inbox and Sent executions and, after both complete, waits the
default 15-minute reconciliation interval. With one page per folder this is
eight task executions per account-hour. Google Workspace normally requires one
Gmail sync per hour plus roughly one watch renewal per day and therefore fits
inside the Microsoft baseline. Bootstrap pagination, retries and notification
bursts are additional workload and must consume the reserved headroom or be
modeled separately before changing the operating envelope.

The model uses 70% target utilization and a deployment-measured Provider call
p95 of two seconds. For account count `A`, executions per account-hour `E`,
target utilization `U`, and Provider latency `L`:

```
tasks_per_second       = A * E / 3600
required_rate_per_min  = ceil(tasks_per_second * 60 / U)
required_concurrency   = ceil(tasks_per_second * L / U)
required_processes     = ceil(required_concurrency / 25)
```

Twenty-five is the current default worker batch, so it is also the effective
per-process concurrency even though the bounded execution pool supports up to
32. A deployment that deliberately configures a batch of at least 32 may use
32 in the last formula.

| Accounts | Tasks/s | Required admissions/min | Concurrent calls at 2s p95 | Processes at default batch 25 |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 22.22 | 1,905 | 64 | 3 |
| 100,000 | 222.22 | 19,048 | 635 | 26 |

The Integration global and per-Provider scheduler ceilings are both 20,000
task admissions per minute. These are internal admission ceilings, not vendor
API quotas. The per-Workspace ceiling remains 1,000/minute and the
per-Connection ceiling remains 120/minute. At 70% utilization, the Microsoft
baseline therefore permits 5,250 accounts per Workspace; 100,000 accounts must
be spread across at least 20 Workspaces. A different tenant distribution or a
higher measured latency requires recalculating the process count rather than
bypassing the limits.

The worker first claims candidates and their connection leases, then executes
independent connections in a bounded pool. The durable connection lease still
prevents two tasks for the same account from running together. Workspace
round-robin selection, four rate scopes, fencing tokens and retry behavior are
unchanged.

## Load acceptance

The opt-in acceptance test creates both 10,000-account and 100,000-account
file-backed SQLite/WAL datasets across 1,000 Workspaces. Each account has a
connection, an owner row and a due native state run. For each size it executes:

- 16 concurrent owner-scoped readers and rejects any cross-owner result;
- four concurrent fair schedulers selecting 500 Workspaces per batch;
- one 500-task burst through real candidate selection, four-scope admission,
  connection lease, a 25 ms deterministic Provider, result commit and lease
  release; and
- one full installation-level Provider run snapshot.

The checked-in gates are owner-list p95 at most 100 ms, fair-selection p95 at
most 250 ms, burst throughput at least 50 tasks/s, at least 16 simultaneous
Provider calls, and snapshot latency at most 500 ms. Run it explicitly:

```sh
INTEGRATION_PROVIDER_CAPACITY_ACCEPTANCE=1 go test \
  ./internal/infrastructure/persistence/database/integration \
  -run '^TestProviderAccountCapacityAcceptance$' -count=1 -v
```

The 2026-09-29 Apple M4 / macOS 15.6.1 / Go 1.27.1 acceptance produced:

| Accounts | Owner list p95 | Fair select 500 p95 | 500-task burst | Max Provider concurrency | Snapshot |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 11.68 ms | 174.22 ms | 229.17 tasks/s | 32 | 5.54 ms |
| 100,000 | 11.11 ms | 177.28 ms | 143.20 tasks/s | 32 | 27.60 ms |

This proves the current local persistence and scheduler path at the stated
dataset sizes. It does not replace production-database testing, deployment
latency measurement, official Google/Microsoft quota telemetry, real-account
end-to-end validation, or SLA alerting. Those are separate acceptance gates.
