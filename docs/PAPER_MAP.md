# Paper-to-code map

This file is an audit/navigation artifact for readers verifying that this
repository implements the DBLog paper's Algorithm 1. It maps each step of the
algorithm to the code that realizes it, the fail-closed guard that enforces
it, and the test that locks it.

**This file is not a spec.** Semantic guarantees and fail-closed boundaries
live in [README.md](../README.md), the Javadocs on the key source types under
[`src/main/java`](../src/main/java), and the adapter/operator docs. This file
is the audit/index layer, not the authoritative contract.

## How to read the table

- **Primary key is the 10-step procedural breakdown in the table below.**
  Each row maps one algorithm step to the code, guardrail, and tests that
  currently realize it.
- **"Paper Alg 1" column** cites the paper's own tighter numbering, verified
  against the paper. Steps (1) and (3) additionally carry inline
  `paper Algorithm 1 step N` citations in the code; the remaining numbered
  steps are documented here but not anchored inline.
- **Prefer symbol anchors over bare line anchors.** Exact line ranges are
  included only where they help audit the reconciler state machine. Reconciler
  edits or method extractions will shift them; see
  [Maintenance norm](#maintenance-norm) below.

## Step ↔ code map

| §3 step | Paper Alg 1 | Code anchor | Fail-closed guard | Test lock |
|---|---|---|---|---|
| 1. pause forwarding | step (1) | `runtime/sql/BoundedSourceEventQueue` Javadoc (single-consumer invariant), `runtime/loop/RuntimeRequestPump#runUntilStopped` | `IllegalStateException` from `BoundedSourceEventQueue#pollNow` / `assertSingleConsumer` when `-Ddblog.assertions=true` | `runtime/sql/BoundedSourceEventQueueTests`; `e2e/LiveMode*AllTablesConvergenceTests` |
| 2. generate LW/HW tokens | (setup; not numbered in paper) | `core/model/WatermarkToken#random` (UUID-backed). Allocation sites: `PostgresLiveStreamingRuntime#executeChunkReadInWatermarkWindow`, `MySqlLiveStreamingRuntime#executeChunkReadInWatermarkWindow`, `MySqlBufferedStreamingRuntime#executeChunkReadInWatermarkWindow`, `InspectionOnlySourceRuntime#executeChunkReadInWatermarkWindow`. Port: `adapter/api/WatermarkWindowRuntime#executeChunkReadInWatermarkWindow` | Blank token → `IllegalArgumentException` in `WatermarkToken` constructor | Exercised by every watermark-window test |
| 3. write LW | step (2) | Runtime `executeChunkReadInWatermarkWindow` implementations write `window.low()` via `WatermarkMetadataWriter#writeWatermark`; concrete writers: `JdbcPostgresWatermarkTableHelper#writeWatermark`, `JdbcMySqlWatermarkTableHelper#writeWatermark` | `WatermarkSequenceException` in `WindowReconciler.ReconciliationSession#apply` at `WindowReconciler.java:275-288` (missing LW, duplicate LW, LW-after-HW) | `e2e/InspectionMode{MySql,Postgres}MetadataContaminationTests` |
| 4. SELECT chunk | step (3) | `DefaultDumpWindowCoordinator#coordinateNextTableChunk` and `DefaultTargetedRepairCoordinator#coordinate` comments cite "Algorithm 1 step 3". Port: `adapter/api/SourceChunkReader` | Unsupported-PK rejection at orchestration time; schema-fingerprint drift → `FullDumpRequiredSignal` | `e2e/InspectionMode{MySql,Postgres}DumpFlowTests`; `e2e/InspectionMode*SchemaDriftInvalidationTests` |
| 5. write HW | step (4) | Same runtime `executeChunkReadInWatermarkWindow` implementations as step 3, with `window.high()` | `WatermarkSequenceException` in `WindowReconciler.ReconciliationSession#apply` at `WindowReconciler.java:290-307` (HW-before-LW, duplicate HW) | `e2e/InspectionMode{MySql,Postgres}MetadataContaminationTests` |
| 6. resume log processing | step (5) | Implicit — `RuntimeRequestPump#runUntilStopped` returns from the reconciliation branch to the sink branch. No explicit "resume" call | Same single-consumer invariant as step 1 | Same as step 1 |
| 7. forward log events normally | (reconciliation loop) | `WindowReconciler.ReconciliationSession#apply` batch loop at `WindowReconciler.java:223-240`; pre/post-window branch at `WindowReconciler.java:322-324` emits as-is | Unexpected watermark token → `WatermarkSequenceException` at `WindowReconciler.java:317-319`; metadata-table non-WATERMARK event → `WindowReconciler.java:265-269` | `WindowReconcilerTests#preservesOrderedEmissionWithoutTimeTravel`; live convergence e2e |
| 8. begin tracking collisions at LW observed | (reconciliation loop) | `WindowReconciler.java:275-288` (`lowSeen = true`; LW event not forwarded downstream) | Duplicate LW, or LW-after-HW, throws | `WindowReconcilerTests` ordering cases |
| 9. in-window non-watermark: forward + maybe remove PK | (reconciliation loop) | `WindowReconciler.java:327-337` (PK collision removal + emit) | Metadata-table non-WATERMARK event rejected at `WindowReconciler.java:265-269` | `WindowReconcilerTests#removesSnapshotRowsThatCollideWithinWindow`; `e2e/LiveMode*Convergence*` |
| 10. at HW: emit remaining, complete chunk | (reconciliation loop) | `WindowReconciler.java:290-307` (HW detect) + `WindowReconciler.java:110-128` (`emitRemainingSelectedRows`); `chunkCompleted()` accessor at `WindowReconciler.java:243` | HW-before-LW, duplicate HW, throws | `WindowReconcilerTests`; `e2e/LiveMode*AllTablesConvergenceTests` |
| Cross-cut: checkpoint-after-local-state ordering | — | `DefaultDumpWindowCoordinator#acknowledgeCompletedBatch` persists `completeChunk` before `runtime.acknowledge`; `readUntilChunkComplete` opens a `WindowReconciler.ReconciliationSession` and waits for `chunkCompleted()` | `chunkCompleted()` gate enforced by caller | `e2e/InspectionMode*RestartRecoveryTests`; `e2e/InspectionMode*TargetedRepairRecoveryTests` |
| Cross-cut: restart + stale-token drainage | — | `state/h2/H2RuntimeStateStore.java` checkpoint reload; `runtime/host/DbLogRuntimeHostLifecycle.java` resume path. Stale-run-id tokens drained silently at `WindowReconciler.java:309-315` | Schema fingerprint mismatch on resume → `FullDumpRequiredSignal`; blank run id still fails closed | `e2e/InspectionMode{MySql,Postgres}RestartRecoveryTests` |

## Existing inline citations

Five inline `paper Algorithm 1 step N` citations exist in the repository
today. When a new citation is added to code or docs, extend the matching row
above and add the file to this list:

- `runtime/sql/BoundedSourceEventQueue.java:25` — step 1
- `adapter/api/SourceRuntime.java:16` — step 1
- `adapter/api/SourceChunkReader.java:15` — step 3
- `core/request/DefaultDumpWindowCoordinator.java:111` — step 3
- `core/request/DefaultTargetedRepairCoordinator.java:96` — step 3

## Maintenance norm

- When a `paper Algorithm 1 step N` citation is added or removed in code
  or docs, update the corresponding row above and the list of existing
  citations.
- When a reconciler line range shifts (method extraction, reformat), resync
  the remaining exact `WindowReconciler.java:*` cells or replace them with a
  stable method anchor if the precise range no longer helps auditability.
- Rows are drift-guarded by the tests in the rightmost column. If a cited
  test class stops existing, update or remove the row — do not leave a dead
  reference.

## Paper-vs-implementation deltas

The step map above covers **conformance** to Algorithm 1. This section covers
the other axis — places where the repository realizes the paper differently,
modernizes it, or deliberately omits a feature the paper mentions. Each row
points at the authoritative prose; the source of truth lives there, not here.

### Realized differently, same guarantee

| Paper concept | Realization here | Source |
|---|---|---|
| Step (1) "pause log event processing" | Not a physical pause of the replication stream. The adapter stream thread keeps reading events off the wire into `BoundedSourceEventQueue`; a single-consumer invariant on the orchestrator thread diverts events through `WindowReconciler` while a window is open. TCP back-pressure kicks in if the queue fills. | [BoundedSourceEventQueue.java](../src/main/java/io/github/aandreakis/dblog/runtime/sql/BoundedSourceEventQueue.java), [SourceRuntime.java](../src/main/java/io/github/aandreakis/dblog/adapter/api/SourceRuntime.java) |
| §3.1 "we assume an LSN encoded as an 8-byte monotonically increasing number" | `ChangeEvent.sourcePosition` is an opaque `SourcePosition` interface typed per adapter (`PostgresLsn`, `MySqlSourcePosition`), not a `long`. Algorithm 1 needs only monotonicity + comparability within a source, which each typed record upholds via its own `Comparable` implementation. A uniform `long` cannot losslessly cover every vendor (SQL Server's 10-byte LSN, Oracle SCNs > 2^63); Debezium's connectors diverge per-source for the same reason. | [SourcePosition.java](../src/main/java/io/github/aandreakis/dblog/core/model/SourcePosition.java), [PostgresLsn.java](../src/main/java/io/github/aandreakis/dblog/adapter/postgres/PostgresLsn.java), [MySqlSourcePosition.java](../src/main/java/io/github/aandreakis/dblog/adapter/mysql/MySqlSourcePosition.java) |

### Modernizations

| Paper choice | Choice here | Why | Source |
|---|---|---|---|
| PostgreSQL logical decoding via `wal2json` (paper, 2019) | `pgoutput` (PostgreSQL 10+ standard) | Ships with every supported PG release (14–18); no external shared library or install step; standard PostgreSQL logical-replication output plugin; binary protocol decoded directly by pgJDBC. Semantically equivalent — both deliver committed-order transaction streams. | [adapters/postgres.md](adapters/postgres.md#why-pgoutput-rather-than-wal2json) |

### Deliberately omitted

| Paper calls for | Shipped here | Compensating lever | Source |
|---|---|---|---|
| Active/passive HA coordinated by Zookeeper | Single-process runtime. `SourceOwnershipRepository` records a single-owner `sourceId`; the default H2 state store holds an exclusive file lock. No leases, fence tokens, or automated takeover. | External single-instance supervisor — Kubernetes `replicas: 1`, systemd `Restart=always`, Nomad single-count, Docker Swarm / ECS single-replica. Optional lease-elector sidecar for hot failover. | [README.md](../README.md), [OPERATION.md §2.5](OPERATION.md#25-deployment-posture-and-high-availability) |
| Chunk-SELECT throttling for large-table dumps against hot sources | No runtime rate limiter. No `chunksPerSecond`, no `rowsPerSecond`, no adaptive source-feedback throttling. | `chunkSize` tuning plus stop/start the DBLog process. Chunk-level progress is persisted at batch boundaries, so restart resumes from the last completed chunk without source-side re-reading. See [CONTROL_PLANE.md §8](CONTROL_PLANE.md#8-pausing-and-resuming-dblog). | [README.md](../README.md), [OPERATION.md §2.4.1](OPERATION.md#241-throttling-dump-chunk-reads) |
| Exactly-once delivery | At-least-once against durable sinks. | JDBC sink is idempotent via primary-key upsert, so duplicates are absorbed at the cost of redundant writes. NDJSON sink is not idempotent — downstream consumers must handle deduplication. | [JdbcApplyChangeEventSink.java](../src/main/java/io/github/aandreakis/dblog/sink/jdbc/JdbcApplyChangeEventSink.java), [NdjsonChangeEventSink.java](../src/main/java/io/github/aandreakis/dblog/sink/ndjson/NdjsonChangeEventSink.java), [OPERATION.md §4.3.6](OPERATION.md#436-replay-and-idempotency-model) |
| Durable DDL history / schema replay across offline periods | Fail-closed. Non-additive schema changes that occur while DBLog is offline fail startup when detected during contract bootstrap, or produce `FullDumpRequiredSignal` on the next affected request. | Operator triggers a fresh dump for affected tables. No schema-history topic. | [README.md](../README.md), [adapters/mysql.md](adapters/mysql.md), [adapters/postgres.md](adapters/postgres.md) |

When code changes touch a behaviour indexed above, update the authoritative
source comment/Javadoc or linked operator/adapter document first; the one-line
summary here can then be resynced if the delta shifts.
