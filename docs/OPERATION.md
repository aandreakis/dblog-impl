# Operation

This document is the practical runbook for operating the DBLog paper
implementation in this repository.

If you are trying to get one real sync run working quickly, start with one of
the canonical demo scripts under `scripts/demo/` (see §7), then use this file
as the deeper operator guide.

Use this file for:

- choosing a boot mode,
- wiring runtime properties,
- enabling sinks and target apply,
- enabling the local control plane,
- interpreting health and schema-issue state,
- deciding what to do when DBLog fails closed.

For semantic guarantees and fail-closed boundaries, use `README.md`, §6.2 in
this file, plus the Javadocs on the key source types under `src/main/java`
(`TableSchema`, `WindowReconciler`, `DefaultDumpWindowCoordinator`,
`DumpTableProgress`, `CheckpointFlushPolicy`, and `ControlPlaneCommandService`).
For a compact supported-source summary, see `README.md`.
For control-plane semantics and payload guidance, use `docs/CONTROL_PLANE.md`.

## 1. Choose a boot mode

Current boot modes:

| Mode | Purpose | Typical use |
| --- | --- | --- |
| `runtime` | Long-running replication host | Local demos and real replication runs |
| `scenario` | Proof/fault-injection harness | Verification and adversarial testing |
| `startup-check` | One-shot startup validation | Smoke checks and preflight-only runs |

For normal replication, use:

```properties
dblog.boot-mode=runtime
```

## 2. Minimal runtime shape

Every runtime deployment needs:

```properties
dblog.boot-mode=runtime
dblog.runtime.state-path=/path/to/state
dblog.source.adapter=mysql|postgres
dblog.source.id=your_source_id
dblog.source.tables[0]=schema_or_database.table
```

**Note on `state-path`:** this is the H2 database-file prefix, not a directory.
Given `dblog.runtime.state-path=/path/to/state`, H2 writes the data file at
`/path/to/state.mv.db` and the trace file at `/path/to/state.trace.db`. To
reset state between runs, remove `/path/to/state*`. Running `rm -rf /path/to/state`
(treating it as a directory) leaves the real state file behind. The same
shape applies to `dblog.scenario.state-path`.

Then add the adapter-specific source properties.
DBLog also requires at least one explicit sink configuration for `runtime` and
`startup-check`. It does not auto-install an implicit discard sink.

### 2.0 JVM defaults for the packaged container path

The repo `Dockerfile` now sets a production-oriented default `JAVA_TOOL_OPTIONS`
for the packaged container runtime:

- `-XX:+UseG1GC`
- `-XX:InitialRAMPercentage=25.0`
- `-XX:MaxRAMPercentage=65.0`
- `-XX:+HeapDumpOnOutOfMemoryError`
- `-XX:HeapDumpPath=/var/lib/dblog/state/heapdump.hprof`
- `-XX:+ExitOnOutOfMemoryError`
- `-Xlog:gc*:stdout:time,uptime,level,tags`
- `-XX:StartFlightRecording=filename=/var/lib/dblog/state/runtime.jfr,settings=profile,maxage=30m,maxsize=256m,dumponexit=true`

Practical effect for the shipped packaged example:

- heap sizing follows container memory instead of a hard-coded `-Xmx`,
- GC logs go to stdout/stderr with timestamps,
- OOM exits fail fast and produce a heap dump under the mounted state path,
- a rolling profile JFR is written to `/var/lib/dblog/state/runtime.jfr`.

The host-run `./gradlew bootRun` examples still use the host JVM defaults unless
you export your own `JAVA_TOOL_OPTIONS` or run `java -jar` with explicit flags.

### 2.1 MySQL runtime properties

Canonical MySQL source properties:

```properties
dblog.source.adapter=mysql
dblog.source.id=my_mysql_source
dblog.source.tables[0]=app.sample_orders
dblog.source.mysql.jdbc-url=jdbc:mysql://host:3306/app
dblog.source.mysql.username=dblog
dblog.source.mysql.password=dblog
dblog.source.mysql.hostname=host
dblog.source.mysql.port=3306
dblog.source.mysql.server-id=223401
```

Optional MySQL runtime tuning:

- `dblog.source.mysql.connect-timeout`
- `dblog.source.mysql.source-event-queue-capacity`
- `dblog.source.mysql.retry-log-connection-loss`
- `dblog.source.mysql.reconnect-backoff`

DBLog ships no typed TLS configuration. See `docs/adapters/mysql.md` for the full picture.
In short, the binlog client cannot negotiate TLS, so a server with
`require_secure_transport=ON` is unsupported by the shipped DBLog.

### 2.2 PostgreSQL runtime properties

Canonical PostgreSQL source properties:

```properties
dblog.source.adapter=postgres
dblog.source.id=my_postgres_source
dblog.source.tables[0]=demo.sample_orders
dblog.source.postgres.jdbc-url=jdbc:postgresql://host:5432/app
dblog.source.postgres.replication-jdbc-url=jdbc:postgresql://host:5432/app
dblog.source.postgres.database-name=app
dblog.source.postgres.username=dblog
dblog.source.postgres.password=dblog
dblog.source.postgres.publication-name=my_pub
dblog.source.postgres.slot-name=my_slot
```

Use a dedicated runtime role rather than the `postgres` superuser. With the
default DBLog-managed publication and slot settings, that role needs logical
replication privileges, write access to `dblog_meta`, and ownership of every
captured table so PostgreSQL permits DBLog to create or repair the explicit
publication. One-time setup, fixture reset, and emergency cleanup can still use
an administrator connection out of band.

Optional PostgreSQL runtime tuning:

- `dblog.source.postgres.publication-ownership`
- `dblog.source.postgres.slot-ownership`
- `dblog.source.postgres.status-interval`
- `dblog.source.postgres.retry-log-connection-loss`
- `dblog.source.postgres.reconnect-backoff`

DBLog ships no typed TLS configuration. If you need TLS, append the standard pgJDBC
parameters (`sslmode`, `sslrootcert`, `sslcert`, `sslkey`) to both `jdbc-url` and
`replication-jdbc-url` directly (see `docs/adapters/postgres.md`).

## 2.3 What a minimal successful startup looks like

For a healthy runtime startup, expect all of the following:

- the process starts with `dblog.boot-mode=runtime`,
- the configured state path is created or reused successfully,
- source preflight passes for the chosen adapter,
- the runtime logs show the application has started,
- if the control plane is enabled, `GET /api/v1/runtime` becomes reachable,
- `GET /api/v1/runtime/health` reports `UP`,
- `GET /api/v1/runtime/status` shows a healthy source component and, if
  enabled, a healthy sink component.

Fail-fast startup boundary:

- the embedded H2 state-store schema is treated as fixed for the current repo
  build. DBLog does not perform in-place H2 schema migration or version
  negotiation.

## 2.4 Full-table dump scope note

For `TABLE` and `ALL_TABLES` dump requests, the current runtime captures the
table's maximum primary key before the first chunk of that table is coordinated.

Practical effect:

- later chunk reads stop at that captured upper bound,
- rows inserted above that bound are left to the live log path rather than being
  pulled into the in-flight full-table scan,
- the table scan finishes when no more rows remain inside that captured
  request-scoped primary-key frontier.

## 2.4.1 Throttling dump chunk reads

The DBLog paper names chunk-selection throttling as a production requirement
for large-table dumps against hot sources. This implementation exposes
two levers for that today and **does not ship a runtime rate limiter**:

- `chunkSize`: rows per chunk SELECT. Lower values reduce per-SELECT load on
  the source primary-key index, while higher values reduce coordination overhead.
  Tune per source, per table family.
- Process-level start / stop. Killing the DBLog process is safe: chunk-level
  progress is persisted at batch boundaries, so a restart resumes `ACTIVE`
  requests from the last completed chunk without source-side re-reading
  (see `docs/CONTROL_PLANE.md` §8).

Not present (deliberate):

- `chunksPerSecond` / `rowsPerSecond` config-level rate limiter,
- adaptive throttling driven by source CPU / IO feedback,
- inter-chunk delay knob,
- operator pause / resume / cancel endpoints on running requests.

If you need sustained throttling against a hot source, combine a smaller
`chunkSize` with process restarts (stop the DBLog process during peak hours,
start it again off-peak). A future configuration-level rate limiter would be a
design decision beyond the current scope of this implementation.

## 2.4.2 Heap sizing for large committed transactions

Both live-runtime adapters buffer the full committed transaction in heap before
handing it to the sink in a single `sink.appendEvents` call. The PostgreSQL
pgoutput session accumulates events in `PostgresTransactionStreamingSession`'s
internal transaction buffer until the `COMMIT` message arrives, whereas the MySQL
binlog session does the same between `BEGIN` / `Gtid` and `Xid` / `COMMIT`.

This keeps commit-boundary semantics intact at the sink (downstream target apply
sees whole transactions, never partial) but caps scalability at
**largest-single-committed-tx × retained-heap-per-event**.

### 2.4.2.1 Observed retained footprint

Empirical measurements against the PostgreSQL adapter on a typical four-column
`(id BIGINT, name TEXT, status TEXT, updated_at TIMESTAMPTZ)` schema:

| single-tx rows | observed peak heap | converge time |
| --- | --- | --- |
| 50 000 | ~430 MB | 3 s |
| 200 000 | ~855 MB | 6 s |
| 1 000 000 | ~4.0 GB | 45 s |

That works out to roughly **4 KB of retained heap per in-flight `ChangeEvent`**
under G1 with default settings. Wider rows (long strings, large blobs) push
this up, and narrower rows bring it down. Treat 4 KB/event as an order-of-magnitude
planning figure, not a guarantee.

### 2.4.2.2 Sizing rule of thumb

Size `-Xmx` so the planned largest-committed-tx can fit with a safety margin
for GC young-generation headroom:

```
-Xmx >= largest_expected_tx_events * 4 KB * 1.5
```

For a workload where the largest single transaction is 500 000 rows, that's
about 3 GB. For 1 M rows, about 6 GB.

### 2.4.2.3 Failure mode when the cap is exceeded

Today there is no fail-closed configurable cap. A transaction large enough to
exhaust the configured heap produces a generic
`java.lang.OutOfMemoryError: Java heap space` inside the pgoutput decode or
binlog decode path, the JVM logs the stack, and the runtime exits. Durable
state persisted before the oversized transaction remains intact (checkpoint,
dump progress, request status), so a restart on a larger heap (or against a
source whose oversized workload has completed) resumes cleanly.

If your workload includes occasional very large transactions (bulk inserts,
`DELETE FROM ...`, bulk `UPDATE`), either size `-Xmx` for the worst case or
route those through application-level chunking at the source.

## 2.5 Deployment posture and high availability

This implementation is designed as a **single-process CDC
runtime**. It is not a clustered, multi-instance, leader-elected system, and it
does not ship a lease, fence token, or takeover protocol. The DBLog paper
describes an active-passive deployment coordinated by Zookeeper. This
implementation intentionally stays within a smaller single-process scope.

What the code does provide:

- `SourceOwnershipRepository` records a single owning `sourceId` in the local
  state store on first claim. A second process started with a **different**
  `sourceId` pointing at the same state store crashes at startup with
  `IllegalStateException`.
- The default H2-backed state store file is held with an exclusive file lock,
  so two processes on the same host cannot open the same state store
  concurrently. A second process fails to start rather than silently corrupting
  state.

What the code does **not** provide:

- No liveness detection. If the owning process dies without shutdown, the
  owning-source-id row sits forever. A replacement process with the *same*
  `sourceId` can restart and resume, but there is no automated takeover.
- No lease / fence-token protection. If two processes somehow both connect to
  the same upstream (e.g. a DBA-induced state-store copy, or the H2 file lock
  is defeated), each will independently read the source replication stream and
  emit events. Downstream sinks that are not idempotent will see duplicates.
- No passive-standby process. Any standby you run is a fresh cold start, not a
  hot-failover replica.

### Recommended operational patterns

For production-like deployments that need resilience, pair DBLog with an
external single-instance supervisor. Representative choices:

- **Kubernetes**: a `Deployment` with `replicas: 1` and a `RollingUpdate`
  strategy (or a `StatefulSet` if you want stable network identity plus a
  PersistentVolume for the H2 file). Let the pod controller handle restart. If
  you need hot failover, front it with a k8s lease-based leader-elector
  sidecar that holds a lease external to DBLog and only starts the DBLog
  process while it holds the lease.
- **systemd**: a `Restart=always` unit with `StartLimitBurst=`. Pair with
  `RuntimeDirectory=` for the state store.
- **Nomad**: a single-count job with `restart { attempts = ... }`.
- **Docker Swarm / ECS**: single-replica service with a restart policy.

If the supervisor needs to discover the OS-assigned control-plane port (e.g. when
`dblog.control-plane.port=0`), use `dblog.control-plane.port-file` (see §5.1).

Two instances accidentally pointed at the same source with the same
`sourceId` produces duplicate events on the sink. The JDBC apply sink is
idempotent via upsert-on-primary-key and will absorb duplicates at the cost of
redundant writes. The NDJSON sink is **not** idempotent and will emit both
copies to the output file. If your pipeline downstream of DBLog is not
idempotent, treat "single live instance per sourceId" as a hard operational
invariant enforced by the supervisor.

### Implementing HA in-process later

Adding real HA to DBLog would require, at minimum: a lease table with TTL and
monotonic fence tokens, fence-token checks on every state-store write, a
renewal thread, a graceful release on shutdown, and tests for expiry, takeover,
and stale-fence rejection. This is a meaningful project, not a small patch. If
it lands, it will be behind a configuration flag and the single-process
posture above will remain the default.

## 3. Source prerequisites

### 3.1 PostgreSQL

Current PostgreSQL runtime requirements:

- captured tables must use `REPLICA IDENTITY FULL`
- the configured publication/slot contract must be valid for the captured set
- the metadata tables must be writable and visible on the same logical stream
- the runtime role must satisfy the privilege and ownership contract documented
  in `docs/adapters/postgres.md`

**Slot lifecycle is an operator responsibility.** DBLog creates the logical
replication slot on first run, reuses it across restarts, and never drops it,
matching Debezium's default posture. A decommissioned DBLog process leaves
its slot active on the source. Without cleanup the server accumulates WAL
indefinitely and will eventually exhaust disk. The `max_slot_wal_keep_size`
server setting (PG13+) is the recommended safety net. For cleanup procedures,
health monitoring queries, and the relationship between slot health and the
runtime's slot-feedback WARN log, see the slot lifecycle section in
`docs/adapters/postgres.md`.

### 3.2 MySQL

Current MySQL runtime requirements:

- binary logging enabled
- `binlog_format=ROW`
- `binlog_row_image=FULL`
- one actual MySQL database per runtime request
- a unique replication `serverId`

## 4. Sinks and target apply

### 4.1 NDJSON

Enable NDJSON to stdout:

```properties
dblog.sink.ndjson.stdout=true
```

Enable NDJSON to file:

```properties
dblog.sink.ndjson.path=/path/to/events.ndjson
```

#### 4.1.1 Durability characteristics (not production-grade)

The NDJSON sink is intended for **debugging, CDC stream inspection, and local
demos**, not production event delivery. Its durability guarantees are
deliberately thin, and operators who treat the output file as an audit log
will be surprised.

What DBLog does on write:

- Appends each encoded event followed by `\n` into a `BufferedWriter`.
- Calls `BufferedWriter.flush()` at the end of every batch. This pushes bytes
  from the JVM buffer into the operating-system buffer. It does **not** call
  `fsync` on the file descriptor.

What this means in practice:

- **JVM crash / kill -9 mid-batch**: any events already written in the current
  batch that have not yet been flushed are lost. Events from prior
  batches that were flushed are held by the OS buffer but may still not be on
  stable storage.
- **Kernel crash / power loss**: any OS-buffered events are lost, even if the
  JVM flushed them. DBLog never fsyncs.
- **Process restart**: the sink opens the target file with
  `StandardOpenOption.APPEND`. Prior content is preserved, and new events are
  appended on top. A restart after a crash therefore replays any events that
  were not acknowledged at the source, producing duplicate lines for already-
  persisted events. This is the documented at-least-once behavior. Downstream
  consumers are responsible for deduplication if needed.
- **Long-running processes**: the file grows without bound. There is no
  rotation, size cap, or retention policy. Operators should pipe through
  `logrotate` externally or point the sink at a path they rotate.
- **Disk full**: writes begin to throw `IOException`, which propagates as a
  runtime failure. There is no back-pressure to the source.

The NDJSON sink also does not provide:

- atomic file replacement (no write-to-temp + rename on success),
- idempotency (replays after recovery produce duplicate lines on any observer
  who reads the file),
- ordering guarantees across sink restarts (events from the replay window
  overlap with events from before the crash),
- schema-change tracking in the output header.

For production workloads use the JDBC apply sink (see §4.3), which provides
idempotent upsert semantics, or the typed H2 inspection sink (see §4.2) for
structured local inspection with proper relational semantics.

### 4.2 Typed H2 sink

Enable the strict typed H2 inspection sink:

```properties
dblog.sink.typed-h2.path=/path/to/typed-events
```

### 4.3 JDBC target apply

Enable JDBC target apply:

```properties
dblog.target.enabled=true
dblog.target.dialect=POSTGRES|MYSQL
dblog.target.jdbc-url=jdbc:...
dblog.target.username=...
dblog.target.password=...
dblog.target.maximum-pool-size=4
```

Optional target tuning:

- `dblog.target.connection-timeout`
- `dblog.target.retry-backoff`
- `dblog.target.table-mappings[]`

#### 4.3.1 Supported targets

Current target dialects:

- PostgreSQL
- MySQL

DBLog does **not** currently claim support for arbitrary JDBC databases.

#### 4.3.2 Target table contract

- target tables must already exist,
- target table names/namespaces must match incoming event identity unless
  explicit table mappings are configured,
- target apply supports single-column and composite primary keys,
- the sink uses the ordered primary-key columns from DBLog's neutral model.

#### 4.3.3 Operation mapping

Current operation mapping is intentionally modest:

- `INSERT` and `UPDATE` become target upsert operations,
- `DELETE` becomes a target delete keyed only by the ordered primary-key values,
- internal watermark/heartbeat events are not target-applied as user-table work.

#### 4.3.4 Upsert and delete semantics

For upsert:

- primary-key columns are always taken from `event.primaryKey`,
- non-primary-key columns come from `event.afterRow`,
- omitted non-primary-key columns are omitted from generated SQL,
- on insert, omitted columns therefore fall back to target defaults / `NULL`,
- on conflict update, omitted columns are left unchanged.

For delete:

- delete uses only the ordered primary-key values,
- deleting an absent target row is treated as a successful no-op.

#### 4.3.5 Batch and transaction boundary

One `appendEvents(List<ChangeEvent>)` call is applied inside one target JDBC
transaction:

- auto-commit is disabled for that apply transaction,
- every applicable event in the batch is translated to target SQL,
- if all statements succeed, the batch is committed once,
- if any statement fails, the whole target batch is rolled back.

#### 4.3.6 Replay and idempotency model

The current delivery model remains **at-least-once**.

The sink is designed to tolerate replay in the specific shape that DBLog
currently produces:

- unacknowledged source work may be replayed after restart or recovery,
- replay of the same upserted row state converges because apply uses upsert,
- replay of an already-applied delete converges because delete is a no-op when
  the row is absent.

This is **not** a generic claim that arbitrary out-of-order stale event replay
is always safe.

#### 4.3.7 Failure and recovery

If target apply fails before source acknowledgement:

- the target batch is rolled back,
- the source checkpoint is not durably advanced through that failed batch,
- DBLog may replay the same source events later.

Transient target availability failures are retried indefinitely with bounded
per-attempt connection timeout plus configured backoff. Target contract breaches
such as auth failure, privilege failure, or missing target objects fail closed
instead of retrying forever. If a JDBC batch reports both target contract
evidence and later availability-looking follow-on errors, DBLog treats the
batch as a hard target contract failure.

#### 4.3.8 Cross-engine routing (MySQL source ↔ Postgres target)

When source and target engines differ, source `TableId` shapes may not map
directly to target-side schemas. MySQL events carry the database name in the
`schemaName` position (MySQL has no analog of Postgres schemas), so the
default identity resolver sends a MySQL event from database `app` to target
`"app"."<table>"`. This works only if the Postgres target happens to have a
schema literally named after the MySQL database.

For cross-engine flows, configure explicit table mappings:

```properties
# Example: MySQL db=app table=orders → Postgres schema=public table=orders
dblog.target.table-mappings[0].source-schema=app
dblog.target.table-mappings[0].source-table=orders
dblog.target.table-mappings[0].target-schema=public
dblog.target.table-mappings[0].target-table=orders
```

If a source table does not resolve to an existing target table, DBLog fails
closed at preflight with a `missingTargetTable` error. The error message names
the expected target identity and hints at this `dblog.target.table-mappings`
configuration.

#### 4.3.9 Cache growth model

The JDBC-apply sink caches target-table metadata, compiled statement plans,
and prepared-statement factories keyed by source `TableId` and statement
shape. The caches grow with `table_count × distinct_column_shape_count ×
statement_kinds` and do not evict. For typical replication flows (a
fixed set of tables with stable column shapes) the caches stay small. For
sources with many tables or highly heterogeneous column shapes across events,
the caches grow without bound: operators who anticipate high cardinality
should budget heap accordingly. This implementation does not bound
these caches.

### 4.4 Explicit no-op sink

If you intentionally want DBLog to discard emitted events, configure the
explicit no-op sink:

```properties
dblog.sink.noop.enabled=true
```

This is the only discard path. If no sink is configured, `runtime` and
`startup-check` now fail fast instead of silently dropping data.

## 5. Control plane

### 5.1 Enable the control plane

```properties
dblog.control-plane.enabled=true
dblog.control-plane.host=127.0.0.1
dblog.control-plane.port=8085
```

Use the loopback host for host-run deployments. The packaged Docker example is
the only different bind mechanic in the shipped assets: DBLog listens on
`0.0.0.0` inside the container, but `ops/docker/compose.runtime.yml` publishes
the host port on `127.0.0.1:8085` only so operator access remains local.

For supervisor integration (k8s sidecars, systemd units, integration tests) where
the operator wants the OS to assign a free port, set `dblog.control-plane.port=0`
and point `dblog.control-plane.port-file` at a path the supervisor can read:

```properties
dblog.control-plane.port=0
dblog.control-plane.port-file=/var/run/dblog/port
```

The bound port is written to that file once the HTTP server is up. The write is
atomic (temp file + rename), so a concurrent reader cannot observe a partial
value. The file is removed on graceful shutdown. This pairs naturally with the
single-instance supervisor patterns in §2.5.

### 5.1.1 Security posture of the control plane

The shipped control plane has **no built-in authentication or authorization.**
This is intentional for this implementation and relies on
an *operator-local access model*:

- The default bind is `127.0.0.1`, which scopes reachability to the host.
- The packaged Docker compose publishes `127.0.0.1:8085` on the host only.
- Anyone with host access (local shells, co-located processes, compromised
  sidecars) can submit dump requests and read all runtime state without a
  credential.

Any deployment that binds beyond loopback is the operator's responsibility to
front with a reverse proxy (e.g. nginx, Caddy, Envoy, a cloud load balancer)
that enforces:

1. TLS termination,
2. authentication (bearer token, mTLS, OIDC, or organizational standard),
3. optional authorization policy per route (e.g. read-only `GET` access for
   monitoring systems, admin access for request mutation endpoints).

Do **not** expose the control plane directly on an untrusted network. The
runtime writes control-plane state (dump request lifecycle, schema drift
signals) to the local state store, and a malicious caller with unfettered
access can trivially submit arbitrary dump requests or induce a full re-dump
via the primary-key-drift signal path.

If a future release adds in-process auth, it will be opt-in via a property and
the loopback bind will remain the default posture. Until then, the shipped
control plane is designed for local operator tools (`curl` on the same host,
the shipped demo scripts) and nothing further.

Useful optional properties:

- `dblog.control-plane.executor-max-threads`
- `dblog.control-plane.executor-queue-capacity`
- `dblog.control-plane.max-request-body-bytes`

Current important endpoints:

- `GET /api/v1/runtime`
- `GET /api/v1/runtime/health`
- `GET /api/v1/runtime/status`
- `GET /api/v1/runtime/schemas`
- `GET /api/v1/runtime/schema-issues`
- `GET /api/v1/requests`
- `POST /api/v1/requests`
- `GET /api/v1/requests/{requestId}`
- `GET /api/v1/metrics`
- `GET /api/v1/events/summary`

Current backpressure visibility:

- MySQL now reads source-log events through a bounded in-process queue
  and pauses source fetching when that queue is full instead of failing closed or
  dropping events,
- PostgreSQL remains direct-poll rather than prefetch-queue based, so sink
  pressure slows later source reads without an intermediate queue inside DBLog,
- `runtime`, `runtime/status`, and `metrics` now expose source flow-control
  state such as queue depth/capacity, whether source fetching is paused, and
  whether the current pressure diagnosis looks like sink unavailability or sink
  slowdown under load.

### 5.1.2 Educational tap

> **Never enable the tap in production.** The tap deliberately blocks the
> DBLog pump thread whenever a subscriber cannot keep up: that is the
> feature's entire purpose and the reason it exists only as a teaching
> artefact. A slow subscriber will stall CDC for the whole runtime.

```properties
dblog.tap.enabled=false
# dblog.tap.queue-capacity=65536
# dblog.tap.standby-threshold-ms=1000
# dblog.tap.heartbeat-interval=2s
```

When enabled, the tap mounts `GET /api/v1/tap/stream` (chunked NDJSON,
one subscriber at a time) on the control plane. The TUI reads events
from this endpoint. The underlying TCP flow control is what stalls the
pump when the subscriber can't keep up (step-mode). The tap route is
only reachable when the control plane itself is enabled.

The startup WARN means only that the educational tap is enabled. Actual
tap-induced pump blocking is surfaced separately:

- server logs emit one `DBLog tap queue is full ...` WARN per tap queue
  lifetime when the queue first fills and the pump blocks,
- the tap stream emits `stream.standby` after
  `dblog.tap.standby-threshold-ms`, then `stream.resumed` after the
  HTTP writer catches up,
- tap `stream.heartbeat` events include `queue_depth` and
  `queue_capacity` for the tap queue.

Do not use `/api/v1/runtime/status` `sourceFlowControl.queueDepth` as tap
queue health. That field describes source-log flow control, not the tap
HTTP stream queue.

### 5.2 Request submission requirements

HTTP request submission currently requires:

- the process to report request submission as available,
- an H2 driver on the runtime path,
- one resolved state path from:
  - `dblog.runtime.state-path`
  - `dblog.scenario.state-path`

Operational response expectations:

- unknown request ids return `404 not_found`,
- invalid `PRIMARY_KEYS` literal content that is rejected during submission-time
  schema-aware canonicalization returns `400 bad_request`,
- invalid `PRIMARY_KEYS` literal content that is only discovered later during
  runtime binding marks that accepted request `FAILED` (the DBLog runtime
  itself continues operating),
- `TABLE` or `PRIMARY_KEYS` requests that target a table outside the current
  captured-schema set are marked `FAILED` (the DBLog runtime itself continues
  operating),
- missing state-path / embedded state-store wiring returns service unavailable.

### 5.3 Lifecycle summary and kill-safe restart

Current request lifecycle summary:

- new submissions start as `QUEUED`,
- the runtime transitions work to `ACTIVE`,
- completed work lands in terminal `COMPLETED`,
- work that cannot be completed (schema drift, missing table, unsupported PK
  type) lands in terminal `FAILED`.

The control plane does not ship operator pause / resume / cancel endpoints.
To pause ingest, stop the DBLog process. To resume, start it again. The
embedded state store persists chunk-level progress at batch boundaries, so a
restart resumes `ACTIVE` requests from the last completed chunk without
re-reading from the source. `COMPLETED` and `FAILED` requests are unaffected
by restart. See [CONTROL_PLANE.md §8](CONTROL_PLANE.md#8-pausing-and-resuming-dblog) for the detailed
kill-safety contract.

## 6. Interpret runtime state

### 6.1 Healthy runtime

At a glance:

- `GET /api/v1/runtime/health` should report `UP`
- `GET /api/v1/runtime/status` should normally show healthy source and sink
  components
- recent logs and metrics should continue moving forward

### 6.2 Schema contract and DDL handling

DBLog does not implement online schema evolution as a supported feature. The
runtime treats schema as a selected-column contract:

- startup inspects each captured source table and stores the observed schema,
- the first successful startup stores the contract schema,
- later startups reconcile the live schema against the stored contract,
- dump and repair chunks must keep the same selected-column fingerprint,
- live stream adapters decode by the selected contract where the source protocol
  gives enough metadata,
- when continuity is uncertain, DBLog records a signal and fails closed rather
  than widening the emitted row shape silently.

The selected contract is the set of columns DBLog emits and fingerprints.
Columns outside that surface are ignored: they do not appear in emitted rows and
do not change the selected-column fingerprint. An extra supported column added
after the contract exists is therefore treated like an ignored column if DBLog
encounters it at a safe inspection boundary.

Startup and restart reconciliation:

| Source shape at startup | Result |
| --- | --- |
| no stored contract | live schema becomes contract |
| extra non-PK column | accepted as ignored |
| selected column removed | fail closed |
| selected type/source/nullability drift | fail closed |
| primary-key shape drift | fail closed |

Live stream behavior differs by adapter because the source protocols expose
different metadata:

| Live condition | MySQL | PostgreSQL |
| --- | --- | --- |
| captured-table `ADD COLUMN` | fail closed | may decode if unselected |
| extra row metadata column | ignored by contract | ignored by contract |
| missing selected column | fail closed | fail closed |
| selected type drift | fail via DDL path | not an OID guard |
| `TRUNCATE` | fail closed | fail closed |
| live PK update | fail closed | fail closed |
| key-only old tuple | n/a | fail closed |
| replica identity drift | n/a | fail closed |

Important details for code reviewers and AI agents:

- MySQL `Query` events expose raw SQL plus a default database, not a structured
  target table id. DBLog therefore fails closed on row-state- or
  schema-affecting DDL in a captured database, including additive `ADD COLUMN`.
- The lower MySQL row decoder can ignore extra `TABLE_MAP` columns when no DDL
  query has stopped the stream first, but online MySQL DDL is still unsupported.
- PostgreSQL `pgoutput` relation messages are structured. The live decoder maps
  tuple values by selected column name, so added unselected relation columns can
  be tolerated mechanically.
- PostgreSQL relation messages carry type OIDs, but the live runtime does not use
  those OIDs as a selected-column type-drift detector. Startup/restart schema
  inspection is the supported point that catches selected type/source/nullability
  drift.
- A tolerated adapter mechanism is not a promise that operators can perform
  online schema evolution. Stop DBLog, make the source change, verify startup
  reconciliation, and submit a fresh full dump when the selected contract changed
  or correctness is uncertain.

Useful code and test anchors for review:

- `core/schema/SchemaPolicyEngine.java`
- `runtime/bootstrap/RelationalRuntimeBootstrap.java`
- `adapter/mysql/internal/MySqlBinlogSession.java`
- `adapter/postgres/internal/PostgresTransactionStreamingSession.java`
- `adapter/mysql/internal/MySqlBinlogSessionTests.java`
- `adapter/postgres/internal/PostgresTransactionStreamingSessionTests.java`

`GET /api/v1/runtime/schemas` reports per-table `schemaStatus` values:

| Status | Meaning |
| --- | --- |
| `OK` | no persisted schema signal for that table |
| `SCHEMA_UNCERTAIN` | schema/metadata continuity is suspect |
| `FULL_DUMP_REQUIRED` | operator action and fresh dump required |

`GET /api/v1/runtime/schema-issues` returns the raw signal lists:

- `fullDumpRequiredSignals`
- `schemaUncertaintySignals`

`FULL_DUMP_REQUIRED` wins over `SCHEMA_UNCERTAIN` for the per-table status.

### 6.3 Full-dump-required

If DBLog reports `FULL_DUMP_REQUIRED`:

1. inspect the reported reason
2. fix the underlying source issue if needed
3. submit a fresh `ALL_TABLES` dump when the source is trustworthy again

Current invalidation behavior:

- PK-drift / full-dump-required invalidation retires stale queued and active
  dump requests as `FAILED`,
- DBLog does not silently put old `TABLE` / `ALL_TABLES` requests back into the
  queue after that invalidation boundary,
- full-dump-required signals for that invalidation stay keyed under the
  configured runtime `sourceId`,
- operator recovery should use a fresh submitted request rather than resuming a
  retired pre-drift request.

Typical triggers:

- purged upstream history
- unresolved schema continuity after DDL
- primary-key drift that invalidated in-flight progress
- live primary-key update on a captured table
- PostgreSQL key-only or missing old tuple for a captured-table update/delete

MySQL-specific limitation: the live binlog path does not parse DDL. MySQL Query
events carry a default database and raw SQL text, so row-state- or
schema-affecting DDL in the captured database can fail closed even when it
targets an uncaptured table. Treat that as a conservative false positive: verify
the source and target state, then use the same fresh full-dump recovery path.

### 6.4 Schema uncertainty

If DBLog reports schema uncertainty without a full-dump-required state:

1. monitor the runtime and logs
2. verify whether the adapter can recover once trustworthy schema evidence
   appears
3. treat repeated or escalating uncertainty as a candidate for a fresh full
   dump

Typical triggers include malformed DBLog metadata rows, unexpected singleton
metadata behavior, or another DBLog run writing heartbeats on the same source
stream after this runtime has confirmed its own heartbeat.

## 7. Canonical demos and packaged example

Current canonical local demos:

- `scripts/demo/mysql_to_ndjson.py`
- `scripts/demo/mysql_to_postgres.py`
- `scripts/demo/postgres_to_mysql.py`

Packaged Docker example:

- `ops/docker/README.md`
- `ops/docker/compose.runtime.yml`
- `ops/docker/examples/mysql-to-postgres/application.properties`

These are the best starting points if you want a real property set instead of
inventing one from scratch. The packaged example still keeps the operator-facing
control plane local by publishing `127.0.0.1:8085` on the host. When you
recreate the fixture databases from scratch, also clear the bind-mounted example
runtime state under `ops/docker/example-state/mysql-to-postgres/` before the
next packaged start so DBLog does not resume from a stale example checkpoint
against a fresh source history.

The canonical demos and local host-run examples enable source reconnect retry
for transient availability failures. Contract and correctness failures, such as
unsupported DDL or incompatible schema changes, still fail closed by design.
For unsupported live DDL, restart alone is not recovery: the same persisted
state/checkpoint can replay the DDL before a fresh dump can be submitted. When
the state store is available, destructive live DDL, truncate, selected-column
source metadata drift, selected-column row tuple drift, and live primary-key
update paths record a full-dump-required signal before failing closed. Verify
or rebootstrap the target, clear or replace the affected runtime state files,
then restart and submit a fresh `ALL_TABLES` dump. Remember that
`dblog.runtime.state-path` is an H2 file prefix, not a directory: remove
`<state-path>.mv.db` and any `<state-path>.trace.db` / `<state-path>.lock.db`
files, or use a new state path.

For the packaged container path, do not stop at `runtime/status`: use the
packaged proof flow in [ops/docker/README.md](../ops/docker/README.md#packaged-example-deployment).
It seeds MySQL, starts the packaged runtime, submits `ALL_TABLES`, verifies
PostgreSQL convergence, applies a later live MySQL change, and verifies
PostgreSQL converges again.

## 7.1 Success criteria for the canonical examples

Use these as the practical "it is working" signals:

| Example | Startup success | Replication success |
| --- | --- | --- |
| `mysql_to_ndjson.py` | runtime starts and writes logs cleanly | NDJSON file receives live events |
| `mysql_to_postgres.py` | runtime starts and control plane is reachable | initial `ALL_TABLES` dump converges, then later live MySQL changes converge into PostgreSQL |
| `postgres_to_mysql.py` | runtime starts and control plane is reachable | initial `ALL_TABLES` dump converges, then later live PostgreSQL changes converge into MySQL |
| Packaged Docker example | container starts and control plane is reachable on `127.0.0.1:8085` | initial `ALL_TABLES` dump converges, then later live MySQL changes converge into PostgreSQL |

Good general success signals:

- request submission is available when expected,
- queued requests drain,
- no schema-issue escalation appears unless intentionally provoked,
- recent logs and metrics continue to move forward,
- source flow-control remains healthy, or if source fetching is paused because a
  bounded queue is full, the operator-facing diagnostics clearly explain whether
  the sink is unavailable or simply slower than the current inbound source
  pressure,
- target-side final row state matches the source-side intended outcome.

## 8. Verification commands

For first-contact validation, start with one local demo or one
`startup-check` run.

Quick live proof (fastest way to see data flowing end-to-end):

```bash
# macOS/Linux
python3 scripts/demo/mysql_to_postgres.py

# Windows (PowerShell / CMD)
py -3 scripts/demo/mysql_to_postgres.py
```

Other canonical demos (see §7):

```bash
# macOS/Linux
python3 scripts/demo/mysql_to_ndjson.py
python3 scripts/demo/postgres_to_mysql.py

# Windows (PowerShell / CMD)
py -3 scripts/demo/mysql_to_ndjson.py
py -3 scripts/demo/postgres_to_mysql.py
```

On macOS/Linux, the demo entrypoints are also executable, so
`./scripts/demo/mysql_to_postgres.py` and its siblings work too.

Short local verification:

`check` is the normal local developer verification task for this repo.

```bash
./gradlew check
```

Unit and in-process integration tests:

```bash
./gradlew test
./gradlew integrationTest
```

CI-style verification (includes the Docker-backed integration and e2e lanes):

```bash
./gradlew check
./gradlew integrationTestDocker
./gradlew e2eTestDocker
```

Cross-version support matrix:

```bash
./gradlew compatibilityMatrix
```

## 9. Common operator failure classes

Current practical interpretation:

- startup preflight failures usually mean source prerequisites or credentials
  are wrong
- target apply retries usually indicate transient target unavailability
- target apply hard failures usually indicate schema/privilege/contract drift
- missing or malformed metadata tables are treated as contract boundaries, not
  soft warnings
- purged source history is a fail-closed condition rather than an automatic
  resnapshot path

## 9.1 Operational hazards specific to this implementation

### 9.1.1 Upgrading the DBLog binary against an existing local state store

The local H2 state store does not carry a schema version. If a DBLog binary
upgrade changes a state-store table shape, the new binary will attempt to open
the old store and may fail mid-boot with an H2 column-shape error. This
implementation does not auto-migrate. Operators upgrading across
binary versions should **reset the local state files** for
`dblog.runtime.state-path` before starting the new binary: remove
`<state-path>.mv.db` and any `<state-path>.trace.db` / `<state-path>.lock.db`
files, or use a new state path. Then re-submit any long-running dump requests.
For a production deployment, run a versioned migration outside DBLog.

### 9.1.2 Source credentials in configuration

Source passwords (`dblog.source.mysql.password`, `dblog.source.postgres.password`,
`dblog.target.password`) are plain string properties bound by Spring's
`@ConfigurationProperties`. DBLog does **not** wrap them in a redacting
secret-value type. Two practical consequences:

- Do not enable DEBUG-level logging on Spring's property-binding packages
  (`org.springframework.boot.context.properties.bind`, `org.springframework.core.env`).
  At DEBUG, Spring may log bound property values including passwords. The
  default log level does not print them.
- The embedded Spring Boot actuator is on the classpath but is not reachable
  over HTTP in the default configuration (`spring.main.web-application-type=none`).
  If you ever enable a web application type or expose the actuator separately,
  configure `management.endpoint.env.keys-to-sanitize` to redact password
  properties.

Heap/thread dumps will contain the password strings in plaintext regardless.
For deployments that carry real secrets, consider injecting credentials from
an external secrets store rather than pinning them in
`application.properties`.

## 10. Troubleshooting

Use this table as the first-pass operator guide.

| Symptom | Likely cause | Where to look | Operator action |
| --- | --- | --- | --- |
| Startup fails before runtime begins | bad credentials, missing table, invalid source prerequisite, or selected-column schema drift against a stored contract | startup logs, implementation spec for the adapter, persisted schema issues if a state store was available | fix source config or source DB state. For contract drift, submit a fresh `ALL_TABLES` dump after the source is trustworthy |
| Control plane is enabled but request submission is unavailable | no active request-processing runtime or unresolved state path | `GET /api/v1/runtime`, request submission message | ensure the process is in the right boot mode and a valid runtime/scenario state path is configured |
| `runtime/health` is `DOWN` | runtime fail-closed boundary hit | `GET /api/v1/runtime/health`, recent logs | inspect the failure class/message, fix the underlying contract problem, then restart or rerun |
| `schemaStatus=FULL_DUMP_REQUIRED` | purged history, unresolved schema continuity, or primary-key drift | `GET /api/v1/runtime/schemas`, `GET /api/v1/runtime/schema-issues`, logs | fix the underlying issue, then submit a fresh `ALL_TABLES` dump |
| `schemaStatus=SCHEMA_UNCERTAIN` persists too long | adapter cannot obtain trustworthy schema or metadata evidence | `runtime/schemas`, `runtime/schema-issues`, recent logs | monitor first. If it does not clear, treat it as a candidate for a fresh full dump |
| Target apply keeps retrying | transient target outage or target DB not yet reachable | `runtime/status`, recent logs | restore target availability and wait for retry convergence |
| Target apply fails hard instead of retrying | target contract breach such as missing schema/table/column or PK mismatch | recent logs, target apply spec | align target schema/privileges/PK contract, then restart or retry |
| No requests drain after submission | runtime not processing requests or state-path mismatch | `GET /api/v1/requests`, `runtime/status` | confirm request submission path and active runtime state |
| Demo starts but no convergence happens | source not changing, dump not submitted, or target not aligned | demo log, control plane, target DB query | submit the expected request and verify target schema plus runtime health |

When in doubt:

1. check `runtime/health`
2. check `runtime/status`
3. check `runtime/schema-issues`
4. inspect recent logs
5. trigger a fresh `ALL_TABLES` dump only after the underlying source/target
   contract is healthy again
