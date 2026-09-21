# Control Plane

This file is the **human-readable control-plane guide** for the DBLog paper
implementation in this repository.

## 1. Purpose and scope

The control plane is a **local embedded HTTP server** inside the long-running
DBLog process. It exists to expose DBLog-owned runtime state and a modest set of
operator actions without requiring direct database inspection or log scraping.

Current shipped scope:

- runtime snapshot, health, detailed status, persisted schemas, and schema issues,
- curated metrics,
- event summary, recent events, and per-table event drill-down,
- request listing and request detail,
- request submission,
- source flow-control / backpressure visibility for the live runtime.

The control plane intentionally does not expose `pause`, `resume`, or `cancel`
endpoints. To pause ingest, stop the DBLog process. To resume, start it again.
The embedded state store is crash-safe (see §8), so a restart picks up at the
last completed chunk without re-reading data from the source.

Not part of the current shipped slice:

- remote or multi-host control,
- internet-facing authn/authz,
- durable event-history storage for the UI.

## 2. Exposure model

- the operator-facing control plane is **local-only**,
- host-run examples should bind directly to loopback (`127.0.0.1` by default),
- the packaged Docker example binds `0.0.0.0` inside the container so Docker
  port publishing works, but publishes `127.0.0.1:8085` on the host,
- remote or internet-facing exposure is out of scope,
- it is disabled by default until explicitly enabled,
- request handling now uses a bounded executor plus a bounded request-body size
  limit rather than an unbounded cached thread pool or unbounded body reads,
- it is intentionally a single versioned API rooted at `/api/v1`.

Namespaces:

- `/api/v1/runtime`
- `/api/v1/requests`
- `/api/v1/metrics`
- `/api/v1/events`

## 3. Request lifecycle semantics

Current durable request states:

- `QUEUED`
- `ACTIVE`
- `COMPLETED`
- `FAILED`

Current operator actions:

- submit

Important semantics:

- new submissions start as `QUEUED`,
- the runtime transitions work to `ACTIVE`,
- `COMPLETED` and `FAILED` are terminal,
- chunk-level progress is persisted at batch boundaries, so a process restart
  resumes `ACTIVE` work from the last completed chunk without re-reading.

### 3.1 Terminal-state retention

The state store keeps every request durably until a newer same-scope, same-table
`COMPLETED` request supersedes it. This bounds the on-disk request log without
losing useful operator history:

- when a `TABLE` or `PRIMARY_KEYS` request reaches `COMPLETED`, every other
  terminal-state (`COMPLETED` or `FAILED`) request that targets the same
  `(databaseName, schemaName, tableName)` and was submitted earlier is pruned
  from the state store atomically, including any dependent rows in the
  primary-key, missing-key, and chunk-progress tables.
- when an `ALL_TABLES` request reaches `COMPLETED`, every prior terminal-state
  `ALL_TABLES` request is pruned.
- pruning never crosses scope: a `TABLE` completion does not delete prior
  `ALL_TABLES` history, and vice versa.
- pruning is gated on `COMPLETED` only: a `FAILED` request never deletes
  history. Failures stay visible until a same-scope, same-table successful
  completion replaces them.
- the just-completed request itself is preserved, and `ACTIVE` requests are
  never touched even when their sequence number is older.

Operator implication: `GET /api/v1/requests` is not a durable audit trail of
every request ever submitted. It is the live + most-recent terminal state
keyed by scope and table. Export historical entries before the next successful
completion if you need long-term records.

## 4. Availability and state-store requirements

HTTP request submission requires:

- request submission to be available from the current runtime,
- that runtime to have started with an explicit sink configuration (DBLog does
  not auto-install an implicit discard sink),
- an H2 driver on the runtime path,
- one resolved state path from:
  - `dblog.runtime.state-path`
  - `dblog.scenario.state-path`

## 5. Endpoint families

### 5.1 Runtime

Use the runtime endpoints for:

- compact runtime summary,
- health probes,
- more detailed status,
- persisted schemas and schema issues,
- source flow-control visibility such as bounded queue depth, paused source
  fetching, and pressure diagnosis.

Relevant runtime properties for the local HTTP server include:

- `dblog.control-plane.executor-max-threads`
- `dblog.control-plane.executor-queue-capacity`
- `dblog.control-plane.max-request-body-bytes`

### 5.2 Requests

Use the request endpoints for:

- filtered request listings,
- request detail,
- new request submission.

Current HTTP shape rules:

- DBLog assigns the durable numeric `requestId` when a submission is accepted,
- `ALL_TABLES` submissions must not include `table` or `primaryKeyLiterals`,
- `TABLE` submissions must include `table` and must not include `primaryKeyLiterals`,
- `PRIMARY_KEYS` submissions must include `table` plus at least one `primaryKeyLiterals` value,
- malformed request shapes are rejected with `400 bad_request` instead of being durably queued.

For `PRIMARY_KEYS` submissions, the current control plane accepts
`primaryKeyLiterals` as strings. DBLog canonicalizes those strings against the
selected table schema before targeted-repair reads bind typed JDBC primary-key
parameters.

A valid `PRIMARY_KEYS` request can complete even when one or more requested
source rows are absent. In that case DBLog records those keys in
`missingPrimaryKeyLiterals` on the completed request.

Current literal shapes:

- single-column primary keys: plain string literals such as `42`,
- composite primary keys: canonical `{column=value,...}` strings or positional
  tuple strings such as `(tenant-a,42)`.

Composite literals reserve `\`, `,`, `=`, `{`, and `}`. Prefix a reserved
character with `\`. Write `\\` when the primary-key value contains a literal
backslash. For example, a value `north,west` is written as
`{external_id=north\,west}`, while `folder\record` is written as
`{external_id=folder\\record}`. The same escaping rules apply to positional
tuple values. A dangling final escape is malformed and is rejected with HTTP
`400 bad_request` rather than being queued. DBLog accepts both composite input
forms, but canonical output always uses the named `{column=value,...}` form.

The `table` object mirrors the internal three-part `TableId`. For PostgreSQL,
`databaseName` is the physical database name and `schemaName` is the schema
name. For MySQL, `databaseName` is the logical DBLog source id and
`schemaName` is the physical source database name. `tableName` remains the
physical table name for every adapter.

If a `PRIMARY_KEYS` submission passes HTTP shape validation but later fails
schema-aware literal canonicalization at submission time, the control plane
rejects it with `400 bad_request` and does not durably queue a request row.
If later typed JDBC binding fails after acceptance, the current runtime marks
that request `FAILED`. In both cases the bad repair request does not bring down
the DBLog runtime host.

Likewise, if a submitted `TABLE` or `PRIMARY_KEYS` request names a table that
is not present in the runtime's current captured-schema set, the current
runtime marks that request `FAILED`. The missing-table request does not bring
down the DBLog runtime host.

### 5.3 Metrics and events

Use these endpoints for curated DBLog-owned observability:

- `/api/v1/metrics`
- `/api/v1/events/summary`
- `/api/v1/events/recent`
- `/api/v1/events/tables/{tableDisplayName}`

Current pressure interpretation:

- if the sink is retrying, the control plane should describe that as sink
  unavailability rather than generic slowdown,
- if MySQL source fetching is paused because the bounded source queue is
  full while the sink is still up, the control plane should describe that as
  sink slowdown or inbound pressure exceeding current apply throughput,
- PostgreSQL remains direct-poll rather than queue-prefetch based, so sink
  slowdown shows up through blocked sink-apply state rather than a source-queue
  stall.

Operator-facing event views hide internal watermark/heartbeat activity.

### 5.4 Educational tap

> **Never enable the tap in production.** The tap deliberately blocks the
> DBLog pump thread when the subscriber cannot keep up: that is the entire
> point of the feature. It exists so a separate TUI can visualise the
> watermark algorithm step by step on a teaching machine. Running it against
> a real workload will stall CDC under any subscriber slowdown.

When `dblog.tap.enabled=true`, the control plane mounts one extra route:

- `GET /api/v1/tap/stream`: chunked NDJSON. Exactly one subscriber at a
  time. A new connection displaces any in-flight one (TCP-like last-wins
  semantics). Pins one executor thread until the subscriber disconnects.
  The subscriber detects the end of the run via socket close. TCP flow
  control is what stalls the pump when the subscriber can't keep up:
  that is the step-mode mechanism.

The startup WARN means only that the educational tap is enabled. Actual
tap-induced pump blocking is surfaced separately:

- server logs emit one `DBLog tap queue is full ...` WARN per tap queue
  lifetime when the queue first fills and the pump blocks,
- the tap stream emits `stream.standby` after
  `dblog.tap.standby-threshold-ms`, then `stream.resumed` after the
  HTTP writer catches up,
- tap `stream.heartbeat` events include `queue_depth` and
  `queue_capacity` for the tap queue.

`/api/v1/runtime/status` `sourceFlowControl.queueDepth` is source-log
flow-control state, not tap HTTP stream queue health.

Every envelope carries `run_id`. Subscribers use it to detect a new run
(process restart) without relying on a handshake event.

The route returns `503 tap_not_enabled` when `dblog.tap.enabled=false`.

Relevant config:

| property                           | default | notes                                                       |
|------------------------------------|---------|-------------------------------------------------------------|
| `dblog.tap.enabled`                | `false` | master toggle. Off by default.                              |
| `dblog.tap.queue-capacity`         | `65536` | bounded queue between pump and HTTP writer                  |
| `dblog.tap.standby-threshold-ms`   | `1000`  | how long the producer must block before `stream.standby`    |
| `dblog.tap.heartbeat-interval`     | `2s`    | idle heartbeat cadence (any Spring-parseable Duration)      |

#### Source of truth for event shape

The per-kind JSON Schemas under `docs/schema/events/` are the authoritative
wire contract. The Java event POJOs under
`src/main/java/io/github/aandreakis/dblog/tap/generated/` are
generated from those schemas via `./gradlew generateTapSchemaClasses` and
committed to the repo. The generator is manual-only and deterministic.
External consumers (Rust via `typify`, TypeScript via `quicktype`, etc.)
point their own codegen at `docs/schema/tap-event.schema.json`, which is
the `oneOf` union of the per-kind files.

Field inclusion follows Jackson's `@JsonInclude(NON_NULL)` convention:
optional fields are **omitted** from the wire when their value is not
applicable, not emitted as explicit `null`. Consumers should treat the
absence of an optional field as equivalent to "not applicable here". The
only exception is `seq` on `stream.standby` and `stream.resumed`, which is
emitted as explicit `null` because those events are published out of band
by the HTTP writer thread and therefore do not carry a pump-assigned
sequence number (see §5.4 *Envelope* below).

#### Envelope

Every tap event starts with the same six envelope fields in this order:

| field       | type            | notes                                                                 |
|-------------|-----------------|-----------------------------------------------------------------------|
| `v`         | integer         | Tap schema version. Always `1` in the current runtime.                |
| `seq`       | integer or null | Monotonic per-run sequence. `null` for the two out-of-band kinds.     |
| `ts`        | string          | ISO-8601 UTC with six fractional-second digits (microseconds).        |
| `run_id`    | string          | DBLog runtime run id. Changes across a process restart.               |
| `source_id` | string          | `dblog.source.id` from runtime config.                                |
| `kind`      | string          | One of the 13 event kinds listed below.                               |

`seq` is monotonically increasing across all non-out-of-band events for a
given `run_id`. A reader can gap-detect by comparing successive `seq`
values. Out-of-band events (`stream.standby`, `stream.resumed`) do not
consume sequence numbers because they are written directly by the HTTP
writer thread without passing through the pump's per-batch buffer.

#### Event catalog

Thirteen event kinds, grouped by what they observe:

| kind                    | summary                                                                                               |
|-------------------------|-------------------------------------------------------------------------------------------------------|
| `cdc`                   | One CDC row change observed on the source change stream.                                              |
| `sink.event`            | One event appended to a sink delegate, post reconciliation. Emitted once per sink per event.         |
| `watermark.written`     | DBLog wrote a LOW or HIGH watermark row on `dblog_meta.watermarks`.                                   |
| `watermark.received`    | The reconciler observed the LOW or HIGH watermark token come back on the source change log.          |
| `chunk.selected`        | A chunk SELECT finished inside the currently open watermark window.                                   |
| `chunk.collision`       | An in-window CDC event caused a PK removal from the chunk refresh buffer.                             |
| `chunk.completed`       | HIGH watermark observed, refresh rows emitted, chunk acknowledged.                                    |
| `checkpoint.advanced`   | Buffered streaming checkpoint rolled forward.                                                         |
| `request.transition`    | A dump request's durable state changed.                                                               |
| `error`                 | A fail-closed runtime boundary was hit.                                                               |
| `stream.heartbeat`      | Liveness plus queue-depth sample. Emitted on batch commits when the heartbeat interval has elapsed.   |
| `stream.standby`        | Producer has blocked on a full queue for at least `dblog.tap.standby-threshold-ms`. Out of band.      |
| `stream.resumed`        | Producer is no longer blocked. Out of band.                                                           |

See the per-kind schema files for the full field catalog and required /
optional split:

- `docs/schema/events/cdc-event.schema.json`
- `docs/schema/events/sink-event.schema.json`
- `docs/schema/events/watermark-written.schema.json`
- `docs/schema/events/watermark-received.schema.json`
- `docs/schema/events/chunk-selected.schema.json`
- `docs/schema/events/chunk-collision.schema.json`
- `docs/schema/events/chunk-completed.schema.json`
- `docs/schema/events/checkpoint-advanced.schema.json`
- `docs/schema/events/request-transition.schema.json`
- `docs/schema/events/error-event.schema.json`
- `docs/schema/events/stream-heartbeat.schema.json`
- `docs/schema/events/stream-standby.schema.json`
- `docs/schema/events/stream-resumed.schema.json`

#### Checkpoint reason vocabulary

The `reason` field on `checkpoint.advanced` is one of the four canonical
strings defined by the schema enum. The `BufferedCheckpointDispatcher`
calls into the tap with its own internal labels which the tap normalises
as follows:

| dispatcher label            | tap `reason` |
|-----------------------------|--------------|
| `null`                      | `count`      |
| `batched-threshold`         | `count`      |
| `batched-time`              | `time`       |
| `stage-end`, `shutdown`     | `boundary`   |

The schema enum also permits `bytes`, reserved for future use. Any
dispatcher label the tap does not recognise is treated as a runtime
contract violation: the tap raises `IllegalStateException` rather than
emitting a wire value that the schema would reject. Adding a new
dispatcher label therefore requires extending both the switch in
`ActiveTap#normaliseCheckpointReason` and the reason enum in
`checkpoint-advanced.schema.json` in the same change, then regenerating
the POJOs.

#### Delivery semantics

At-most-once per subscriber. There is a narrow window between
`queue.poll(...)` and a successful `out.write(...) + out.flush(...)` in
which a line has been dequeued but not yet written to the socket. If the
client disconnects in that window the line is dropped. A reconnecting
reader starts from wherever the queue head is at the time of reconnection
rather than replaying. If an external consumer needs
exactly-once-per-attached-subscriber delivery in the future, the intended
fix is a peek-then-commit inversion: design decision deferred until a
concrete consumer-side requirement exists.

The feature is source-available only as a teaching artefact and is not
supported for production use.

## 6. HTTP polling targets

The control plane is **polling-first** rather than push-first. Operator
tooling (curl scripts, dashboards, monitoring agents) should poll these
endpoints on a reasonable cadence rather than expecting server-pushed updates.

Recommended polling targets include:

- `/api/v1/runtime`
- `/api/v1/runtime/status`
- `/api/v1/metrics`
- `/api/v1/events/summary`
- `/api/v1/events/recent`
- `/api/v1/requests`

## 7. Error model

Common HTTP error classes:

- `400 bad_request` for malformed JSON or invalid query values,
- `404 not_found` for unknown request ids or unmatched routes,
- `413 request_too_large` when the request body exceeds the configured
  `dblog.control-plane.max-request-body-bytes` limit,
- `405 method_not_allowed` for wrong HTTP methods,
- `503 service_unavailable` for missing state-store/configuration requirements or
  unavailable request-submission infrastructure,
- `500 internal_error` for unexpected internal failures.

Use this document and the current server/test code for exact endpoint paths,
query parameters, request bodies, and response envelopes.

## 8. Pausing and resuming DBLog

DBLog does not expose operator pause/resume/cancel endpoints. The equivalent
operation is a **process restart**:

- to pause ingest, stop the DBLog process (SIGTERM or SIGKILL, both being safe),
- to resume, start the DBLog process again against the same state path.

The embedded state store persists enough progress at chunk boundaries for a
fresh process to resume where the previous one stopped:

- stream checkpoints are flushed at batch boundaries, so streaming resumes from
  the last acknowledged source position rather than from the start,
- in-flight dumps resume at the last completed chunk. Rows already emitted are
  not re-emitted from the source, and the reconciliation algorithm still holds
  because low/high watermark rows sit in the ordinary source log,
- requests that were `ACTIVE` at the moment of the kill remain `ACTIVE` in the
  durable state store and the restarted process picks them up on its next
  coordinator poll,
- `COMPLETED` and `FAILED` requests are terminal and unaffected by restart.

A restart is not a hard preemption of the in-flight batch that was executing
when the process died: any events that had already been written to the sink
and checkpointed remain durable, and the next process re-runs the watermark
window for the chunk that was in flight. Sinks must therefore be idempotent at
the `(tableDisplayName, primaryKey, sourcePosition)` grain, which is the same
contract already assumed by the at-least-once delivery model.

To cancel a request, either stop DBLog and prune the row from the durable
state store directly, or accept that the request will complete on its own
(dumps are bounded by the source's current row count).
