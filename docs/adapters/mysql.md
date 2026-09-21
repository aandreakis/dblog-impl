# MySQL Adapter

This guide keeps the MySQL-specific information that is useful to operators and
readers evaluating the adapter. It intentionally does not restate the shared
core algorithm contract that now lives in the source Javadocs.

## Supported shape

Current supported slice:

- MySQL source versions verified in CI: **8.0**, **8.4**, **9.6**
- live source capture through the MySQL binlog
- JDBC chunk reads and targeted primary-key repair reads
- one actual MySQL database per runtime request
- metadata tables in a dedicated MySQL database named **`dblog_meta`**
- durable checkpoints stored as binlog file/position, with optional GTID set
  carried alongside as fallback resume help

The adapter is intentionally DBLog-owned and does **not** embed Debezium.

## Required source preconditions

The adapter is intentionally strict. A MySQL source is usable only if all of
the following hold:

- binary logging is enabled
- `binlog_format=ROW`
- `binlog_row_image=FULL`
- `binlog_row_metadata=FULL`
- captured-table live schema can be reconciled against the configured
  selected-column contract at startup
- all captured tables for one runtime request belong to the same actual MySQL
  database

The `binlog_row_metadata=FULL` requirement is deliberate: DBLog depends on
column names in `TABLE_MAP` events so row decoding remains stable across an
offline `ALTER`.

### Transport security (TLS)

**DBLog ships no typed TLS support for the MySQL adapter.** The binlog client used for CDC capture is constructed without an SSL mode, which means it negotiates plaintext only. The chunk-read JDBC connection is opened with the operator's `dblog.source.mysql.jdbc-url` verbatim. MySQL Connector/J's TLS parameters (`?sslMode=REQUIRED`, `?trustCertificateKeyStoreUrl=…`, `?clientCertificateKeyStoreUrl=…`) work if you put them there, but DBLog does nothing to coordinate the two connections.

Practical consequences:

- A MySQL server with `require_secure_transport=ON` will **reject the binlog connection** at handshake time. The shipped DBLog cannot consume CDC from such a server. Fork if you need this.
- The chunk-read JDBC connection can be made TLS-encrypted purely through `jdbc-url` parameters, which is operator-managed and not validated by DBLog.

This is a deliberate scope choice for this implementation. The shipped Docker fixtures do not enable TLS and the `compatibilityMatrix` lane runs plaintext.

### Required user privileges

The DBLog user must hold at least:

- `REPLICATION SLAVE` and `REPLICATION CLIENT` on `*.*`
- sufficient privileges on the captured user schema(s) for chunk reads and
  metadata inspection
- sufficient privileges on `dblog_meta` for watermark and heartbeat table
  management
- `SESSION_VARIABLES_ADMIN` on MySQL 8+ (or `SUPER` on 5.7) so DBLog can
  session-scope `sql_log_bin = 0` around its own metadata bootstrap DDL

The last privilege is not strictly required for correctness. Without it, DBLog
falls back to a narrow binlog-session bypass for its own metadata bootstrap
DDL. That keeps DBLog working, but those DDL statements still land in the
binlog, which matters if other consumers or downstream replicas observe the
same stream.

## Metadata tables

The adapter uses two singleton metadata tables in `dblog_meta`:

- `watermarks`
- `heartbeats`

Their row changes are surfaced as internal DBLog control events rather than as
ordinary user-table updates. The adapter fails closed on malformed metadata
events, unexpected singleton-row behavior, same-stream heartbeat interference,
or metadata deletes. When the state store is available, these paths record a
schema-uncertainty signal before failing closed.

`dblog_meta.watermarks` is a singleton control table, not a watermark history
table. DBLog updates the single `id = 1` row for each LOW/HIGH watermark, so
`SELECT * FROM dblog_meta.watermarks` shows only the latest token. Use the tap
stream or the source binlog if you need a chronological watermark trace.

## Schema and DDL policy

The MySQL adapter does **not** ship a schema-history subsystem and does **not**
attempt to stream through captured-table DDL.

Current policy:

- additive column adds may be reconciled at startup or across an offline
  restart if they do not break the configured selected-column contract
- non-additive changes such as type changes, renames, selected-column removals,
  or primary-key shape changes require a fresh dump
- row-state- or schema-affecting DDL observed in the live binlog with the
  captured database as the MySQL Query event default database records a
  full-dump-required signal and fails closed, including additive `ADD COLUMN`
  statements while DBLog is running
- live `TABLE_MAP` metadata that is missing a selected column records a
  table-scoped full-dump-required signal and fails closed
- live user-row tuples that are shorter than the selected-column shape from
  `TABLE_MAP` metadata record a table-scoped full-dump-required signal and
  fail closed
- live primary-key updates on captured tables record a table-scoped
  full-dump-required signal and fail closed

The MySQL live path deliberately does not parse DDL. MySQL Query events expose
the session default database and raw SQL text, not a structured target table id.
As a result, DBLog does not auto-accept online MySQL `ADD COLUMN`, even when the
new column is outside the selected replicated surface. It can also fail closed
on row-state- or schema-affecting DDL that targets an uncaptured table in the
captured database. These false positives are intentional limitations. Avoiding
silent divergence on captured-table DDL takes priority over accepting every
unrelated table change in the same MySQL database.

The lower row decoder still maps binlog tuples by the selected contract when a
`TABLE_MAP` event exposes column names. If extra columns appear in that metadata
without a preceding unsupported Query event stopping the stream, DBLog ignores
those extra columns. That mechanical tolerance is not support for online MySQL
schema evolution. Live captured-database DDL remains a fail-closed boundary.

If the live binlog fail-closed path trips on unsupported DDL, selected-column
`TABLE_MAP` drift, selected-column row tuple drift, or a live primary-key
update, a restart with the same persisted state will resume from the
pre-failure checkpoint and replay the same event. The runtime records a
full-dump-required signal when the state store is available. Recover by
verifying or re-bootstrapping the target, clearing or replacing the persisted
runtime state/checkpoint for that source, then restarting and submitting a
fresh `ALL_TABLES` dump. The configured `dblog.runtime.state-path` is an H2
file prefix, not a directory: remove `<state-path>.mv.db` and any
`<state-path>.trace.db` / `<state-path>.lock.db` files, or use a new state
path.

This is a deliberate tradeoff: smaller implementation, no Kafka-style schema
history dependency, and explicit operator action when schema continuity becomes
uncertain.

## Runtime behavior worth knowing

- one committed transaction is buffered in memory before being handed to the
  sink as a batch
- watermark and heartbeat writes happen on the same source-side SQL path as
  chunk reads
- targeted repair reuses the same watermark-window machinery as ordinary table
  dumps

### Temporal value limitation

MySQL `DATETIME` is zone-less, and the JDBC chunk-read path preserves it as a
zone-less `LocalDateTime`. The live binlog path in this implementation depends
on `mysql-binlog-connector-java`'s decoded Java value. For `DATETIME(6)` row
events, that path can surface a `java.util.Date`, so DBLog emits a UTC
`Instant`-shaped value with millisecond precision rather than a zone-less value
with full microsecond precision. This limitation applies to user-row column
values only. The educational tap envelope `ts` fields are separate runtime
timestamps. If exact MySQL `DATETIME(6)` live-path fidelity matters, avoid
putting those columns in the selected replicated surface or treat them as a
known adapter limitation.

### `TIME` precision limitation

MySQL `TIME(n)` values are carried at **millisecond** precision on both capture
origins. `mysql-binlog-connector-java` builds its `TIME` row values as a
`java.sql.Time`, which cannot represent microseconds, so `TIME(4)` through
`TIME(6)` lose their last three digits on the live path regardless of what the
chunk read does. The chunk read is deliberately held at the same ceiling rather
than reading full microseconds: making the two origins disagree would hide the
in-window collision between a snapshot row and a fresher log event for the same
row. PostgreSQL is not limited this way (see `docs/adapters/postgres.md`).

Two related edges are unsupported rather than approximated:

- MySQL `TIME` outside `00:00:00..23:59:59` (legal in MySQL, which treats
  `TIME` as a duration) has no neutral representation, since the neutral type
  is `java.time.LocalTime`. The two paths do not even fail the same way: the
  chunk read raises a driver error, while the binlog path silently wraps modulo
  24 hours, so `25:00:00` arrives as `01:00:00` and `838:59:59` as `22:59:59`.
  Negative values are worse than a wrap: the decoder never interprets the
  `TIME2` sign bit, so `-01:00:00` is read as hour 1023 and arrives as `15:00`.
  Keep such columns out of the replicated surface.
- Because the chunk read stops at milliseconds, a `TIME(4..6)` **primary key**
  still yields a table-scan upper bound truncated by up to one millisecond, so
  rows in that final sub-millisecond window are not dumped.

### Known live-path value defects

These are unfixed divergences between the chunk path and the binlog path. All
are silent, and none fails closed:

- `ENUM` is decoded as its ordinal and `SET` as its bitmask, so a live event
  carries `3` where a dump carries `shipped`. The label arrays are captured in
  the table-map message but not yet consumed.
- Unsigned integers are sign-extended, so values above the signed range arrive
  negative on the live path.
- `TIMESTAMP` primary keys are read UTC-pinned but bound back using the JVM
  default zone, which can silently skip or replay rows during keyset
  pagination. The same JVM-zone assumption applies when the JDBC apply sink
  binds a neutral `Instant` to a MySQL target.

For heap sizing guidance around large committed transactions, see
`docs/OPERATION.md` §2.4.2.

## Intentional limits

The current MySQL adapter intentionally does not do the following:

- capture multiple actual MySQL databases in one runtime request
- replicate broad DDL
- maintain a schema-history store
- support Debezium signaling tables or a second CDC architecture inside the
  repo
- broaden into a general-purpose MySQL CDC platform

## Read the code

If you need the implementation entry points rather than the operator guide,
start here:

- `adapter/mysql/internal/MySqlSourcePreflight.java`
- `adapter/mysql/internal/MySqlBinlogSession.java`
- `adapter/mysql/MySqlLiveStreamingRuntime.java`
- `adapter/mysql/internal/JdbcMySqlChunkReader.java`
