# PostgreSQL Adapter

This guide keeps the PostgreSQL-specific information that is useful to
operators and readers evaluating the adapter. It intentionally does not restate
the shared core algorithm contract that now lives in the source Javadocs.

## Supported shape

Current supported slice:

- PostgreSQL source majors verified in CI: **14** through **18**
- logical decoding plugin: **`pgoutput`**
- one PostgreSQL database per runtime request
- explicit-table publication contract
- logical replication slot reused across restarts
- metadata tables in schema **`dblog_meta`**
- captured user tables must use **`REPLICA IDENTITY FULL`**

The adapter is intentionally narrow rather than trying to support every
PostgreSQL logical-replication feature.

## Why `pgoutput` Rather Than `wal2json`?

The DBLog paper describes `wal2json`; this implementation uses `pgoutput`
instead. That is a modernization, not a semantic change.

Reasons:

- `pgoutput` ships with every supported PostgreSQL release
- no external shared library or extra install step is required
- `pgoutput` is a standard PostgreSQL logical-replication output plugin
- pgJDBC can decode the binary protocol directly
- the DBLog watermark algorithm only needs committed-order change streams, and
  both plugins provide that

## Required source preconditions

The adapter is usable only when all of the following hold:

- logical decoding is available on the source
- every captured table uses `REPLICA IDENTITY FULL`
- the live publication contains the captured user tables and
  `dblog_meta.watermarks`
- on the standard DBLog-managed path, the publication also contains
  `dblog_meta.heartbeats`
- the logical slot uses plugin `pgoutput`
- publication, slot, and captured tables all belong to the same database
- the runtime role has the privileges and ownership needed for the configured
  publication and slot ownership modes

The shared runtime SQL connection is expected to be:

- open
- `autoCommit=true`
- `READ_COMMITTED`

## Transport security (TLS)

**DBLog ships no typed TLS support for the PostgreSQL adapter.** Both the regular SQL connection (chunk reads, watermark writes, schema inspection, preflight) and the logical-replication connection (CDC stream via `pgoutput`) are opened with the operator's `dblog.source.postgres.jdbc-url` and `dblog.source.postgres.replication-jdbc-url` verbatim. pgJDBC's TLS parameters work because the driver parses them — DBLog itself does nothing.

If you need TLS, configure it through the JDBC URL:

```properties
dblog.source.postgres.jdbc-url=jdbc:postgresql://host:5432/app?sslmode=verify-full&sslrootcert=/path/to/ca.crt
dblog.source.postgres.replication-jdbc-url=jdbc:postgresql://host:5432/app?sslmode=verify-full&sslrootcert=/path/to/ca.crt
```

Both URLs must carry the TLS parameters independently — DBLog does not propagate them between connections. mTLS (`sslcert`/`sslkey`) works the same way.

This is a deliberate scope choice for a reference implementation. The shipped Docker fixtures do not enable TLS and the `compatibilityMatrix` lane runs plaintext.

## Runtime role and ownership

Run DBLog with a dedicated PostgreSQL role, not the `postgres` superuser. The
bundled fixture role is named `dblog`, but the name is not special.

For the default `DBLOG_MANAGED` publication and slot ownership mode, the runtime
role must:

- have `LOGIN` and `REPLICATION`
- connect to the configured database
- have `USAGE` and `CREATE` on schema `dblog_meta`
- be able to create, insert, and update `dblog_meta.watermarks` and
  `dblog_meta.heartbeats`
- own every captured user table that DBLog may add to the publication
- own the DBLog-managed publication after creation

If the `dblog_meta` schema does not yet exist, the runtime role additionally
needs `CREATE` on the database so it can create the schema on first run. The
recommended setup is to pre-create the schema with an admin connection and
grant `USAGE, CREATE` on it to the runtime role, which is what the bundled
fixture does via
`CREATE SCHEMA IF NOT EXISTS dblog_meta AUTHORIZATION dblog` in
`ops/docker/postgres/init/01-create-users.sql`.

The table-ownership rule is PostgreSQL's publication rule: the role that creates
or alters an explicit-table publication must own the tables in that publication.
If DBLog runs as a role that only has table grants, startup can fail with
`must be owner of table ...` when it creates or repairs the publication.

Administrator connections may still perform one-time setup, fixture reset, slot
cleanup, and emergency repair out of band. They are not required for the normal
DBLog runtime path.

## Publication contract

The PostgreSQL publication contract is intentionally explicit and narrow.

The publication must:

- be an exact explicit table set
- not use `FOR ALL TABLES`
- not use `TABLES IN SCHEMA`
- publish `INSERT`, `UPDATE`, and `DELETE`
- not publish `TRUNCATE`
- not use row filters
- not use column lists
- not enable `publish_via_partition_root`

DBLog-managed publications may be created or repaired inside that narrow model.
If an existing publication is already broad or schema-scoped, the adapter fails
closed rather than mutating it in place.

## Logical-slot contract

The PostgreSQL slot contract is also intentionally narrow.

The slot must:

- be a logical slot
- belong to the configured database
- use plugin `pgoutput`
- match the configured temporary / two-phase / failover flags
- be usable (`wal_status` not `lost`, blank invalidation reason)
- not already be active

A missing slot is created only when ownership is DBLog-managed.

## Metadata tables

The adapter uses two singleton metadata tables in schema `dblog_meta`:

- `watermarks`
- `heartbeats`

The publication must include `watermarks`, and the standard DBLog-managed path
also includes `heartbeats` so same-stream interference detection can see them
on the logical stream. Malformed metadata events and same-stream heartbeat
interference fail closed. When the state store is available, these paths record
a schema-uncertainty signal before failing closed.

`dblog_meta.watermarks` is a singleton control table, not a watermark history
table. DBLog updates the single `id = 1` row for each LOW/HIGH watermark, so
`SELECT * FROM dblog_meta.watermarks` shows only the latest token. Use the tap
stream or the PostgreSQL logical stream if you need a chronological watermark
trace.

## Replica-identity preflight

DBLog checks `REPLICA IDENTITY FULL` before opening the replication stream.
That gate runs up front on purpose: deferring it until the first relation or
row event would allow the runtime to open the stream and risk leaking partial
results before rejecting the table shape.

## Runtime behavior worth knowing

- one committed transaction is buffered in memory before being handed to the
  sink as a batch
- watermark and heartbeat writes happen on the same source-side SQL path as
  chunk reads
- targeted repair reuses the same watermark-window machinery as ordinary table
  dumps
- replication-slot feedback failure does not lose DBLog's durable local
  checkpoint, but it can cause WAL retention to grow on the server until the
  slot advances again

### `TIMETZ` normalization

PostgreSQL `TIMETZ` values use DBLog's neutral `TIME` representation. Both the
JDBC chunk path and the `pgoutput` streaming path preserve the value's local
wall-clock component as a `LocalTime` and deliberately discard its UTC offset.
They do not convert the value through the JVM default time zone. Because that
neutral form cannot preserve offset-sensitive identity or ordering, `TIMETZ`
columns are supported as ordinary selected values but not as primary keys on
captured tables. Schema inspection and live startup fail closed before a
replication stream is opened when a captured primary key contains `TIMETZ`.

For heap sizing guidance around large committed transactions, see
`docs/OPERATION.md` §2.4.2.

## Slot Lifecycle

Logical replication slots are persistent server objects. A slot whose consumer
disappears retains WAL on the source until the slot advances or is dropped.
DBLog behaves like Debezium here: it creates a missing slot when DBLog-managed,
reuses it across restarts, and never drops it automatically.

### Operator contract

Operators are responsible for slot hygiene.

Consequences:

- decommissioning DBLog requires explicit slot cleanup
- recreating local fixtures without clearing old slot/state can leave the slot
  pointing at WAL that no longer exists
- two DBLog deployments pointed at the same slot name will fail closed when the
  second finds the slot already active

Recommended safety net:

- set `max_slot_wal_keep_size` on PostgreSQL 13+ so an abandoned slot cannot
  retain unbounded WAL forever

Useful checks:

```sql
SELECT slot_name, database, active, restart_lsn, confirmed_flush_lsn, wal_status, invalidation_reason
  FROM pg_replication_slots
 WHERE plugin = 'pgoutput';
```

```sql
SELECT slot_name,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS restart_lsn_lag_bytes,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS confirmed_flush_lsn_lag_bytes
  FROM pg_replication_slots
 WHERE plugin = 'pgoutput';
```

Decommissioning cleanup:

```sql
SELECT pg_drop_replication_slot('<slot_name>');
```

Do not drop a slot while a DBLog process is still connected.

## Schema and DDL policy

The PostgreSQL adapter does **not** ship a schema-history subsystem and does
**not** attempt broad DDL replay.

Current policy:

- additive schema changes may be reconciled if they preserve the configured
  selected-column contract
- non-additive or continuity-breaking changes require a fresh dump
- unsupported or malformed `pgoutput` shapes fail closed
- captured-table `TRUNCATE` messages, runtime replica-identity drift,
  selected-column relation metadata drift, key-only or missing old tuples for
  captured-table `UPDATE` / `DELETE` messages, and live primary-key updates
  record a full-dump-required signal and fail closed

The live `pgoutput` decoder maps tuple values by selected column name, so an
added unselected relation column can be ignored mechanically. Relation messages
also carry type OIDs, but this runtime does not use live relation OIDs as a
selected-column type-drift detector. Startup/restart schema inspection remains
the supported point that catches selected type, source-type, and nullability
drift. Operators should not rely on online PostgreSQL schema evolution as a
feature; stop DBLog, change the source, verify startup reconciliation, and submit
a fresh dump when the selected contract changed or correctness is uncertain.

This is a deliberate tradeoff: smaller implementation, no schema-history
dependency, and explicit operator action when schema continuity becomes
uncertain.

## Intentional limits

The current PostgreSQL adapter intentionally does not do the following:

- support `FOR ALL TABLES` or schema-scoped publications
- clean up logical slots automatically
- replay broad DDL history
- broaden into a general-purpose PostgreSQL CDC platform

## Read the code

If you need the implementation entry points rather than the operator guide,
start here:

- `adapter/postgres/internal/PostgresSourcePreflight.java`
- `adapter/postgres/internal/JdbcPostgresPublicationManager.java`
- `adapter/postgres/internal/JdbcPostgresReplicationSlotManager.java`
- `adapter/postgres/internal/PostgresTransactionStreamingSession.java`
- `adapter/postgres/PostgresLiveStreamingRuntime.java`
