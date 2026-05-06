//! Built-in event scripts for offline / demo mode.
//!
//! Each function returns a `Vec<TapEvent>` in seq order, timestamped with
//! realistic microsecond-level `ts` values anchored at a fixed `ANCHOR`
//! instant. The runner can replay these at any pace — they're the same
//! `TapEvent` the live wire produces, so demo and live share the state
//! machine and renderers.

use crate::event::*;
use chrono::{DateTime, Duration, TimeZone, Utc};

pub const DEMO_RUN_ID: &str = "3f290e82-4a17-48b1-b8c1-a2f9e0c7d312";
pub const DEMO_SOURCE_ID: &str = "my_mysql_source";

fn anchor() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 4, 20, 12, 7, 22).unwrap()
}

fn at(ms_from_anchor: i64) -> DateTime<Utc> {
    anchor() + Duration::milliseconds(ms_from_anchor)
}

/// Schema used by the `chunk42` teaching scenario. Carries the two
/// table names and a `pk_fn` for turning the numeric PK (the underlying
/// u64 index into the chunk range) into the string the wire carries.
/// Plain numeric keys use `|n| n.to_string()`; composite-key schemas
/// stringify the u64 into a multi-field literal.
pub struct Chunk42Schema {
    pub main_table: &'static str,
    pub other_table: &'static str,
    pub pk_fn: fn(u64) -> String,
}

fn numeric_pk(n: u64) -> String {
    n.to_string()
}

/// Schema for the default scenario — `app.orders` / `app.accounts` /
/// plain numeric PKs. The `app.` prefix is stripped in the renderer
/// (`short_table`), so tables display as `orders` / `accounts`.
pub const CHUNK42_SCHEMA: Chunk42Schema = Chunk42Schema {
    main_table: "app.orders",
    other_table: "app.accounts",
    pk_fn: numeric_pk,
};

/// Schema for the long-name / composite-key stress variant. The main
/// table is a real-world-style fully-qualified name (schema + table),
/// and PKs are composite literals combining a LONG tenant id, a LONG
/// order id, and a line id — exercising the worst case the wire
/// actually produces. Keeps the same u64 index the scenario uses so
/// row counts, collisions, and refresh fan-out stay identical.
fn long_pk(n: u64) -> String {
    // `n` is the scenario's sequential PK (e.g. 1001..=1040). We spray
    // it out to three LONG fields so the composite literal is realistic
    // enterprise-shape: tenant + order + line.
    format!(
        "{{tenant_id=17001234,order_id={:013},line_id={}}}",
        90_000_000_000u64 + n,
        (n % 7) + 1,
    )
}

pub const CHUNK42_LONG_SCHEMA: Chunk42Schema = Chunk42Schema {
    main_table: "production_analytics.customer_order_line_items",
    other_table: "identity_platform.enterprise_customer_accounts",
    pk_fn: long_pk,
};

/// The canonical chunk-42 teaching scenario:
///
/// - request 17 (TABLE app.orders) transitions QUEUED→ACTIVE
/// - chunk 42 (pk range [1001..1040], 40 rows) selected under LW/HW
/// - two in-window CDC events hit chunk-buffer PKs → 2 collisions
/// - several out-of-window CDC events pass through unchanged
/// - HW lands, 38 refresh rows fan out with origin=SELECT and lsn=HW's LSN
/// - chunk.completed; checkpoint advances to HW's LSN
pub fn chunk42_scenario() -> Vec<TapEvent> {
    chunk42_scenario_with(&CHUNK42_SCHEMA)
}

/// Same event shape as [`chunk42_scenario`] but with long / composite
/// identifiers — used to stress-test how the TUI lanes cope with
/// fully-qualified table names and LONG-typed composite keys.
pub fn chunk42_long_scenario() -> Vec<TapEvent> {
    chunk42_scenario_with(&CHUNK42_LONG_SCHEMA)
}

pub fn chunk42_scenario_with(schema: &Chunk42Schema) -> Vec<TapEvent> {
    let main_table = schema.main_table;
    let other_table = schema.other_table;
    let pk = schema.pk_fn;
    let mut out: Vec<TapEvent> = Vec::new();

    // Request lifecycle: ACTIVE. (DBLog emits only ACTIVE/COMPLETED/FAILED,
    // not QUEUED — first-ever transition has no prev_state.)
    out.push(TapEvent::RequestTransition(RequestTransitionBody {
        v: 1, seq: Some(1), ts: at(-200),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        request_id: "17".into(),
        scope: RequestScope::Table,
        table: Some(main_table.into()),
        state: RequestState::Active,
        prev_state: None,
        reason: None,
    }));

    // Early heartbeat — idle queue.
    out.push(TapEvent::StreamHeartbeat(StreamHeartbeatBody {
        v: 1, seq: Some(2), ts: at(-100),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        queue_depth: 0, queue_capacity: 65_536,
    }));

    // LW written.
    out.push(TapEvent::WatermarkWritten(WatermarkWrittenBody {
        v: 1, seq: Some(3), ts: at(0),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        level: WatermarkLevel::Low,
        token: "a71c88f1-4e02-4d0a-91d5-1cb9e4e7a0a2".into(),
        chunk_id: 42,
    }));

    // Chunk SELECT: 40 rows, pk range [1001..1040], after 1000.
    out.push(TapEvent::ChunkSelected(ChunkSelectedBody {
        v: 1, seq: Some(4), ts: at(1),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        chunk_id: 42,
        request_id: "17".into(),
        dump_id: "r-17".into(),
        table: main_table.into(),
        mode: ChunkMode::Range,
        pk_min: Some(pk(1001)),
        pk_max: Some(pk(1040)),
        start_after_pk: Some(pk(1000)),
        row_count: 40,
        final_chunk: false,
        fingerprint: "c8c4e91cf2b34b8d9e1c7fcb6c8b2e92".into(),
    }));

    // HW written.
    out.push(TapEvent::WatermarkWritten(WatermarkWrittenBody {
        v: 1, seq: Some(5), ts: at(3),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        level: WatermarkLevel::High,
        token: "b7e099c2-3d50-4e3e-8a37-4a9b5d1f7c08".into(),
        chunk_id: 42,
    }));

    // LW received → window OPEN.
    out.push(TapEvent::WatermarkReceived(WatermarkReceivedBody {
        v: 1, seq: Some(6), ts: at(10),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        level: WatermarkLevel::Low,
        token: "a71c88f1-4e02-4d0a-91d5-1cb9e4e7a0a2".into(),
        chunk_id: 42,
        lsn: "binlog.000042:1024".into(),
        latency_ms: Some(10),
    }));

    // CDC pk=2044 (above upper bound, in-window passthrough).
    out.push(TapEvent::Cdc(CdcBody {
        v: 1, seq: Some(7), ts: at(18),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1048".into(),
        op: Op::Insert,
        table: main_table.into(),
        pk: pk(2044),
        tx_id: Some("tx-a912".into()),
    }));
    out.push(TapEvent::SinkEvent(SinkEventBody {
        v: 1, seq: Some(8), ts: at(19),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1048".into(),
        op: Op::Insert,
        table: main_table.into(),
        pk: pk(2044),
        origin: Origin::Log,
        tx_id: Some("tx-a912".into()),
        dump_id: None,
        chunk_id: Some(42),
        sink_name: "ndjson".into(),
    }));

    // CDC pk=1002 → will cause chunk.collision, then sink.event LOG.
    out.push(TapEvent::Cdc(CdcBody {
        v: 1, seq: Some(9), ts: at(21),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1152".into(),
        op: Op::Update,
        table: main_table.into(),
        pk: pk(1002),
        tx_id: Some("tx-a912".into()),
    }));
    out.push(TapEvent::ChunkCollision(ChunkCollisionBody {
        v: 1, seq: Some(10), ts: at(22),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        chunk_id: 42,
        excluded_pk: Some(pk(1002)),
        cause_lsn: "binlog.000042:1152".into(),
        cause_op: Op::Update,
        cause_tx_id: Some("tx-a912".into()),
    }));
    out.push(TapEvent::SinkEvent(SinkEventBody {
        v: 1, seq: Some(11), ts: at(22),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1152".into(),
        op: Op::Update,
        table: main_table.into(),
        pk: pk(1002),
        origin: Origin::Log,
        tx_id: Some("tx-a912".into()),
        dump_id: None,
        chunk_id: Some(42),
        sink_name: "ndjson".into(),
    }));

    // CDC pk=1004 → second collision.
    out.push(TapEvent::Cdc(CdcBody {
        v: 1, seq: Some(12), ts: at(33),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1208".into(),
        op: Op::Update,
        table: main_table.into(),
        pk: pk(1004),
        tx_id: Some("tx-a912".into()),
    }));
    out.push(TapEvent::ChunkCollision(ChunkCollisionBody {
        v: 1, seq: Some(13), ts: at(34),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        chunk_id: 42,
        excluded_pk: Some(pk(1004)),
        cause_lsn: "binlog.000042:1208".into(),
        cause_op: Op::Update,
        cause_tx_id: Some("tx-a912".into()),
    }));
    out.push(TapEvent::SinkEvent(SinkEventBody {
        v: 1, seq: Some(14), ts: at(34),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1208".into(),
        op: Op::Update,
        table: main_table.into(),
        pk: pk(1004),
        origin: Origin::Log,
        tx_id: Some("tx-a912".into()),
        dump_id: None,
        chunk_id: Some(42),
        sink_name: "ndjson".into(),
    }));

    // Other-table passthrough.
    out.push(TapEvent::Cdc(CdcBody {
        v: 1, seq: Some(15), ts: at(40),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1332".into(),
        op: Op::Update,
        table: other_table.into(),
        pk: pk(77),
        tx_id: Some("tx-b012".into()),
    }));
    out.push(TapEvent::SinkEvent(SinkEventBody {
        v: 1, seq: Some(16), ts: at(41),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1332".into(),
        op: Op::Update,
        table: other_table.into(),
        pk: pk(77),
        origin: Origin::Log,
        tx_id: Some("tx-b012".into()),
        dump_id: None,
        chunk_id: Some(42),
        sink_name: "ndjson".into(),
    }));

    // Another above-bound CDC (same transaction, 2 events).
    out.push(TapEvent::Cdc(CdcBody {
        v: 1, seq: Some(17), ts: at(51),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1416".into(),
        op: Op::Insert,
        table: main_table.into(),
        pk: pk(2045),
        tx_id: Some("tx-b018".into()),
    }));
    out.push(TapEvent::SinkEvent(SinkEventBody {
        v: 1, seq: Some(18), ts: at(52),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1416".into(),
        op: Op::Insert, table: main_table.into(),
        pk: pk(2045),
        origin: Origin::Log,
        tx_id: Some("tx-b018".into()),
        dump_id: None, chunk_id: Some(42),
        sink_name: "ndjson".into(),
    }));
    out.push(TapEvent::Cdc(CdcBody {
        v: 1, seq: Some(19), ts: at(52),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1420".into(),
        op: Op::Update,
        table: main_table.into(),
        pk: pk(2045),
        tx_id: Some("tx-b018".into()),
    }));
    out.push(TapEvent::SinkEvent(SinkEventBody {
        v: 1, seq: Some(20), ts: at(53),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1420".into(),
        op: Op::Update, table: main_table.into(),
        pk: pk(2045),
        origin: Origin::Log,
        tx_id: Some("tx-b018".into()),
        dump_id: None, chunk_id: Some(42),
        sink_name: "ndjson".into(),
    }));

    // Another accounts update.
    out.push(TapEvent::Cdc(CdcBody {
        v: 1, seq: Some(21), ts: at(60),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1524".into(),
        op: Op::Update, table: other_table.into(),
        pk: pk(77),
        tx_id: Some("tx-b032".into()),
    }));
    out.push(TapEvent::SinkEvent(SinkEventBody {
        v: 1, seq: Some(22), ts: at(61),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1524".into(),
        op: Op::Update, table: other_table.into(),
        pk: pk(77),
        origin: Origin::Log,
        tx_id: Some("tx-b032".into()),
        dump_id: None, chunk_id: Some(42),
        sink_name: "ndjson".into(),
    }));

    // Above-bound cdc on orders near the end of the window.
    out.push(TapEvent::Cdc(CdcBody {
        v: 1, seq: Some(23), ts: at(99),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1728".into(),
        op: Op::Update, table: main_table.into(),
        pk: pk(1900),
        tx_id: Some("tx-b050".into()),
    }));
    out.push(TapEvent::SinkEvent(SinkEventBody {
        v: 1, seq: Some(24), ts: at(100),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        lsn: "binlog.000042:1728".into(),
        op: Op::Update, table: main_table.into(),
        pk: pk(1900),
        origin: Origin::Log,
        tx_id: Some("tx-b050".into()),
        dump_id: None, chunk_id: Some(42),
        sink_name: "ndjson".into(),
    }));

    // HW received → window CLOSE.
    out.push(TapEvent::WatermarkReceived(WatermarkReceivedBody {
        v: 1, seq: Some(25), ts: at(105),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        level: WatermarkLevel::High,
        token: "b7e099c2-3d50-4e3e-8a37-4a9b5d1f7c08".into(),
        chunk_id: 42,
        lsn: "binlog.000042:1920".into(),
        latency_ms: Some(102),
    }));

    // Refresh rows fan out with origin=SELECT and lsn = HW's LSN.
    // Every surviving PK in [1001..1040] minus {1002, 1004} — exactly 38 rows,
    // matching the `emitted: 38` field on the chunk.completed event below.
    // A running seq counter (seeded from the last fixed seq) keeps the bottom
    // of the scenario from drifting out of order when PKs are added or removed.
    let mut seq_counter: u64 = 25; // last fixed seq above is 25 (HW received)
    for (idx, pk_num) in (1001u64..=1040).filter(|n| *n != 1002 && *n != 1004).enumerate() {
        seq_counter += 1;
        out.push(TapEvent::SinkEvent(SinkEventBody {
            v: 1, seq: Some(seq_counter),
            ts: at(106 + (idx as i64).min(15)),
            run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
            lsn: "binlog.000042:1920".into(),
            op: Op::Update, table: main_table.into(),
            pk: pk(pk_num),
            origin: Origin::Select,
            tx_id: None,
            dump_id: Some("r-17".into()),
            chunk_id: Some(42),
            sink_name: "ndjson".into(),
        }));
    }

    // chunk.completed — emitted=38 (40 rows - 2 collisions), excluded=2.
    seq_counter += 1;
    out.push(TapEvent::ChunkCompleted(ChunkCompletedBody {
        v: 1, seq: Some(seq_counter), ts: at(119),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        chunk_id: 42,
        request_id: "17".into(),
        table: main_table.into(),
        emitted: 38,
        excluded: 2,
        last_pk: Some(pk(1040)),
        lsn: "binlog.000042:1920".into(),
        duration_ms: 119,
        final_chunk: false,
    }));

    // checkpoint.advanced — reason=boundary.
    seq_counter += 1;
    out.push(TapEvent::CheckpointAdvanced(CheckpointAdvancedBody {
        v: 1, seq: Some(seq_counter), ts: at(120),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        position: "binlog.000042:1920".into(),
        buffered_events: 0,
        reason: CheckpointReason::Boundary,
    }));

    // Late heartbeat — shows queue drained.
    seq_counter += 1;
    out.push(TapEvent::StreamHeartbeat(StreamHeartbeatBody {
        v: 1, seq: Some(seq_counter), ts: at(125),
        run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
        queue_depth: 0, queue_capacity: 65_536,
    }));

    out
}

/// An extended scenario that layers a `stream.standby` + `stream.resumed`
/// pair over the tail of [`chunk42_scenario`] for visualising backpressure.
pub fn chunk42_with_standby() -> Vec<TapEvent> {
    let mut out = chunk42_scenario();
    // Insert standby right before the last heartbeat (which is the final entry).
    let insert_at = out.len().saturating_sub(1);
    out.insert(
        insert_at,
        TapEvent::StreamStandby(StreamStandbyBody {
            v: 1, seq: None, ts: at(122),
            run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
            queue_full_ms: 1_250, queue_capacity: 65_536,
            reason: StandbyReason::QueueFull,
            message: "DBLog is on standby — waiting for the reader to advance".into(),
        }),
    );
    out.insert(
        insert_at + 1,
        TapEvent::StreamResumed(StreamResumedBody {
            v: 1, seq: None, ts: at(124),
            run_id: DEMO_RUN_ID.into(), source_id: DEMO_SOURCE_ID.into(),
            standby_total_ms: 1_400,
        }),
    );
    out
}
