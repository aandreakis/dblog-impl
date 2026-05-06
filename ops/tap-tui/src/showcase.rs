//! Procedural demo generator — scripted teaching intro followed by an
//! infinite continuous traffic loop. Unlike [`crate::demo::chunk42_scenario`]
//! (used by unit tests and for the simple single-chunk teaching shot), this
//! source emits events forever until shutdown; both the scripted acts and
//! the continuous loop pass through the same pacer, so `--slowdown` and
//! `--step` work identically either way.
//!
//! The **intro** walks through the key algorithmic cases in one narrative:
//!
//! 1. Pre-request warmup: heartbeat + plain CDC + a checkpoint
//! 2. Request 42 submitted (scope TABLE, app.orders) → ACTIVE
//! 3. Chunk 1 — 15 rows, 0 collisions (the happy path; no exclusions)
//! 4. Plain-CDC gap with a DELETE and an INSERT
//! 5. Chunk 2 — 15 rows, 3 collisions (including one DELETE hit)
//! 6. `stream.standby` + `stream.resumed` (backpressure interlude)
//! 7. Chunk 3 — 20 rows, 5 collisions (heavy contention)
//! 8. Chunk 4 — 8 rows, 1 collision, `final_chunk=true`
//! 9. Request 42 COMPLETED
//! 10. Post-request plain CDC
//! 11. Final heartbeat
//!
//! After the intro, the **continuous loop** generates an endless mix of:
//! plain CDC bursts (orders + accounts), fresh requests opening new chunks,
//! chunks with random collision counts, heartbeats, checkpoints, and the
//! occasional standby/resumed pair. The RNG is a deterministic xorshift
//! seeded from a fixed constant so replays are reproducible.

use crate::event::*;
use crate::pacing::Pacer;
use crate::source::SourceMessage;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

pub const SHOWCASE_RUN_ID: &str = "a1b2c3d4-e5f6-4890-abcd-ef1234567890";
pub const SHOWCASE_SOURCE_ID: &str = "my_mysql_source";

/// Schema used to dress the showcase's events. `Standard` emits the
/// canonical `app.orders` / `app.accounts` tables with plain numeric
/// PKs; `Long` swaps in fully-qualified production-shape names and
/// composite LONG-typed PK literals — same event sequence, same
/// timing, just fatter identifiers. Operators can stress-test how
/// the TUI behaves under real-world enterprise naming without
/// maintaining a second procedural generator.
#[derive(Debug, Clone, Copy)]
pub enum SchemaKind {
    Standard,
    Long,
}

/// Entry point for the canonical showcase — short tables / numeric PKs.
pub fn run(pacer: Pacer, shutdown: Arc<AtomicBool>, tx: SyncSender<SourceMessage>) {
    run_with_schema(SchemaKind::Standard, "demo://showcase", pacer, shutdown, tx);
}

/// Entry point for the long-name / composite-PK variant of the showcase.
pub fn run_long(pacer: Pacer, shutdown: Arc<AtomicBool>, tx: SyncSender<SourceMessage>) {
    run_with_schema(SchemaKind::Long, "demo://showcase-long", pacer, shutdown, tx);
}

fn run_with_schema(
    schema: SchemaKind,
    label: &'static str,
    pacer: Pacer,
    shutdown: Arc<AtomicBool>,
    tx: SyncSender<SourceMessage>,
) {
    let _ = tx.send(SourceMessage::Connected {
        label: label.into(),
    });
    let mut ctx = DemoCtx::new(schema);

    if !play_intro(&mut ctx, &pacer, &shutdown, &tx) {
        let _ = tx.send(SourceMessage::Finished);
        return;
    }

    while !shutdown.load(Ordering::Acquire) {
        if !play_continuous_act(&mut ctx, &pacer, &shutdown, &tx) {
            break;
        }
    }

    let _ = tx.send(SourceMessage::Finished);
}

// ---------------------------------------------------------------------------
// DemoCtx — rolling state the generator carries through the run
// ---------------------------------------------------------------------------

struct DemoCtx {
    /// Which set of table names + PK shape the generator emits. The
    /// whole showcase traffic pattern is reused for both variants —
    /// only the display of `table.name` + `pk` on the wire differs.
    schema: SchemaKind,
    seq: u64,
    chunk_id_counter: u64,
    lsn_cursor: u64,
    /// Next PK the next chunk will start at. Increments by `rows` after each chunk.
    pk_cursor: u64,
    request_id_counter: u64,
    active_request: Option<String>,
    anchor: DateTime<Utc>,
    elapsed_ms: i64,
    rng_state: u64,
    act_count: u64,
    acts_since_heartbeat: u64,
}

impl DemoCtx {
    fn new(schema: SchemaKind) -> Self {
        Self {
            schema,
            seq: 0,
            chunk_id_counter: 0,
            lsn_cursor: 16,
            pk_cursor: 100,
            request_id_counter: 41, // first request becomes 42
            active_request: None,
            anchor: Utc.with_ymd_and_hms(2026, 4, 20, 14, 30, 0).unwrap(),
            elapsed_ms: 0,
            rng_state: 0xC0FFEE_1234_5678,
            act_count: 0,
            acts_since_heartbeat: 0,
        }
    }

    /// Format a numeric pk through the active schema. Standard emits
    /// the plain decimal (`1002`); Long wraps it in a composite
    /// literal so the TUI sees the same worst-case shape the
    /// `chunk42-long` scenario uses. Keeping this on `DemoCtx`
    /// spares every call site from threading the schema separately.
    fn fmt_pk(&self, pk: u64) -> String {
        match self.schema {
            SchemaKind::Standard => pk.to_string(),
            SchemaKind::Long => format!(
                "{{tenant_id=17001234,order_id={:013},line_id={}}}",
                90_000_000_000u64 + pk,
                (pk % 7) + 1,
            ),
        }
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn advance_time(&mut self, ms: i64) {
        self.elapsed_ms = self.elapsed_ms.saturating_add(ms);
    }

    fn now(&self) -> DateTime<Utc> {
        self.anchor + ChronoDuration::milliseconds(self.elapsed_ms)
    }

    fn next_lsn(&mut self, bump: u64) -> String {
        self.lsn_cursor = self.lsn_cursor.saturating_add(bump);
        format!("binlog.000042:{}", self.lsn_cursor)
    }

    fn next_chunk_id(&mut self) -> u64 {
        self.chunk_id_counter += 1;
        self.chunk_id_counter
    }

    fn rng(&mut self) -> u64 {
        // xorshift64 — deterministic across rebuilds
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        x
    }

    fn rng_range(&mut self, lo: u64, hi_exclusive: u64) -> u64 {
        if hi_exclusive <= lo {
            return lo;
        }
        lo + self.rng() % (hi_exclusive - lo)
    }

    fn fake_uuid(&mut self) -> String {
        format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            (self.rng() & 0xffff_ffff) as u32,
            (self.rng() & 0xffff) as u16,
            (self.rng() & 0xffff) as u16,
            (self.rng() & 0xffff) as u16,
            self.rng() & 0xffff_ffff_ffff
        )
    }
}

fn emit(
    event: TapEvent,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> bool {
    if shutdown.load(Ordering::Acquire) {
        return false;
    }
    if tx.send(SourceMessage::Event(event)).is_err() {
        return false;
    }
    pacer.wait();
    !shutdown.load(Ordering::Acquire)
}

// ---------------------------------------------------------------------------
// Primitive event builders
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Table {
    Orders,
    Accounts,
}

impl Table {
    /// Wire-form display of the table. The pair is selected by
    /// `schema`: Standard uses the canonical `app.orders` /
    /// `app.accounts`; Long swaps in fully-qualified real-world-shape
    /// names so the TUI's `table / pk` column has to cope with FQNs.
    fn display(self, schema: SchemaKind) -> &'static str {
        match (schema, self) {
            (SchemaKind::Standard, Self::Orders) => "app.orders",
            (SchemaKind::Standard, Self::Accounts) => "app.accounts",
            (SchemaKind::Long, Self::Orders) => {
                "production_analytics.customer_order_line_items"
            }
            (SchemaKind::Long, Self::Accounts) => {
                "identity_platform.enterprise_customer_accounts"
            }
        }
    }
}

fn play_cdc_pair(
    ctx: &mut DemoCtx,
    table: Table,
    pk: u64,
    op: Op,
    chunk_id: Option<u64>,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> bool {
    let lsn_bump = 1 + ctx.rng() % 4;
    let lsn = ctx.next_lsn(lsn_bump);
    let tx_id = format!("tx-{:04x}", (ctx.rng() & 0xffff) as u16);
    let advance = 2 + (ctx.rng() % 6) as i64;
    ctx.advance_time(advance);

    let cdc = TapEvent::Cdc(CdcBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        lsn: lsn.clone(),
        op,
        table: table.display(ctx.schema).into(),
        pk: ctx.fmt_pk(pk),
        tx_id: Some(tx_id.clone()),
    });
    if !emit(cdc, pacer, shutdown, tx) {
        return false;
    }

    ctx.advance_time(1);
    let sink = TapEvent::SinkEvent(SinkEventBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        lsn,
        op,
        table: table.display(ctx.schema).into(),
        pk: ctx.fmt_pk(pk),
        origin: Origin::Log,
        tx_id: Some(tx_id),
        dump_id: None,
        chunk_id,
        sink_name: "ndjson".into(),
    });
    emit(sink, pacer, shutdown, tx)
}

fn play_heartbeat(
    ctx: &mut DemoCtx,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> bool {
    ctx.acts_since_heartbeat = 0;
    let advance = 30 + (ctx.rng() % 50) as i64;
    ctx.advance_time(advance);
    let depth = ctx.rng() % 25;
    let e = TapEvent::StreamHeartbeat(StreamHeartbeatBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        queue_depth: depth,
        queue_capacity: 65_536,
    });
    emit(e, pacer, shutdown, tx)
}

fn play_checkpoint(
    ctx: &mut DemoCtx,
    reason: CheckpointReason,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> bool {
    ctx.advance_time(1);
    let position = format!("binlog.000042:{}", ctx.lsn_cursor);
    let e = TapEvent::CheckpointAdvanced(CheckpointAdvancedBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        position,
        buffered_events: ctx.rng() % 8,
        reason,
    });
    emit(e, pacer, shutdown, tx)
}

fn play_standby_resumed(
    ctx: &mut DemoCtx,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> bool {
    // Standby is out-of-band; seq is None.
    ctx.advance_time(20);
    let full_ms = 1_200 + ctx.rng_range(100, 800);
    let standby = TapEvent::StreamStandby(StreamStandbyBody {
        v: 1,
        seq: None,
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        queue_full_ms: full_ms,
        queue_capacity: 65_536,
        reason: StandbyReason::QueueFull,
        message: "DBLog is on standby — waiting for the reader to advance".into(),
    });
    if !emit(standby, pacer, shutdown, tx) {
        return false;
    }

    ctx.advance_time(full_ms as i64 + 150);
    let resumed = TapEvent::StreamResumed(StreamResumedBody {
        v: 1,
        seq: None,
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        standby_total_ms: full_ms + 150,
    });
    emit(resumed, pacer, shutdown, tx)
}

#[allow(dead_code)] // kept as a demo primitive — wire back in if desired
fn play_error(
    ctx: &mut DemoCtx,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> bool {
    ctx.advance_time(5);
    let mut context = std::collections::HashMap::new();
    context.insert(
        "expected_token".to_string(),
        serde_json::Value::String(ctx.fake_uuid()),
    );
    context.insert(
        "actual_token".to_string(),
        serde_json::Value::String(ctx.fake_uuid()),
    );
    context.insert(
        "chunk_id".to_string(),
        serde_json::Value::Number(ctx.chunk_id_counter.into()),
    );
    let e = TapEvent::Error(ErrorBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        class: "WatermarkSequenceException".into(),
        message: "watermark token did not match outstanding token (transient)".into(),
        context: Some(context),
    });
    emit(e, pacer, shutdown, tx)
}

fn play_request_transition(
    ctx: &mut DemoCtx,
    request_id: &str,
    scope: RequestScope,
    table: Option<&str>,
    state: RequestState,
    prev_state: Option<RequestState>,
    reason: Option<&str>,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> bool {
    ctx.advance_time(2);
    let e = TapEvent::RequestTransition(RequestTransitionBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        request_id: request_id.to_string(),
        scope,
        table: table.map(String::from),
        state,
        prev_state,
        reason: reason.map(String::from),
    });
    emit(e, pacer, shutdown, tx)
}

// ---------------------------------------------------------------------------
// Compound act: a full chunk cycle with configurable contention
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ChunkConfig {
    rows: u64,
    collisions: u64,
    passthroughs: u64,
    final_chunk: bool,
    table: Table,
}

fn play_chunk(
    ctx: &mut DemoCtx,
    cfg: ChunkConfig,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> bool {
    let chunk_id = ctx.next_chunk_id();
    let pk_min = ctx.pk_cursor;
    let pk_max = ctx.pk_cursor + cfg.rows - 1;
    let start_after_pk = if chunk_id == 1 {
        None
    } else {
        Some(ctx.fmt_pk(ctx.pk_cursor - 1))
    };
    let request_id = ctx.active_request.clone().unwrap_or_else(|| "?".into());
    let dump_id = format!("d-{}", request_id);
    let lw_token = ctx.fake_uuid();
    let hw_token = ctx.fake_uuid();
    let fingerprint = format!(
        "fp{:08x}{:08x}",
        (ctx.rng() & 0xffff_ffff) as u32,
        (ctx.rng() & 0xffff_ffff) as u32
    );

    // 1. watermark.written LW
    ctx.advance_time(1);
    let e = TapEvent::WatermarkWritten(WatermarkWrittenBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        level: WatermarkLevel::Low,
        token: lw_token.clone(),
        chunk_id,
    });
    if !emit(e, pacer, shutdown, tx) {
        return false;
    }

    // 2. chunk.selected
    ctx.advance_time(2);
    let e = TapEvent::ChunkSelected(ChunkSelectedBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        chunk_id,
        request_id: request_id.clone(),
        dump_id: dump_id.clone(),
        table: cfg.table.display(ctx.schema).into(),
        mode: ChunkMode::Range,
        pk_min: Some(ctx.fmt_pk(pk_min)),
        pk_max: Some(ctx.fmt_pk(pk_max)),
        start_after_pk,
        row_count: cfg.rows,
        final_chunk: cfg.final_chunk,
        fingerprint,
    });
    if !emit(e, pacer, shutdown, tx) {
        return false;
    }

    // 3. watermark.written HW
    ctx.advance_time(1);
    let e = TapEvent::WatermarkWritten(WatermarkWrittenBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        level: WatermarkLevel::High,
        token: hw_token.clone(),
        chunk_id,
    });
    if !emit(e, pacer, shutdown, tx) {
        return false;
    }

    // 4. watermark.received LW — window OPEN
    let lw_advance = 8 + (ctx.rng() % 6) as i64;
    ctx.advance_time(lw_advance);
    let lw_rx_lsn = ctx.next_lsn(8);
    let e = TapEvent::WatermarkReceived(WatermarkReceivedBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        level: WatermarkLevel::Low,
        token: lw_token,
        chunk_id,
        lsn: lw_rx_lsn,
        latency_ms: Some(8 + ctx.rng_range(2, 15)),
    });
    if !emit(e, pacer, shutdown, tx) {
        return false;
    }

    // 5. Interleave in-window events: collisions + passthroughs.
    // Collision PKs are picked deterministically from the chunk range.
    let mut collision_pks: Vec<u64> = (0..cfg.collisions)
        .map(|i| {
            let step = cfg.rows / cfg.collisions.max(1);
            pk_min + i * step + (ctx.rng() % step.max(1))
        })
        .collect();
    collision_pks.sort();
    collision_pks.dedup();

    let mut passthrough_remaining = cfg.passthroughs;
    for (i, &coll_pk) in collision_pks.iter().enumerate() {
        // Occasionally emit a passthrough before this collision for realism.
        if passthrough_remaining > 0 && (i % 2 == 0 || cfg.collisions == 0) {
            let pt_pk = pk_max + 100 + passthrough_remaining;
            let op = if ctx.rng() % 3 == 0 {
                Op::Insert
            } else {
                Op::Update
            };
            if !play_cdc_pair(ctx, cfg.table, pt_pk, op, Some(chunk_id), pacer, shutdown, tx) {
                return false;
            }
            passthrough_remaining -= 1;
        }

        // Collision CDC: pick op with occasional DELETE for drama.
        let op = match ctx.rng() % 5 {
            0 => Op::Delete,
            1 => Op::Insert,
            _ => Op::Update,
        };
        let lsn_bump = 1 + ctx.rng() % 4;
        let lsn = ctx.next_lsn(lsn_bump);
        let tx_id = format!("tx-w{:04x}", (ctx.rng() & 0xffff) as u16);
        let advance = 2 + (ctx.rng() % 5) as i64;
        ctx.advance_time(advance);

        let cdc = TapEvent::Cdc(CdcBody {
            v: 1,
            seq: Some(ctx.next_seq()),
            ts: ctx.now(),
            run_id: SHOWCASE_RUN_ID.into(),
            source_id: SHOWCASE_SOURCE_ID.into(),
            lsn: lsn.clone(),
            op,
            table: cfg.table.display(ctx.schema).into(),
            pk: ctx.fmt_pk(coll_pk),
            tx_id: Some(tx_id.clone()),
        });
        if !emit(cdc, pacer, shutdown, tx) {
            return false;
        }

        ctx.advance_time(1);
        let coll = TapEvent::ChunkCollision(ChunkCollisionBody {
            v: 1,
            seq: Some(ctx.next_seq()),
            ts: ctx.now(),
            run_id: SHOWCASE_RUN_ID.into(),
            source_id: SHOWCASE_SOURCE_ID.into(),
            chunk_id,
            excluded_pk: Some(ctx.fmt_pk(coll_pk)),
            cause_lsn: lsn.clone(),
            cause_op: op,
            cause_tx_id: Some(tx_id.clone()),
        });
        if !emit(coll, pacer, shutdown, tx) {
            return false;
        }

        ctx.advance_time(1);
        let sink = TapEvent::SinkEvent(SinkEventBody {
            v: 1,
            seq: Some(ctx.next_seq()),
            ts: ctx.now(),
            run_id: SHOWCASE_RUN_ID.into(),
            source_id: SHOWCASE_SOURCE_ID.into(),
            lsn,
            op,
            table: cfg.table.display(ctx.schema).into(),
            pk: ctx.fmt_pk(coll_pk),
            origin: Origin::Log,
            tx_id: Some(tx_id),
            dump_id: None,
            chunk_id: Some(chunk_id),
            sink_name: "ndjson".into(),
        });
        if !emit(sink, pacer, shutdown, tx) {
            return false;
        }
    }

    // Emit any remaining passthroughs.
    while passthrough_remaining > 0 {
        let pt_pk = pk_max + 100 + passthrough_remaining;
        let op = if ctx.rng() % 3 == 0 {
            Op::Insert
        } else {
            Op::Update
        };
        if !play_cdc_pair(ctx, cfg.table, pt_pk, op, Some(chunk_id), pacer, shutdown, tx) {
            return false;
        }
        passthrough_remaining -= 1;
    }

    // 6. watermark.received HW — window CLOSE
    let hw_latency = 70 + ctx.rng_range(20, 70);
    ctx.advance_time(hw_latency as i64);
    let hw_rx_lsn = ctx.next_lsn(12);
    let e = TapEvent::WatermarkReceived(WatermarkReceivedBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        level: WatermarkLevel::High,
        token: hw_token,
        chunk_id,
        lsn: hw_rx_lsn.clone(),
        latency_ms: Some(hw_latency),
    });
    if !emit(e, pacer, shutdown, tx) {
        return false;
    }

    // 7. Refresh rows — one per surviving PK, all carrying HW's LSN.
    let collision_set: HashSet<u64> = collision_pks.iter().copied().collect();
    let mut emit_count = 0u64;
    for pk in pk_min..=pk_max {
        if collision_set.contains(&pk) {
            continue;
        }
        ctx.advance_time(1);
        let e = TapEvent::SinkEvent(SinkEventBody {
            v: 1,
            seq: Some(ctx.next_seq()),
            ts: ctx.now(),
            run_id: SHOWCASE_RUN_ID.into(),
            source_id: SHOWCASE_SOURCE_ID.into(),
            lsn: hw_rx_lsn.clone(),
            op: Op::Update,
            table: cfg.table.display(ctx.schema).into(),
            pk: ctx.fmt_pk(pk),
            origin: Origin::Select,
            tx_id: None,
            dump_id: Some(dump_id.clone()),
            chunk_id: Some(chunk_id),
            sink_name: "ndjson".into(),
        });
        if !emit(e, pacer, shutdown, tx) {
            return false;
        }
        emit_count += 1;
    }

    // 8. chunk.completed
    ctx.advance_time(2);
    let duration_ms = 100 + ctx.rng_range(30, 90);
    let e = TapEvent::ChunkCompleted(ChunkCompletedBody {
        v: 1,
        seq: Some(ctx.next_seq()),
        ts: ctx.now(),
        run_id: SHOWCASE_RUN_ID.into(),
        source_id: SHOWCASE_SOURCE_ID.into(),
        chunk_id,
        request_id,
        table: cfg.table.display(ctx.schema).into(),
        emitted: emit_count,
        excluded: cfg.collisions,
        last_pk: Some(ctx.fmt_pk(pk_max)),
        lsn: hw_rx_lsn,
        duration_ms,
        final_chunk: cfg.final_chunk,
    });
    if !emit(e, pacer, shutdown, tx) {
        return false;
    }

    // 9. checkpoint.advanced
    if !play_checkpoint(ctx, CheckpointReason::Boundary, pacer, shutdown, tx) {
        return false;
    }

    ctx.pk_cursor += cfg.rows;
    true
}

// ---------------------------------------------------------------------------
// Intro — the scripted teaching sequence
// ---------------------------------------------------------------------------

fn play_intro(
    ctx: &mut DemoCtx,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> bool {
    // Phase 1: pre-request warmup
    if !play_heartbeat(ctx, pacer, shutdown, tx) {
        return false;
    }
    if !play_cdc_pair(ctx, Table::Accounts, 500, Op::Update, None, pacer, shutdown, tx) {
        return false;
    }
    if !play_cdc_pair(ctx, Table::Orders, 851, Op::Insert, None, pacer, shutdown, tx) {
        return false;
    }
    if !play_checkpoint(ctx, CheckpointReason::Count, pacer, shutdown, tx) {
        return false;
    }

    // Phase 2: request submitted
    ctx.request_id_counter += 1;
    let req = ctx.request_id_counter.to_string();
    if !play_request_transition(
        ctx,
        &req,
        RequestScope::Table,
        Some("app.orders"),
        RequestState::Active,
        None,
        None,
        pacer,
        shutdown,
        tx,
    ) {
        return false;
    }
    ctx.active_request = Some(req);

    // Phase 3: Chunk 1 — clean, no collisions (the happy path)
    if !play_chunk(
        ctx,
        ChunkConfig {
            rows: 15,
            collisions: 0,
            passthroughs: 2,
            final_chunk: false,
            table: Table::Orders,
        },
        pacer,
        shutdown,
        tx,
    ) {
        return false;
    }

    // Phase 4: plain CDC between chunks (including a DELETE)
    if !play_cdc_pair(ctx, Table::Orders, 837, Op::Delete, None, pacer, shutdown, tx) {
        return false;
    }
    if !play_cdc_pair(ctx, Table::Orders, 853, Op::Insert, None, pacer, shutdown, tx) {
        return false;
    }
    if !play_heartbeat(ctx, pacer, shutdown, tx) {
        return false;
    }

    // Phase 5: Chunk 2 — 3 collisions (medium contention, one DELETE hit)
    if !play_chunk(
        ctx,
        ChunkConfig {
            rows: 15,
            collisions: 3,
            passthroughs: 2,
            final_chunk: false,
            table: Table::Orders,
        },
        pacer,
        shutdown,
        tx,
    ) {
        return false;
    }

    // Phase 6: standby/resumed — backpressure interlude
    if !play_standby_resumed(ctx, pacer, shutdown, tx) {
        return false;
    }

    // Phase 7: Chunk 3 — 5 collisions (heavy contention)
    if !play_chunk(
        ctx,
        ChunkConfig {
            rows: 20,
            collisions: 5,
            passthroughs: 1,
            final_chunk: false,
            table: Table::Orders,
        },
        pacer,
        shutdown,
        tx,
    ) {
        return false;
    }

    // Phase 8: Chunk 4 — final chunk, 1 collision
    if !play_chunk(
        ctx,
        ChunkConfig {
            rows: 8,
            collisions: 1,
            passthroughs: 0,
            final_chunk: true,
            table: Table::Orders,
        },
        pacer,
        shutdown,
        tx,
    ) {
        return false;
    }

    // Phase 9: request COMPLETED
    if let Some(req) = ctx.active_request.clone() {
        if !play_request_transition(
            ctx,
            &req,
            RequestScope::Table,
            Some("app.orders"),
            RequestState::Completed,
            Some(RequestState::Active),
            None,
            pacer,
            shutdown,
            tx,
        ) {
            return false;
        }
    }
    ctx.active_request = None;

    // Phase 10: post-request plain CDC before the continuous loop takes over
    if !play_cdc_pair(ctx, Table::Orders, 856, Op::Update, None, pacer, shutdown, tx) {
        return false;
    }
    if !play_cdc_pair(ctx, Table::Accounts, 700, Op::Update, None, pacer, shutdown, tx) {
        return false;
    }
    if !play_cdc_pair(ctx, Table::Orders, 857, Op::Update, None, pacer, shutdown, tx) {
        return false;
    }

    // Phase 11: final heartbeat before entering the continuous loop
    play_heartbeat(ctx, pacer, shutdown, tx)
}

// ---------------------------------------------------------------------------
// Continuous loop — forever after the intro
// ---------------------------------------------------------------------------

fn play_continuous_act(
    ctx: &mut DemoCtx,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> bool {
    ctx.act_count += 1;
    ctx.acts_since_heartbeat += 1;

    // Always keep a request active so the chunk pipeline stays hot.
    if ctx.active_request.is_none() {
        ctx.request_id_counter += 1;
        let req = ctx.request_id_counter.to_string();
        if !play_request_transition(
            ctx,
            &req,
            RequestScope::Table,
            Some("app.orders"),
            RequestState::Active,
            None,
            None,
            pacer,
            shutdown,
            tx,
        ) {
            return false;
        }
        ctx.active_request = Some(req);
    }

    // Force a heartbeat every ~15 acts so the queue-depth line stays fresh.
    if ctx.acts_since_heartbeat >= 15 {
        if !play_heartbeat(ctx, pacer, shutdown, tx) {
            return false;
        }
    }

    let roll = ctx.rng() % 100;
    if roll < 40 {
        // Plain CDC burst: 3–7 events, mixed orders/accounts, mixed ops.
        let n = ctx.rng_range(3, 8) as u64;
        for _ in 0..n {
            let pk = 800 + ctx.rng_range(1, 200);
            let table = if ctx.rng() % 4 == 0 {
                Table::Accounts
            } else {
                Table::Orders
            };
            let op = match ctx.rng() % 4 {
                0 => Op::Insert,
                1 => Op::Delete,
                _ => Op::Update,
            };
            if !play_cdc_pair(ctx, table, pk, op, None, pacer, shutdown, tx) {
                return false;
            }
        }
        true
    } else if roll < 75 {
        // A chunk with random characteristics.
        let rows = ctx.rng_range(10, 30);
        let collisions = (ctx.rng() % 6).min(rows);
        let passthroughs = ctx.rng() % 4;
        // 1-in-25 chance this chunk is the final chunk of its request.
        let final_chunk = ctx.rng() % 25 == 0;
        let cfg = ChunkConfig {
            rows,
            collisions,
            passthroughs,
            final_chunk,
            table: Table::Orders,
        };
        if !play_chunk(ctx, cfg, pacer, shutdown, tx) {
            return false;
        }
        if final_chunk {
            if let Some(req) = ctx.active_request.clone() {
                if !play_request_transition(
                    ctx,
                    &req,
                    RequestScope::Table,
                    Some("app.orders"),
                    RequestState::Completed,
                    Some(RequestState::Active),
                    None,
                    pacer,
                    shutdown,
                    tx,
                ) {
                    return false;
                }
            }
            ctx.active_request = None;
        }
        true
    } else if roll < 90 {
        play_heartbeat(ctx, pacer, shutdown, tx)
    } else if roll < 98 {
        let reason = match ctx.rng() % 3 {
            0 => CheckpointReason::Count,
            1 => CheckpointReason::Time,
            _ => CheckpointReason::Boundary,
        };
        play_checkpoint(ctx, reason, pacer, shutdown, tx)
    } else {
        play_standby_resumed(ctx, pacer, shutdown, tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc::{sync_channel, TryRecvError};

    /// Sanity-check the intro: it should produce at least four chunks with
    /// the expected excluded counts, a request ACTIVE→COMPLETED transition,
    /// a standby/resumed pair, and a DELETE-op cdc.
    #[test]
    fn intro_covers_expected_cases() {
        let (_h, pacer) = crate::pacing::channel(0, false);
        let shutdown = Arc::new(AtomicBool::new(false));
        // Large buffer so the synchronous test harness can drive the
        // generator without the send-side blocking on a full channel.
        // The runtime uses a tight cap (16) for live pacing; tests need
        // headroom because nothing is reading the rx in parallel.
        let (tx, rx) = sync_channel::<SourceMessage>(4096);

        let mut ctx = DemoCtx::new(SchemaKind::Standard);
        let ok = play_intro(&mut ctx, &pacer, &shutdown, &tx);
        drop(tx);

        assert!(ok, "intro aborted unexpectedly");

        let mut chunks_completed = Vec::new();
        let mut active_transitions = 0;
        let mut completed_transitions = 0;
        let mut saw_standby = false;
        let mut saw_resumed = false;
        let mut saw_error = false;
        let mut saw_delete_cdc = false;
        let mut saw_final_chunk = false;
        loop {
            match rx.try_recv() {
                Ok(SourceMessage::Event(TapEvent::ChunkCompleted(b))) => {
                    if b.final_chunk {
                        saw_final_chunk = true;
                    }
                    chunks_completed.push(b.excluded);
                }
                Ok(SourceMessage::Event(TapEvent::RequestTransition(b))) => match b.state {
                    RequestState::Active => active_transitions += 1,
                    RequestState::Completed => completed_transitions += 1,
                    _ => {}
                },
                Ok(SourceMessage::Event(TapEvent::StreamStandby(_))) => saw_standby = true,
                Ok(SourceMessage::Event(TapEvent::StreamResumed(_))) => saw_resumed = true,
                Ok(SourceMessage::Event(TapEvent::Error(_))) => saw_error = true,
                Ok(SourceMessage::Event(TapEvent::Cdc(b))) if b.op == Op::Delete => {
                    saw_delete_cdc = true;
                }
                Ok(_) => {}
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        assert_eq!(chunks_completed, vec![0, 3, 5, 1], "excluded counts per chunk");
        assert_eq!(active_transitions, 1);
        assert_eq!(completed_transitions, 1);
        assert!(saw_standby, "intro should include a stream.standby");
        assert!(saw_resumed);
        assert!(!saw_error, "intro should NOT produce an error event");
        assert!(saw_delete_cdc, "intro should include a DELETE cdc to show op coverage");
        assert!(saw_final_chunk, "one chunk should have final_chunk=true");
    }

    /// Continuous loop should keep emitting events indefinitely; check that a
    /// handful of acts produce at least some sink events and advance seq.
    #[test]
    fn continuous_loop_emits_forever_until_shutdown() {
        let (h, pacer) = crate::pacing::channel(0, false);
        let shutdown = Arc::new(AtomicBool::new(false));
        // Large buffer so the synchronous test harness can drive the
        // generator without the send-side blocking on a full channel.
        // The runtime uses a tight cap (16) for live pacing; tests need
        // headroom because nothing is reading the rx in parallel.
        let (tx, rx) = sync_channel::<SourceMessage>(4096);

        let mut ctx = DemoCtx::new(SchemaKind::Standard);
        ctx.active_request = Some("99".into());
        for _ in 0..20 {
            let ok = play_continuous_act(&mut ctx, &pacer, &shutdown, &tx);
            assert!(ok, "act aborted early");
        }
        shutdown.store(true, Ordering::Release);
        h.advance(); // wake up any step-blocked waiter (no-op here)
        drop(tx);

        let mut event_count = 0;
        let mut max_seq = 0;
        while let Ok(msg) = rx.try_recv() {
            if let SourceMessage::Event(ev) = msg {
                event_count += 1;
                if let Some(s) = ev.seq() {
                    max_seq = max_seq.max(s);
                }
            }
        }
        assert!(event_count > 20, "20 acts should produce >20 events");
        assert!(max_seq > 10, "seq should advance");
    }
}
