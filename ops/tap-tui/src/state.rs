//! Consumer-side state machine.
//!
//! Applies one [`TapEvent`] at a time to build a rolling view of what DBLog is
//! doing: recent source-log events, recent sink events, the currently-active
//! chunk's derived state, the completed-chunks history, and the latest
//! transport / lifecycle signals (heartbeat, standby, request.transition,
//! error). The UI renders from this struct — nothing in the renderers parses
//! JSON, and nothing reaches out across threads.
//!
//! Invariants:
//!
//! - `run_id` is set to the first envelope's value; a subsequent envelope
//!   with a different `run_id` triggers a full state reset (restart detection).
//! - `last_seq` tracks the most recent non-OOB seq. A later event with
//!   `seq <= last_seq` is dropped (duplicate / out-of-order); a gap
//!   (`seq > last_seq + 1`) is recorded in `seq_gaps` but the event is still
//!   applied.
//! - `active_chunk` is `Some` from `chunk.selected` (or the first
//!   `watermark.written` of that chunk, whichever arrives first) until
//!   `chunk.completed`, which snapshots a [`ChunkSummary`] into history.

use crate::event::*;
use chrono::{DateTime, Utc};
use std::collections::{HashSet, VecDeque};

/// Rolling-buffer caps — big enough to show history, bounded so the UI stays cheap.
pub const DEFAULT_CAP_SOURCE: usize = 500;
pub const DEFAULT_CAP_SINK: usize = 1_000;
pub const DEFAULT_CAP_HISTORY: usize = 32;
/// Cap on the sticky cause-LSN index that backs `is_collision_cause_lsn`.
/// Sized to outlive the source-log buffer so a CDC row that's still visible
/// in the source pane keeps its collision tint even after its chunk has
/// aged out of `chunks_history` (cap 32).
pub const DEFAULT_CAP_CAUSE_LSNS: usize = 2_000;

/// Source-log entry — one of the six source-log-facing kinds.
///
/// `checkpoint.advanced` events are accepted on the wire but intentionally
/// dropped: they're producer-side bookkeeping and don't affect the
/// reconciler, so the TUI leaves them to DBLog.
#[derive(Debug, Clone)]
pub enum SourceEntry {
    Cdc(CdcBody),
    WatermarkWritten(WatermarkWrittenBody),
    WatermarkReceived(WatermarkReceivedBody),
    ChunkSelected(ChunkSelectedBody),
    ChunkCollision(ChunkCollisionBody),
    ChunkCompleted(ChunkCompletedBody),
}

impl SourceEntry {
    pub fn seq(&self) -> Option<u64> {
        match self {
            Self::Cdc(b) => b.seq,
            Self::WatermarkWritten(b) => b.seq,
            Self::WatermarkReceived(b) => b.seq,
            Self::ChunkSelected(b) => b.seq,
            Self::ChunkCollision(b) => b.seq,
            Self::ChunkCompleted(b) => b.seq,
        }
    }
    pub fn ts(&self) -> DateTime<Utc> {
        match self {
            Self::Cdc(b) => b.ts,
            Self::WatermarkWritten(b) => b.ts,
            Self::WatermarkReceived(b) => b.ts,
            Self::ChunkSelected(b) => b.ts,
            Self::ChunkCollision(b) => b.ts,
            Self::ChunkCompleted(b) => b.ts,
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Cdc(_) => "cdc",
            Self::WatermarkWritten(_) => "watermark.written",
            Self::WatermarkReceived(_) => "watermark.received",
            Self::ChunkSelected(_) => "chunk.selected",
            Self::ChunkCollision(_) => "chunk.collision",
            Self::ChunkCompleted(_) => "chunk.completed",
        }
    }
}

/// Derived state for the currently-open watermark window.
#[derive(Debug, Clone, Default)]
pub struct ActiveChunk {
    pub chunk_id: u64,
    pub request_id: Option<String>,
    pub dump_id: Option<String>,
    pub table: Option<String>,
    pub pk_min: Option<String>,
    pub pk_max: Option<String>,
    pub start_after_pk: Option<String>,
    pub row_count: Option<u64>,
    pub final_chunk: Option<bool>,
    pub fingerprint: Option<String>,
    pub lw_token: Option<String>,
    pub hw_token: Option<String>,
    pub lw_written_ts: Option<DateTime<Utc>>,
    pub hw_written_ts: Option<DateTime<Utc>>,
    pub lw_received_ts: Option<DateTime<Utc>>,
    pub hw_received_ts: Option<DateTime<Utc>>,
    pub lw_received_lsn: Option<String>,
    pub hw_received_lsn: Option<String>,
    pub lw_latency_ms: Option<u64>,
    pub hw_latency_ms: Option<u64>,
    /// Canonical PK literals of rows excluded by in-window collisions.
    pub excluded_pks: Vec<String>,
    /// Source-log LSNs that caused a collision on this chunk. UIs use this set
    /// to tint CDC / sink.event rows whose `lsn` is in it.
    pub cause_lsns: HashSet<String>,
    pub excluded: u64,
}

impl ActiveChunk {
    /// Predicted refresh-row count on HW: `row_count - excluded`, saturating.
    pub fn emit_queue(&self) -> u64 {
        self.row_count.unwrap_or(0).saturating_sub(self.excluded)
    }
}

/// Immutable snapshot pushed to `chunks_history` on `chunk.completed`. Carries
/// enough information for the Loom timeline to render closed windows and
/// recognise their collision LSNs even after `active_chunk` has been cleared.
#[derive(Debug, Clone)]
pub struct ChunkSummary {
    pub chunk_id: u64,
    pub request_id: String,
    pub table: String,
    pub emitted: u64,
    pub excluded: u64,
    pub duration_ms: u64,
    pub final_chunk: bool,
    pub completed_ts: DateTime<Utc>,
    /// `watermark.received.lsn` for LW — window-open LSN, copied from the
    /// matching `ActiveChunk` at the moment `chunk.completed` fires.
    pub lw_lsn: Option<String>,
    /// `watermark.received.lsn` for HW — window-close LSN. Same value as
    /// `chunk.completed.lsn`.
    pub completed_lsn: String,
    /// Every `chunk.collision.cause_lsn` observed inside this chunk's window.
    /// Lets the timeline keep the collision tint on CDC rows in historical
    /// windows after `active_chunk` has moved on.
    pub cause_lsns: HashSet<String>,
}

/// Top-level consumer state. Owned by the UI thread.
#[derive(Debug)]
pub struct State {
    pub run_id: Option<String>,
    pub source_id: Option<String>,
    pub first_ts: Option<DateTime<Utc>>,
    pub last_ts: Option<DateTime<Utc>>,
    pub last_seq: Option<u64>,
    pub seq_gaps: u64,
    pub events_total: u64,
    pub cdc_total: u64,
    pub sink_log_total: u64,
    pub sink_refresh_total: u64,
    pub chunks_done: u64,
    pub collisions_total: u64,
    pub source_log: VecDeque<SourceEntry>,
    pub sink_output: VecDeque<SinkEventBody>,
    pub active_chunk: Option<ActiveChunk>,
    pub chunks_history: VecDeque<ChunkSummary>,
    pub latest_request: Option<RequestTransitionBody>,
    pub latest_error: Option<ErrorBody>,
    pub latest_heartbeat: Option<StreamHeartbeatBody>,
    pub standby: Option<StreamStandbyBody>,
    pub last_resumed: Option<StreamResumedBody>,
    pub cap_source: usize,
    pub cap_sink: usize,
    pub cap_history: usize,
    pub cap_cause_lsns: usize,
    /// Sticky FIFO of all `chunk.collision.cause_lsn` values seen, bounded
    /// by `cap_cause_lsns`. Drives `is_collision_cause_lsn` and
    /// `collision_chunk_for_lsn` so tinting and "replaced pk in chunk N"
    /// notes on CDC rows in `source_log` outlive the chunk's eviction
    /// from history.
    cause_lsn_fifo: VecDeque<String>,
    cause_lsn_index: std::collections::HashMap<String, u64>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            run_id: None,
            source_id: None,
            first_ts: None,
            last_ts: None,
            last_seq: None,
            seq_gaps: 0,
            events_total: 0,
            cdc_total: 0,
            sink_log_total: 0,
            sink_refresh_total: 0,
            chunks_done: 0,
            collisions_total: 0,
            source_log: VecDeque::new(),
            sink_output: VecDeque::new(),
            active_chunk: None,
            chunks_history: VecDeque::new(),
            latest_request: None,
            latest_error: None,
            latest_heartbeat: None,
            standby: None,
            last_resumed: None,
            cap_source: DEFAULT_CAP_SOURCE,
            cap_sink: DEFAULT_CAP_SINK,
            cap_history: DEFAULT_CAP_HISTORY,
            cap_cause_lsns: DEFAULT_CAP_CAUSE_LSNS,
            cause_lsn_fifo: VecDeque::new(),
            cause_lsn_index: std::collections::HashMap::new(),
        }
    }
}

/// Out-of-band signals that `apply` may return alongside the mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Normal apply, no anomaly.
    Ok,
    /// First envelope of a new run — state was reset.
    NewRun { previous_run_id: String, new_run_id: String },
    /// Seq jumped forward — indicates a drop (at-most-once narrow window).
    Gap { last_seq: u64, got: u64 },
    /// Event dropped because its seq was already seen (duplicate / reorder).
    DroppedDuplicate { seq: u64 },
}

impl State {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one event. Mutates `self`; returns an [`ApplyOutcome`] describing
    /// any anomalies the envelope bookkeeping detected.
    pub fn apply(&mut self, event: TapEvent) -> ApplyOutcome {
        let event_run_id = event.run_id().to_string();
        let ts = event.ts();
        let seq = event.seq();

        let mut outcome = ApplyOutcome::Ok;

        // ---- run_id tracking ----
        match &self.run_id {
            None => {
                self.run_id = Some(event_run_id.clone());
                self.source_id = Some(event.source_id().to_string());
                self.first_ts = Some(ts);
            }
            Some(current) if current != &event_run_id => {
                let previous = current.clone();
                *self = Self {
                    run_id: Some(event_run_id.clone()),
                    source_id: Some(event.source_id().to_string()),
                    first_ts: Some(ts),
                    cap_source: self.cap_source,
                    cap_sink: self.cap_sink,
                    cap_history: self.cap_history,
                    cap_cause_lsns: self.cap_cause_lsns,
                    ..Self::default()
                };
                outcome = ApplyOutcome::NewRun {
                    previous_run_id: previous,
                    new_run_id: event_run_id,
                };
            }
            _ => {}
        }
        self.last_ts = Some(ts);

        // ---- seq tracking (non-OOB only) ----
        if let Some(s) = seq {
            if let Some(last) = self.last_seq {
                if s <= last {
                    return ApplyOutcome::DroppedDuplicate { seq: s };
                }
                if s > last + 1 {
                    self.seq_gaps += 1;
                    // Don't overwrite a NewRun outcome.
                    if matches!(outcome, ApplyOutcome::Ok) {
                        outcome = ApplyOutcome::Gap { last_seq: last, got: s };
                    }
                }
            }
            self.last_seq = Some(s);
        }

        self.events_total += 1;

        // ---- dispatch ----
        match event {
            TapEvent::Cdc(b) => self.on_cdc(b),
            TapEvent::SinkEvent(b) => self.on_sink_event(b),
            TapEvent::WatermarkWritten(b) => self.on_wm_written(b),
            TapEvent::WatermarkReceived(b) => self.on_wm_received(b),
            TapEvent::ChunkSelected(b) => self.on_chunk_selected(b),
            TapEvent::ChunkCollision(b) => self.on_chunk_collision(b),
            TapEvent::ChunkCompleted(b) => self.on_chunk_completed(b),
            // checkpoint.advanced is accepted on the wire but intentionally
            // dropped — producer-side bookkeeping, not reconciler-facing.
            TapEvent::CheckpointAdvanced(_) => {}
            TapEvent::RequestTransition(b) => self.latest_request = Some(b),
            TapEvent::Error(b) => self.latest_error = Some(b),
            TapEvent::StreamHeartbeat(b) => self.latest_heartbeat = Some(b),
            TapEvent::StreamStandby(b) => self.standby = Some(b),
            TapEvent::StreamResumed(b) => {
                self.standby = None;
                self.last_resumed = Some(b);
            }
        }

        outcome
    }

    /// Tap `first_ts` as a convenience so renderers can show `t+X.XXX` deltas
    /// without reaching into the envelope every time.
    pub fn rel_ts(&self, ts: DateTime<Utc>) -> String {
        let Some(anchor) = self.first_ts else {
            return ts.to_rfc3339();
        };
        let delta = ts.signed_duration_since(anchor);
        let total_ms = delta.num_milliseconds();
        let sign = if total_ms < 0 { "-" } else { "" };
        let total_ms = total_ms.unsigned_abs();
        let secs = total_ms / 1_000;
        let ms = total_ms % 1_000;
        format!("{}t+{}.{:03}", sign, secs, ms)
    }

    // -----------------------------------------------------------------------
    // Per-kind handlers
    // -----------------------------------------------------------------------

    fn on_cdc(&mut self, b: CdcBody) {
        self.cdc_total += 1;
        self.push_source(SourceEntry::Cdc(b));
    }

    fn on_sink_event(&mut self, b: SinkEventBody) {
        match b.origin {
            Origin::Log => self.sink_log_total += 1,
            Origin::Select => self.sink_refresh_total += 1,
        }
        if self.sink_output.len() >= self.cap_sink {
            self.sink_output.pop_front();
        }
        self.sink_output.push_back(b);
    }

    fn on_wm_written(&mut self, b: WatermarkWrittenBody) {
        self.ensure_active_chunk(b.chunk_id);
        let chunk = self.active_chunk.as_mut().expect("ensure_active_chunk");
        match b.level {
            WatermarkLevel::Low => {
                chunk.lw_token = Some(b.token.clone());
                chunk.lw_written_ts = Some(b.ts);
            }
            WatermarkLevel::High => {
                chunk.hw_token = Some(b.token.clone());
                chunk.hw_written_ts = Some(b.ts);
            }
        }
        self.push_source(SourceEntry::WatermarkWritten(b));
    }

    fn on_wm_received(&mut self, b: WatermarkReceivedBody) {
        self.ensure_active_chunk(b.chunk_id);
        let chunk = self.active_chunk.as_mut().expect("ensure_active_chunk");
        match b.level {
            WatermarkLevel::Low => {
                chunk.lw_received_ts = Some(b.ts);
                chunk.lw_received_lsn = Some(b.lsn.clone());
                chunk.lw_latency_ms = b.latency_ms;
            }
            WatermarkLevel::High => {
                chunk.hw_received_ts = Some(b.ts);
                chunk.hw_received_lsn = Some(b.lsn.clone());
                chunk.hw_latency_ms = b.latency_ms;
            }
        }
        self.push_source(SourceEntry::WatermarkReceived(b));
    }

    fn on_chunk_selected(&mut self, b: ChunkSelectedBody) {
        self.ensure_active_chunk(b.chunk_id);
        let chunk = self.active_chunk.as_mut().expect("ensure_active_chunk");
        chunk.request_id = Some(b.request_id.clone());
        chunk.dump_id = Some(b.dump_id.clone());
        chunk.table = Some(b.table.clone());
        chunk.pk_min = b.pk_min.clone();
        chunk.pk_max = b.pk_max.clone();
        chunk.start_after_pk = b.start_after_pk.clone();
        chunk.row_count = Some(b.row_count);
        chunk.final_chunk = Some(b.final_chunk);
        chunk.fingerprint = Some(b.fingerprint.clone());
        self.push_source(SourceEntry::ChunkSelected(b));
    }

    fn on_chunk_collision(&mut self, b: ChunkCollisionBody) {
        self.collisions_total += 1;
        self.ensure_active_chunk(b.chunk_id);
        let chunk = self.active_chunk.as_mut().expect("ensure_active_chunk");
        chunk.excluded += 1;
        if let Some(ref pk) = b.excluded_pk {
            chunk.excluded_pks.push(pk.clone());
        } else {
            // Schema guarantees excluded_pk only when a row-image with a
            // captured schema is available. Synthesize a stable placeholder
            // so the reconciler's "excluded" list stays consistent with the
            // per-chunk `excluded` counter instead of silently diverging.
            chunk.excluded_pks.push(format!("«no schema» ({})", b.cause_lsn));
        }
        chunk.cause_lsns.insert(b.cause_lsn.clone());
        // Sticky global index — see `is_collision_cause_lsn` /
        // `collision_chunk_for_lsn`. Keeps the FIFO and map in lockstep
        // so eviction drops the right key.
        if !self.cause_lsn_index.contains_key(&b.cause_lsn) {
            self.cause_lsn_index.insert(b.cause_lsn.clone(), b.chunk_id);
            self.cause_lsn_fifo.push_back(b.cause_lsn.clone());
            if self.cause_lsn_fifo.len() > self.cap_cause_lsns {
                if let Some(evicted) = self.cause_lsn_fifo.pop_front() {
                    self.cause_lsn_index.remove(&evicted);
                }
            }
        }
        self.push_source(SourceEntry::ChunkCollision(b));
    }

    fn on_chunk_completed(&mut self, b: ChunkCompletedBody) {
        self.chunks_done += 1;
        let (lw_lsn, cause_lsns) = self
            .active_chunk
            .as_ref()
            .filter(|c| c.chunk_id == b.chunk_id)
            .map(|c| (c.lw_received_lsn.clone(), c.cause_lsns.clone()))
            .unwrap_or_default();
        let summary = ChunkSummary {
            chunk_id: b.chunk_id,
            request_id: b.request_id.clone(),
            table: b.table.clone(),
            emitted: b.emitted,
            excluded: b.excluded,
            duration_ms: b.duration_ms,
            final_chunk: b.final_chunk,
            completed_ts: b.ts,
            lw_lsn,
            completed_lsn: b.lsn.clone(),
            cause_lsns,
        };
        if self.chunks_history.len() >= self.cap_history {
            self.chunks_history.pop_front();
        }
        self.chunks_history.push_back(summary);
        // Clear any active_chunk — the completion closes the window. When the
        // chunk_id matches, this is the normal path. When it doesn't (a
        // malformed/reordered stream), treating the in-flight chunk as
        // abandoned is safer than leaving it pinned forever on the reconciler.
        self.active_chunk = None;
        self.push_source(SourceEntry::ChunkCompleted(b));
    }

    /// True if `lsn` was recorded as a `chunk.collision.cause_lsn` in any
    /// window currently trackable by the UI — either on the active chunk,
    /// in the chunks_history ring (cap 32), or in the sticky cause-LSN
    /// index (cap 2000) that outlives chunks_history. Used by renderers
    /// to tint CDC / sink.event rows consistently.
    pub fn is_collision_cause_lsn(&self, lsn: &str) -> bool {
        self.cause_lsn_index.contains_key(lsn)
    }

    /// Chunk id whose refresh buffer `lsn` evicted a row from, if any.
    /// Same lookup scope as `is_collision_cause_lsn` (active + history +
    /// sticky 2000-entry index). Used by the source pane to render
    /// "replaced pk in chunk N" on the collision-triggering CDC row.
    pub fn collision_chunk_for_lsn(&self, lsn: &str) -> Option<u64> {
        self.cause_lsn_index.get(lsn).copied()
    }

    fn ensure_active_chunk(&mut self, chunk_id: u64) {
        match &self.active_chunk {
            Some(c) if c.chunk_id == chunk_id => {}
            _ => {
                self.active_chunk = Some(ActiveChunk {
                    chunk_id,
                    ..ActiveChunk::default()
                });
            }
        }
    }

    fn push_source(&mut self, entry: SourceEntry) {
        if self.source_log.len() >= self.cap_source {
            self.source_log.pop_front();
        }
        self.source_log.push_back(entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::demo::chunk42_scenario;

    fn mk_cdc(seq: u64, lsn: &str, pk: &str) -> TapEvent {
        TapEvent::Cdc(CdcBody {
            v: 1,
            seq: Some(seq),
            ts: Utc::now(),
            run_id: "run".into(),
            source_id: "src".into(),
            lsn: lsn.into(),
            op: Op::Update,
            table: "t".into(),
            pk: pk.into(),
            tx_id: None,
        })
    }

    #[test]
    fn first_event_seeds_connection_identity() {
        let mut s = State::new();
        let out = s.apply(mk_cdc(5, "l", "p"));
        assert_eq!(out, ApplyOutcome::Ok);
        assert_eq!(s.run_id.as_deref(), Some("run"));
        assert_eq!(s.source_id.as_deref(), Some("src"));
        assert_eq!(s.last_seq, Some(5));
        assert_eq!(s.cdc_total, 1);
    }

    #[test]
    fn new_run_id_resets_state() {
        let mut s = State::new();
        s.apply(mk_cdc(5, "l", "p"));
        let other = TapEvent::Cdc(CdcBody {
            v: 1, seq: Some(1),
            ts: Utc::now(),
            run_id: "run-2".into(),
            source_id: "src".into(),
            lsn: "l".into(),
            op: Op::Insert,
            table: "t".into(),
            pk: "x".into(),
            tx_id: None,
        });
        let out = s.apply(other);
        match out {
            ApplyOutcome::NewRun { previous_run_id, new_run_id } => {
                assert_eq!(previous_run_id, "run");
                assert_eq!(new_run_id, "run-2");
            }
            other => panic!("expected NewRun, got {:?}", other),
        }
        assert_eq!(s.run_id.as_deref(), Some("run-2"));
        assert_eq!(s.last_seq, Some(1));
        assert_eq!(s.cdc_total, 1);
    }

    #[test]
    fn duplicate_seq_is_dropped() {
        let mut s = State::new();
        s.apply(mk_cdc(5, "l", "p"));
        let out = s.apply(mk_cdc(5, "l2", "p2"));
        assert_eq!(out, ApplyOutcome::DroppedDuplicate { seq: 5 });
        assert_eq!(s.cdc_total, 1);
    }

    #[test]
    fn seq_gap_is_recorded_but_applied() {
        let mut s = State::new();
        s.apply(mk_cdc(1, "l", "p"));
        let out = s.apply(mk_cdc(5, "l", "p"));
        assert_eq!(out, ApplyOutcome::Gap { last_seq: 1, got: 5 });
        assert_eq!(s.cdc_total, 2);
        assert_eq!(s.seq_gaps, 1);
    }

    #[test]
    fn oob_events_do_not_advance_seq() {
        let mut s = State::new();
        s.apply(mk_cdc(10, "l", "p"));
        let standby = TapEvent::StreamStandby(StreamStandbyBody {
            v: 1, seq: None, ts: Utc::now(),
            run_id: "run".into(), source_id: "src".into(),
            queue_full_ms: 1200, queue_capacity: 65536,
            reason: StandbyReason::QueueFull,
            message: "stall".into(),
        });
        s.apply(standby);
        assert!(s.standby.is_some());
        assert_eq!(s.last_seq, Some(10));
        let resume = TapEvent::StreamResumed(StreamResumedBody {
            v: 1, seq: None, ts: Utc::now(),
            run_id: "run".into(), source_id: "src".into(),
            standby_total_ms: 1400,
        });
        s.apply(resume);
        assert!(s.standby.is_none());
        assert!(s.last_resumed.is_some());
    }

    #[test]
    fn chunk42_scenario_produces_expected_state() {
        let mut s = State::new();
        for ev in chunk42_scenario() {
            s.apply(ev);
        }
        // Window opened and closed, chunk marked done
        assert_eq!(s.chunks_done, 1);
        assert!(s.active_chunk.is_none(), "chunk should be closed");
        // Two collisions recorded and the excluded_pks propagate into history
        assert_eq!(s.collisions_total, 2);
        let done = s.chunks_history.front().expect("one completed chunk");
        assert_eq!(done.chunk_id, 42);
        assert_eq!(done.emitted, 38);
        assert_eq!(done.excluded, 2);
        // Sink rows split correctly
        assert!(s.sink_log_total >= 8, "had {} LOG sink events", s.sink_log_total);
        assert!(s.sink_refresh_total >= 8);
        // Request transition seen
        assert!(s.latest_request.is_some());
    }

    #[test]
    fn replay_is_idempotent_under_duplicates() {
        let events: Vec<TapEvent> = chunk42_scenario();
        let mut base = State::new();
        for ev in events.iter().cloned() { base.apply(ev); }
        // Replay with duplicates mixed in
        let mut dup = State::new();
        for ev in events.iter().cloned() {
            dup.apply(ev.clone());
            dup.apply(ev);
        }
        assert_eq!(base.cdc_total, dup.cdc_total);
        assert_eq!(base.collisions_total, dup.collisions_total);
        assert_eq!(base.chunks_done, dup.chunks_done);
    }

    #[test]
    fn collision_tint_survives_history_eviction() {
        // Sticky cause-LSN index must outlive chunks_history (cap 32). A
        // CDC still sitting in source_log must keep its collision tint
        // after its originating chunk has aged out.
        let now = Utc::now();
        let mut s = State::new();
        // Chunk 1 registers a collision with a specific LSN.
        s.apply(TapEvent::WatermarkWritten(WatermarkWrittenBody {
            v: 1, seq: Some(1), ts: now,
            run_id: "r".into(), source_id: "s".into(),
            level: WatermarkLevel::Low, token: "t".into(), chunk_id: 1,
        }));
        s.apply(TapEvent::ChunkCollision(ChunkCollisionBody {
            v: 1, seq: Some(2), ts: now,
            run_id: "r".into(), source_id: "s".into(),
            chunk_id: 1, excluded_pk: Some("x".into()),
            cause_lsn: "lsn-sticky".into(), cause_op: Op::Update,
            cause_tx_id: None,
        }));
        s.apply(TapEvent::ChunkCompleted(ChunkCompletedBody {
            v: 1, seq: Some(3), ts: now,
            run_id: "r".into(), source_id: "s".into(),
            chunk_id: 1, request_id: "r".into(), table: "t".into(),
            emitted: 0, excluded: 1, last_pk: None,
            lsn: "end-1".into(), duration_ms: 1, final_chunk: false,
        }));
        assert!(s.is_collision_cause_lsn("lsn-sticky"));

        // Push enough chunks to evict chunk 1 from chunks_history (cap 32).
        for i in 2u64..=s.cap_history as u64 + 5 {
            let base = i * 100;
            s.apply(TapEvent::WatermarkWritten(WatermarkWrittenBody {
                v: 1, seq: Some(base + 1), ts: now,
                run_id: "r".into(), source_id: "s".into(),
                level: WatermarkLevel::Low, token: "t".into(), chunk_id: i,
            }));
            s.apply(TapEvent::ChunkCompleted(ChunkCompletedBody {
                v: 1, seq: Some(base + 2), ts: now,
                run_id: "r".into(), source_id: "s".into(),
                chunk_id: i, request_id: "r".into(), table: "t".into(),
                emitted: 0, excluded: 0, last_pk: None,
                lsn: format!("end-{}", i), duration_ms: 1, final_chunk: false,
            }));
        }
        assert_eq!(s.chunks_history.len(), s.cap_history);
        // Chunk 1 is now gone from chunks_history — but the sticky index
        // still recognises its cause_lsn.
        assert!(s.is_collision_cause_lsn("lsn-sticky"));
    }
}
