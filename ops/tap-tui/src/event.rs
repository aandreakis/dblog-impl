//! Tap wire types.
//!
//! Mirrors the JSON-Schema event catalog at `docs/schema/events/*.schema.json`
//! in the DBLog repo. Serde-tagged enum discriminates on `kind`; each variant
//! carries a body struct whose fields are the per-kind schema's required +
//! optional fields, with `#[serde(default)]` on anything the schema marks
//! non-required.
//!
//! Consumer rules (from `docs/CONTROL_PLANE.md § 5.4`):
//!
//! - ignore unknown top-level keys (forward compat)
//! - skip unknown `kind` values rather than erroring — deserialisation into
//!   this enum fails for unknown kinds; callers should treat a parse failure
//!   on a line as "unknown kind, skip"
//! - `@JsonInclude(NON_NULL)` on the server: optional fields are OMITTED on
//!   the wire, not emitted as `null`. `#[serde(default)]` on our side handles
//!   that correctly
//! - `seq` is `Option<u64>` — null on the two out-of-band kinds
//!   (`stream.standby`, `stream.resumed`)
//! - `ts` always has exactly six fractional digits; `DateTime<Utc>` parses fine

use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Op {
    Insert,
    Update,
    Delete,
}

impl Op {
    /// Three-letter display verb (INS/UPD/DEL).
    pub fn short(self) -> &'static str {
        match self {
            Op::Insert => "INS",
            Op::Update => "UPD",
            Op::Delete => "DEL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum WatermarkLevel {
    Low,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Origin {
    Log,
    Select,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RequestScope {
    Table,
    PrimaryKeys,
    AllTables,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RequestState {
    Active,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckpointReason {
    Count,
    Time,
    Bytes,
    Boundary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChunkMode {
    Range,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StandbyReason {
    QueueFull,
}

// ---------------------------------------------------------------------------
// Per-kind bodies. Envelope fields (v, seq, ts, run_id, source_id) are inlined
// on each body. `kind` is the enum tag, not a body field.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct CdcBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub lsn: String,
    pub op: Op,
    pub table: String,
    pub pk: String,
    #[serde(default)]
    pub tx_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkEventBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub lsn: String,
    pub op: Op,
    pub table: String,
    pub pk: String,
    pub origin: Origin,
    #[serde(default)]
    pub tx_id: Option<String>,
    #[serde(default)]
    pub dump_id: Option<String>,
    #[serde(default)]
    pub chunk_id: Option<u64>,
    pub sink_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WatermarkWrittenBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub level: WatermarkLevel,
    pub token: String,
    pub chunk_id: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WatermarkReceivedBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub level: WatermarkLevel,
    pub token: String,
    pub chunk_id: u64,
    pub lsn: String,
    #[serde(default)]
    pub latency_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkSelectedBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub chunk_id: u64,
    pub request_id: String,
    pub dump_id: String,
    pub table: String,
    pub mode: ChunkMode,
    #[serde(default)]
    pub pk_min: Option<String>,
    #[serde(default)]
    pub pk_max: Option<String>,
    #[serde(default)]
    pub start_after_pk: Option<String>,
    pub row_count: u64,
    pub final_chunk: bool,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkCollisionBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub chunk_id: u64,
    #[serde(default)]
    pub excluded_pk: Option<String>,
    pub cause_lsn: String,
    pub cause_op: Op,
    #[serde(default)]
    pub cause_tx_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkCompletedBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub chunk_id: u64,
    pub request_id: String,
    pub table: String,
    pub emitted: u64,
    pub excluded: u64,
    #[serde(default)]
    pub last_pk: Option<String>,
    pub lsn: String,
    pub duration_ms: u64,
    pub final_chunk: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CheckpointAdvancedBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub position: String,
    pub buffered_events: u64,
    pub reason: CheckpointReason,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RequestTransitionBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub request_id: String,
    pub scope: RequestScope,
    #[serde(default)]
    pub table: Option<String>,
    pub state: RequestState,
    #[serde(default)]
    pub prev_state: Option<RequestState>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ErrorBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub class: String,
    pub message: String,
    #[serde(default)]
    pub context: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamHeartbeatBody {
    pub v: u32,
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub queue_depth: u64,
    pub queue_capacity: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamStandbyBody {
    pub v: u32,
    /// Always `None` — `stream.standby` is out-of-band.
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub queue_full_ms: u64,
    pub queue_capacity: u64,
    pub reason: StandbyReason,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamResumedBody {
    pub v: u32,
    /// Always `None` — `stream.resumed` is out-of-band.
    pub seq: Option<u64>,
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub source_id: String,
    pub standby_total_ms: u64,
}

// ---------------------------------------------------------------------------
// The tagged enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind")]
pub enum TapEvent {
    #[serde(rename = "cdc")]
    Cdc(CdcBody),
    #[serde(rename = "sink.event")]
    SinkEvent(SinkEventBody),
    #[serde(rename = "watermark.written")]
    WatermarkWritten(WatermarkWrittenBody),
    #[serde(rename = "watermark.received")]
    WatermarkReceived(WatermarkReceivedBody),
    #[serde(rename = "chunk.selected")]
    ChunkSelected(ChunkSelectedBody),
    #[serde(rename = "chunk.collision")]
    ChunkCollision(ChunkCollisionBody),
    #[serde(rename = "chunk.completed")]
    ChunkCompleted(ChunkCompletedBody),
    #[serde(rename = "checkpoint.advanced")]
    CheckpointAdvanced(CheckpointAdvancedBody),
    #[serde(rename = "request.transition")]
    RequestTransition(RequestTransitionBody),
    #[serde(rename = "error")]
    Error(ErrorBody),
    #[serde(rename = "stream.heartbeat")]
    StreamHeartbeat(StreamHeartbeatBody),
    #[serde(rename = "stream.standby")]
    StreamStandby(StreamStandbyBody),
    #[serde(rename = "stream.resumed")]
    StreamResumed(StreamResumedBody),
}

macro_rules! delegate {
    ($self:expr, $field:ident) => {
        match $self {
            TapEvent::Cdc(b) => &b.$field,
            TapEvent::SinkEvent(b) => &b.$field,
            TapEvent::WatermarkWritten(b) => &b.$field,
            TapEvent::WatermarkReceived(b) => &b.$field,
            TapEvent::ChunkSelected(b) => &b.$field,
            TapEvent::ChunkCollision(b) => &b.$field,
            TapEvent::ChunkCompleted(b) => &b.$field,
            TapEvent::CheckpointAdvanced(b) => &b.$field,
            TapEvent::RequestTransition(b) => &b.$field,
            TapEvent::Error(b) => &b.$field,
            TapEvent::StreamHeartbeat(b) => &b.$field,
            TapEvent::StreamStandby(b) => &b.$field,
            TapEvent::StreamResumed(b) => &b.$field,
        }
    };
}

/// Outcome of a lenient parse: either a well-formed event, a line whose
/// `kind` we don't recognise (forward-compat skip), or a hard parse error
/// that the UI should surface to the operator.
#[derive(Debug)]
pub enum LineParse {
    Event(TapEvent),
    UnknownKind { kind: String },
    Malformed(serde_json::Error),
}

/// Set of `kind` values this consumer knows how to decode. Mirrors the
/// `#[serde(rename)]` tags on `TapEvent`. Used by `parse_line` so lines with
/// new-to-us kinds skip silently (per `docs/CONTROL_PLANE.md §5.4`).
pub const KNOWN_KINDS: &[&str] = &[
    "cdc",
    "sink.event",
    "watermark.written",
    "watermark.received",
    "chunk.selected",
    "chunk.collision",
    "chunk.completed",
    "checkpoint.advanced",
    "request.transition",
    "error",
    "stream.heartbeat",
    "stream.standby",
    "stream.resumed",
];

/// Lenient NDJSON-line parser. Peeks at `kind`; if unknown, returns
/// `UnknownKind` so the caller can skip without raising a parse-error
/// banner. Malformed JSON or schema mismatches bubble up as `Malformed`,
/// including the two contract checks the serde types cannot enforce:
///   - seq MUST be integer on every non-OOB kind (schema uses `Option`
///     there only because seq is only omitted on the two OOB kinds).
///   - seq MUST be null/missing on `stream.standby` and `stream.resumed`.
pub fn parse_line(line: &str) -> LineParse {
    #[derive(Deserialize)]
    struct KindPeek<'a> {
        #[serde(borrow)]
        kind: Option<std::borrow::Cow<'a, str>>,
    }
    if let Ok(peek) = serde_json::from_str::<KindPeek<'_>>(line) {
        if let Some(k) = peek.kind.as_deref() {
            if !KNOWN_KINDS.contains(&k) {
                return LineParse::UnknownKind { kind: k.to_string() };
            }
        }
    }
    match serde_json::from_str::<TapEvent>(line) {
        Ok(ev) => {
            let is_oob = ev.is_oob();
            let seq = ev.seq();
            if !is_oob && seq.is_none() {
                return LineParse::Malformed(
                    serde::de::Error::custom(format!(
                        "non-OOB event {:?} is missing required `seq` field",
                        ev.kind()
                    )),
                );
            }
            if is_oob && seq.is_some() {
                return LineParse::Malformed(
                    serde::de::Error::custom(format!(
                        "OOB event {:?} must have null/missing `seq` (got {:?})",
                        ev.kind(), seq
                    )),
                );
            }
            LineParse::Event(ev)
        }
        Err(e) => LineParse::Malformed(e),
    }
}

impl TapEvent {
    pub fn seq(&self) -> Option<u64> {
        *delegate!(self, seq)
    }
    pub fn ts(&self) -> DateTime<Utc> {
        *delegate!(self, ts)
    }
    pub fn run_id(&self) -> &str {
        delegate!(self, run_id)
    }
    pub fn source_id(&self) -> &str {
        delegate!(self, source_id)
    }
    /// Short kind label for renderers; mirrors the wire `kind` string.
    pub fn kind(&self) -> &'static str {
        match self {
            TapEvent::Cdc(_) => "cdc",
            TapEvent::SinkEvent(_) => "sink.event",
            TapEvent::WatermarkWritten(_) => "watermark.written",
            TapEvent::WatermarkReceived(_) => "watermark.received",
            TapEvent::ChunkSelected(_) => "chunk.selected",
            TapEvent::ChunkCollision(_) => "chunk.collision",
            TapEvent::ChunkCompleted(_) => "chunk.completed",
            TapEvent::CheckpointAdvanced(_) => "checkpoint.advanced",
            TapEvent::RequestTransition(_) => "request.transition",
            TapEvent::Error(_) => "error",
            TapEvent::StreamHeartbeat(_) => "stream.heartbeat",
            TapEvent::StreamStandby(_) => "stream.standby",
            TapEvent::StreamResumed(_) => "stream.resumed",
        }
    }
    /// Is this event out-of-band (no seq, written by the HTTP writer thread)?
    pub fn is_oob(&self) -> bool {
        matches!(self, TapEvent::StreamStandby(_) | TapEvent::StreamResumed(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialise_cdc() {
        let line = r#"{"v":1,"kind":"cdc","seq":42,"ts":"2026-04-20T12:07:22.421591Z","run_id":"run-1","source_id":"src","lsn":"binlog.000042:12345","op":"INSERT","table":"app.orders","pk":"2044","tx_id":"tx-a912"}"#;
        let e: TapEvent = serde_json::from_str(line).unwrap();
        match e {
            TapEvent::Cdc(b) => {
                assert_eq!(b.seq, Some(42));
                assert_eq!(b.op, Op::Insert);
                assert_eq!(b.pk, "2044");
                assert_eq!(b.tx_id.as_deref(), Some("tx-a912"));
            }
            other => panic!("unexpected variant: {:?}", other),
        }
    }

    #[test]
    fn deserialise_sink_event_select() {
        let line = r#"{"v":1,"kind":"sink.event","seq":100,"ts":"2026-04-20T12:07:23.000000Z","run_id":"r","source_id":"s","lsn":"binlog.000042:1920","op":"UPDATE","table":"app.orders","pk":"1001","origin":"SELECT","dump_id":"r-17","chunk_id":42,"sink_name":"ndjson"}"#;
        let e: TapEvent = serde_json::from_str(line).unwrap();
        match e {
            TapEvent::SinkEvent(b) => {
                assert_eq!(b.origin, Origin::Select);
                assert_eq!(b.chunk_id, Some(42));
                assert_eq!(b.dump_id.as_deref(), Some("r-17"));
                assert!(b.tx_id.is_none());
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn deserialise_stream_standby_with_null_seq() {
        let line = r#"{"v":1,"kind":"stream.standby","seq":null,"ts":"2026-04-20T12:07:24.000000Z","run_id":"r","source_id":"s","queue_full_ms":1250,"queue_capacity":65536,"reason":"queue_full","message":"DBLog is on standby — waiting for the reader to advance"}"#;
        let e: TapEvent = serde_json::from_str(line).unwrap();
        match e {
            TapEvent::StreamStandby(b) => {
                assert!(b.seq.is_none());
                assert_eq!(b.queue_full_ms, 1250);
                assert_eq!(b.reason, StandbyReason::QueueFull);
            }
            other => panic!("unexpected: {:?}", other),
        }
        assert!(serde_json::from_str::<TapEvent>(line).unwrap().is_oob());
    }

    #[test]
    fn deserialise_request_transition_with_missing_prev_state() {
        let line = r#"{"v":1,"kind":"request.transition","seq":5,"ts":"2026-04-20T12:07:20.000000Z","run_id":"r","source_id":"s","request_id":"17","scope":"TABLE","table":"app.orders","state":"ACTIVE"}"#;
        let e: TapEvent = serde_json::from_str(line).unwrap();
        match e {
            TapEvent::RequestTransition(b) => {
                assert_eq!(b.scope, RequestScope::Table);
                assert_eq!(b.state, RequestState::Active);
                assert!(b.prev_state.is_none());
                assert!(b.reason.is_none());
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn unknown_kind_fails_to_deserialise() {
        let line = r#"{"v":1,"kind":"some.future.kind","seq":5,"ts":"2026-04-20T12:07:20.000000Z","run_id":"r","source_id":"s"}"#;
        assert!(serde_json::from_str::<TapEvent>(line).is_err());
    }

    #[test]
    fn parse_line_skips_unknown_kind_without_error() {
        // Forward-compat: a new server-side kind must not trip the
        // consumer's "malformed" banner. It should parse to a silent
        // "UnknownKind" outcome so the reader can skip and keep reading.
        let line = r#"{"v":1,"kind":"some.future.kind","seq":5,"ts":"2026-04-20T12:07:20.000000Z","run_id":"r","source_id":"s"}"#;
        match parse_line(line) {
            LineParse::UnknownKind { kind } => assert_eq!(kind, "some.future.kind"),
            other => panic!("expected UnknownKind, got {:?}", other),
        }
        // Malformed JSON (unclosed brace) still surfaces as Malformed.
        let bad = r#"{"v":1,"kind":"cdc""#;
        assert!(matches!(parse_line(bad), LineParse::Malformed(_)));
        // Well-formed known-kind still produces Event.
        let ok = r#"{"v":1,"kind":"cdc","seq":1,"ts":"2026-04-20T12:07:22.000000Z","run_id":"r","source_id":"s","lsn":"l","op":"INSERT","table":"t","pk":"p"}"#;
        assert!(matches!(parse_line(ok), LineParse::Event(TapEvent::Cdc(_))));
    }

    #[test]
    fn parse_line_rejects_seq_contract_violations() {
        // Non-OOB kind with null seq — server contract violation.
        let null_seq = r#"{"v":1,"kind":"cdc","seq":null,"ts":"2026-04-20T12:07:22.000000Z","run_id":"r","source_id":"s","lsn":"l","op":"INSERT","table":"t","pk":"p"}"#;
        assert!(matches!(parse_line(null_seq), LineParse::Malformed(_)));
        // Non-OOB kind with missing seq — same violation.
        let no_seq = r#"{"v":1,"kind":"cdc","ts":"2026-04-20T12:07:22.000000Z","run_id":"r","source_id":"s","lsn":"l","op":"INSERT","table":"t","pk":"p"}"#;
        assert!(matches!(parse_line(no_seq), LineParse::Malformed(_)));
        // OOB kind with a seq value — also a violation (schema pins seq to null).
        let oob_with_seq = r#"{"v":1,"kind":"stream.standby","seq":5,"ts":"2026-04-20T12:07:22.000000Z","run_id":"r","source_id":"s","queue_full_ms":1,"queue_capacity":1,"reason":"queue_full","message":""}"#;
        assert!(matches!(parse_line(oob_with_seq), LineParse::Malformed(_)));
        // OOB with null seq — the schema-compliant shape.
        let oob_ok = r#"{"v":1,"kind":"stream.standby","seq":null,"ts":"2026-04-20T12:07:22.000000Z","run_id":"r","source_id":"s","queue_full_ms":1,"queue_capacity":1,"reason":"queue_full","message":""}"#;
        assert!(matches!(parse_line(oob_ok), LineParse::Event(TapEvent::StreamStandby(_))));
    }
}
