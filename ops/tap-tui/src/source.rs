//! Event sources — live HTTP NDJSON tap, and offline demo replay.
//!
//! Both sources run on a dedicated reader thread and push
//! [`SourceMessage`]s into a channel that the UI thread polls. The reader
//! thread calls [`Pacer::wait`] between events, so continuous delay / step
//! mode live on this side of the pipeline — a slow reader backpressures the
//! socket, which backpressures the DBLog pump, which eventually emits a
//! `stream.standby` out-of-band event. That's the whole educational
//! mechanism.
//!
//! Live mode also implements a minimal reconnect loop: on `io::Error`,
//! publish `Disconnected { .. }`, wait `RECONNECT_BACKOFF`, try again. The
//! consumer state machine detects a new `run_id` at apply time and resets
//! downstream state.

use crate::event::{parse_line, LineParse, TapEvent};
use crate::pacing::Pacer;
use anyhow::{anyhow, Result};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

const RECONNECT_BACKOFF: Duration = Duration::from_millis(750);

/// Messages the reader thread may push to the UI thread.
#[derive(Debug, Clone)]
pub enum SourceMessage {
    /// Connection attempt succeeded (live) or the demo source started.
    Connected { label: String },
    /// Connection dropped; the reader will retry after a short backoff.
    Disconnected { reason: String },
    /// The remote side closed the stream cleanly. This is expected when a tap
    /// subscriber is displaced by a newer connection or when DBLog shuts down.
    Closed { reason: String },
    /// A parsed envelope.
    Event(TapEvent),
    /// A parse error on one line — the line is skipped and reading continues.
    ParseError { line_preview: String, error: String },
    /// The source has been asked to stop and the thread is exiting cleanly.
    Finished,
}

/// Start reading the live DBLog tap at `url`. The thread loops until
/// `shutdown` flips to `true` or `tx`'s receiver is dropped.
pub fn spawn_http(
    url: String,
    pacer: Pacer,
    shutdown: Arc<AtomicBool>,
    tx: SyncSender<SourceMessage>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("tap-reader-http".into())
        .spawn(move || run_http(url, pacer, shutdown, tx))
        .expect("failed to spawn reader thread")
}

/// Replay a static event list in seq order, honouring `pacer` between events.
/// Emits `Finished` and exits after the last event. The intent is the demo
/// mode behave the same as live: same types, same state machine, same
/// renderers — just with a deterministic source.
///
/// `label` is the string the reader announces via `SourceMessage::Connected`;
/// it shows up on the TUI banner and in screenshots. Kept parameterised so
/// scenario variants (e.g. `chunk42-standby`) can disambiguate themselves
/// rather than all masquerading under one pseudo-URL.
pub fn spawn_demo(
    label: String,
    events: Vec<TapEvent>,
    pacer: Pacer,
    shutdown: Arc<AtomicBool>,
    tx: SyncSender<SourceMessage>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("tap-reader-demo".into())
        .spawn(move || run_demo(label, events, pacer, shutdown, tx))
        .expect("failed to spawn reader thread")
}

/// Run the procedural showcase generator on its own thread — scripted intro
/// followed by an infinite continuous loop. See `crate::showcase`.
pub fn spawn_showcase(
    pacer: Pacer,
    shutdown: Arc<AtomicBool>,
    tx: SyncSender<SourceMessage>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("tap-reader-showcase".into())
        .spawn(move || crate::showcase::run(pacer, shutdown, tx))
        .expect("failed to spawn reader thread")
}

/// Same as [`spawn_showcase`] but dressed with long fully-qualified
/// table names and composite LONG-typed primary keys. See
/// `crate::showcase::run_long`.
pub fn spawn_showcase_long(
    pacer: Pacer,
    shutdown: Arc<AtomicBool>,
    tx: SyncSender<SourceMessage>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("tap-reader-showcase-long".into())
        .spawn(move || crate::showcase::run_long(pacer, shutdown, tx))
        .expect("failed to spawn reader thread")
}

fn run_http(url: String, pacer: Pacer, shutdown: Arc<AtomicBool>, tx: SyncSender<SourceMessage>) {
    while !shutdown.load(Ordering::Acquire) {
        match connect_and_read(&url, &pacer, &shutdown, &tx) {
            Ok(()) => {
                // `connect_and_read` returns Ok only on shutdown.
                let _ = tx.send(SourceMessage::Finished);
                return;
            }
            Err(err) if err.to_string() == "server closed connection" => {
                let _ = tx.send(SourceMessage::Closed {
                    reason: err.to_string(),
                });
                let _ = tx.send(SourceMessage::Finished);
                return;
            }
            Err(err) => {
                let _ = tx.send(SourceMessage::Disconnected {
                    reason: err.to_string(),
                });
                for _ in 0..(RECONNECT_BACKOFF.as_millis() as u64 / 50 + 1) {
                    if shutdown.load(Ordering::Acquire) {
                        let _ = tx.send(SourceMessage::Finished);
                        return;
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
    let _ = tx.send(SourceMessage::Finished);
}

fn connect_and_read(
    url: &str,
    pacer: &Pacer,
    shutdown: &Arc<AtomicBool>,
    tx: &SyncSender<SourceMessage>,
) -> Result<()> {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(3))
        .build();
    let response = match agent
        .get(url)
        .set("Accept", "application/x-ndjson")
        .call()
    {
        Ok(r) => r,
        Err(ureq::Error::Status(503, r)) => {
            let body = r.into_string().unwrap_or_default();
            return Err(anyhow!(
                "tap returned 503 (not enabled?): {}",
                body.trim()
            ));
        }
        Err(ureq::Error::Status(code, _)) => {
            return Err(anyhow!("tap returned HTTP {}", code));
        }
        Err(ureq::Error::Transport(t)) => {
            return Err(anyhow!("transport error: {}", t));
        }
    };

    if tx
        .send(SourceMessage::Connected {
            label: url.to_string(),
        })
        .is_err()
    {
        return Ok(()); // receiver gone
    }

    let reader = BufReader::new(response.into_reader());
    for line_result in reader.lines() {
        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }
        let line = match line_result {
            Ok(l) => l,
            Err(e) => return Err(anyhow!("read error: {}", e)),
        };
        if line.is_empty() {
            continue;
        }
        match parse_line(&line) {
            LineParse::Event(ev) => {
                if tx.send(SourceMessage::Event(ev)).is_err() {
                    return Ok(());
                }
                pacer.wait();
            }
            LineParse::UnknownKind { .. } => {
                // Forward-compat: the schema catalogue grew a new kind.
                // Per docs/CONTROL_PLANE.md §5.4, consumers skip rather
                // than error. No banner — that would make every new
                // server-side event fire a warning on every old client.
            }
            LineParse::Malformed(err) => {
                let preview: String = line.chars().take(160).collect();
                if tx
                    .send(SourceMessage::ParseError {
                        line_preview: preview,
                        error: err.to_string(),
                    })
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
    }
    Err(anyhow!("server closed connection"))
}

fn run_demo(
    label: String,
    events: Vec<TapEvent>,
    pacer: Pacer,
    shutdown: Arc<AtomicBool>,
    tx: SyncSender<SourceMessage>,
) {
    let _ = tx.send(SourceMessage::Connected { label });
    for event in events {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        if tx.send(SourceMessage::Event(event)).is_err() {
            return;
        }
        pacer.wait();
    }
    let _ = tx.send(SourceMessage::Finished);
}

