//! DBLog tap TUI — shared library used by the `hydroscope` binary.
//! Contains the wire types, consumer state machine, event sources (live
//! HTTP + offline demo), pacing controller, and the shared runner that
//! the bin plugs its `render` function into.
//!
//! Layering:
//!
//! ```text
//! +------------+     +-------+     +--------+     +-------+
//! | source.rs  | --> | state | --> | render | --> | bins  |
//! +------------+     +-------+     +--------+     +-------+
//!       ^
//!       | pacer
//!       |
//!   pacing.rs  <-- keyboard --- run_app
//! ```
//!
//! # Colour semantics (hydroscope Steel Navy palette)
//!
//! Colours carry consistent meaning:
//!
//! * **BLUE** — "flowing / complete / healthy". CDC events, connected
//!   banner, LW received, HW received on CLOSED chunk, final=yes.
//! * **YELLOW** — "anomaly / pending". Collision triggers, HW-pending
//!   marker on an open chunk, STANDBY banner, excluded pk.
//! * **CYAN** — "process state". Sink events with `origin=SELECT`
//!   (snapshot refresh), chunk `SELECTED` state, request lifecycle.
//! * **RED** — "hard signal". Low-watermark marker on the ruler,
//!   DISCONNECTED / ERROR banners. Reserved for things that need
//!   immediate attention.
//! * **GRAY / dim variants** — chrome, orientation aids, labels,
//!   and "no data yet" placeholders. Should never be the eye's first
//!   stop.
//!
//! If you're adding a new coloured element, map it to one of the above
//! rather than introducing a new hue.

pub mod demo;
pub mod event;
pub mod pacing;
pub mod showcase;
pub mod source;
pub mod state;

use crate::pacing::{PaceHandle, STEP_MS};
use crate::source::SourceMessage;
use crate::state::{ApplyOutcome, State};
use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{self as ct_event, Event as CtEvent, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Frame, Terminal};
use std::collections::VecDeque;
use std::io::{self, Stdout};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Scenario {
    /// Single-chunk teaching scenario (chunk 42, 2 collisions, 38 refresh rows).
    /// Finite — completes after ~65 events.
    #[value(name = "chunk42")]
    Chunk42,
    /// chunk42 plus a `stream.standby` + `stream.resumed` pair at the tail.
    #[value(name = "chunk42-standby")]
    Chunk42Standby,
    /// chunk42 with long fully-qualified table names and LONG composite
    /// primary keys — stress variant for table/pk column layout.
    #[value(name = "chunk42-long")]
    Chunk42Long,
    /// Rich procedural demo: scripted teaching intro (4 chunks, increasing contention,
    /// standby, final+completed request, error) followed by **infinite** continuous
    /// traffic (random chunks, CDC bursts, heartbeats, checkpoints, occasional
    /// standby/error). Runs until you quit.
    #[value(name = "showcase")]
    Showcase,
    /// `showcase` with fully-qualified production-style table names
    /// and composite LONG-typed primary keys. Same traffic pattern,
    /// same timing — just the worst-case identifier shape so the TUI
    /// columns get exercised under infinite traffic, not just the
    /// one-shot `chunk42-long` scenario.
    #[value(name = "showcase-long")]
    ShowcaseLong,
}

#[derive(Debug, Clone, Parser)]
#[command(version, about = "DBLog tap consumer TUI")]
pub struct Args {
    /// Tap endpoint URL. Ignored when `--demo` or `--scenario` is used.
    #[arg(long, default_value = "http://127.0.0.1:8085/api/v1/tap/stream")]
    pub url: String,

    /// Shorthand for `--scenario chunk42`. Kept for backwards compatibility;
    /// `--scenario` is the primary knob.
    #[arg(long)]
    pub demo: bool,

    /// Replay a built-in demo scenario instead of connecting to a live tap.
    /// Setting this automatically enables demo mode.
    #[arg(long, value_enum)]
    pub scenario: Option<Scenario>,

    /// Slow-down delay between reads, in milliseconds. Higher = slower
    /// (more delay); `0` = full speed. Also adjustable at runtime with
    /// `n` / `m` / `f`. Default: 100 ms for `showcase`, 350 ms for
    /// chunk42 scenarios, 0 ms for live.
    #[arg(long)]
    pub slowdown: Option<u64>,

    /// Start in step mode — one event per `space` keypress. Toggle at runtime
    /// with `s`. In step mode, `space` advances; out of step mode, `space`
    /// enters step mode.
    #[arg(long)]
    pub step: bool,
}

impl Args {
    /// Resolve which scenario to run (if any). `--scenario` wins; else `--demo`
    /// implies chunk42; else `None` means live mode.
    pub fn resolved_scenario(&self) -> Option<Scenario> {
        if let Some(s) = self.scenario {
            return Some(s);
        }
        if self.demo {
            return Some(Scenario::Chunk42);
        }
        None
    }

    /// Initial pacer delay, respecting `--slowdown` or falling back to
    /// the mode-appropriate default.
    pub fn initial_delay_ms(&self) -> u64 {
        if let Some(ms) = self.slowdown {
            return ms;
        }
        match self.resolved_scenario() {
            Some(Scenario::Showcase) | Some(Scenario::ShowcaseLong) => 100,
            Some(Scenario::Chunk42)
            | Some(Scenario::Chunk42Standby)
            | Some(Scenario::Chunk42Long) => 350,
            None => 0,
        }
    }
}

// ---------------------------------------------------------------------------
// App context — everything the render function needs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// Reader thread is starting up — haven't seen a Connected message yet.
    Pending,
    /// At least one successful connection; events may be flowing.
    Connected,
    /// Reader reported a disconnect; it will retry after a short backoff.
    Disconnected,
    /// The remote end closed the stream cleanly.
    Closed,
    /// Reader finished normally (demo exhausted or shutdown signalled).
    Finished,
}

pub struct AppContext {
    pub state: State,
    pub pace: PaceHandle,
    pub connection: ConnectionStatus,
    pub source_label: String,
    pub last_disconnect: Option<String>,
    pub parse_errors: VecDeque<(String, String)>,
    pub last_outcome: Option<ApplyOutcome>,
    pub started_at: Instant,
}

impl AppContext {
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

struct TermGuard;

impl TermGuard {
    fn enter() -> Result<()> {
        enable_raw_mode()?;
        execute!(io::stdout(), EnterAlternateScreen)?;
        Ok(())
    }
}

impl Drop for TermGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

/// Execute the shared event loop. Each bin supplies a `render` function that
/// draws a frame from the current [`AppContext`].
pub fn run_app<R>(args: Args, render: R) -> Result<()>
where
    R: Fn(&mut Frame, &AppContext),
{
    // Install a panic hook so a panic mid-render still restores the terminal.
    // (Drop of TermGuard fires on normal exit and propagated unwinds alike,
    // but printing the panic message goes through stderr — restore first so
    // the message lands on a sane terminal.)
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        default_hook(info);
    }));

    TermGuard::enter()?;
    let _guard = TermGuard; // Drop restores on any early return / panic.

    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    // Pacing + channel
    let (pace_handle, pacer) = pacing::channel(args.initial_delay_ms(), args.step);
    // Bounded channel so the reader can't pile an unbounded backlog ahead
    // of the UI. With a tiny capacity, `pacer.wait()` in the reader is
    // the rate gate the operator actually sees: slowdown / step mode
    // take effect on the next frame instead of after the backlog drains.
    const SOURCE_CHANNEL_CAP: usize = 16;
    let (tx, rx) = sync_channel::<SourceMessage>(SOURCE_CHANNEL_CAP);
    let shutdown = Arc::new(AtomicBool::new(false));

    let scenario = args.resolved_scenario();
    let source_label: String = match scenario {
        Some(Scenario::Chunk42) => "demo://chunk-42".into(),
        Some(Scenario::Chunk42Standby) => "demo://chunk-42-standby".into(),
        Some(Scenario::Chunk42Long) => "demo://chunk-42-long".into(),
        Some(Scenario::Showcase) => "demo://showcase".into(),
        Some(Scenario::ShowcaseLong) => "demo://showcase-long".into(),
        None => args.url.clone(),
    };

    // Reader thread
    let _reader = match scenario {
        Some(Scenario::Chunk42) => source::spawn_demo(
            source_label.clone(),
            demo::chunk42_scenario(),
            pacer,
            shutdown.clone(),
            tx.clone(),
        ),
        Some(Scenario::Chunk42Standby) => source::spawn_demo(
            source_label.clone(),
            demo::chunk42_with_standby(),
            pacer,
            shutdown.clone(),
            tx.clone(),
        ),
        Some(Scenario::Chunk42Long) => source::spawn_demo(
            source_label.clone(),
            demo::chunk42_long_scenario(),
            pacer,
            shutdown.clone(),
            tx.clone(),
        ),
        Some(Scenario::Showcase) => {
            source::spawn_showcase(pacer, shutdown.clone(), tx.clone())
        }
        Some(Scenario::ShowcaseLong) => {
            source::spawn_showcase_long(pacer, shutdown.clone(), tx.clone())
        }
        None => source::spawn_http(args.url.clone(), pacer, shutdown.clone(), tx.clone()),
    };
    drop(tx);

    let mut ctx = AppContext {
        state: State::new(),
        pace: pace_handle,
        connection: ConnectionStatus::Pending,
        source_label,
        last_disconnect: None,
        parse_errors: VecDeque::new(),
        last_outcome: None,
        started_at: Instant::now(),
    };

    let res = main_loop(&mut terminal, &mut ctx, &rx, &render);

    shutdown.store(true, Ordering::Release);
    // Wake a step-blocked reader so it can see `shutdown` and unwind.
    ctx.pace.advance();
    // Reader thread exits after seeing shutdown=true or when its tx fails;
    // don't block on join — the Drop of the JoinHandle detaches if necessary.

    res
}

fn main_loop<R>(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    ctx: &mut AppContext,
    rx: &Receiver<SourceMessage>,
    render: &R,
) -> Result<()>
where
    R: Fn(&mut Frame, &AppContext),
{
    const REDRAW_MIN: Duration = Duration::from_millis(50);
    // Force a redraw at this cadence even when no events or keys arrive,
    // so wall-clock-derived bits (heartbeat "stale Ns" marker, uptime,
    // error-banner TTL) advance visibly in step mode / idle streams.
    const TICK_INTERVAL: Duration = Duration::from_secs(1);
    let mut last_draw = Instant::now() - REDRAW_MIN;
    let mut dirty = true;

    loop {
        // Drain any pending source messages
        let mut drained = 0;
        while let Ok(msg) = rx.try_recv() {
            apply_message(ctx, msg);
            dirty = true;
            drained += 1;
            if drained >= 128 {
                break; // Don't starve the UI
            }
        }

        // Poll keyboard briefly (even in event-dense streams, we want snappy keys)
        if ct_event::poll(Duration::from_millis(20))? {
            match ct_event::read()? {
                CtEvent::Key(k) => {
                    if handle_key(ctx, k) {
                        return Ok(());
                    }
                    dirty = true;
                }
                CtEvent::Resize(_, _) => {
                    dirty = true;
                }
                _ => {}
            }
        }

        // Tick-driven repaint: if nothing touched `dirty` this cycle but
        // the tick interval has elapsed, mark dirty so the frame redraws
        // and time-sensitive widgets catch up.
        if !dirty && last_draw.elapsed() >= TICK_INTERVAL {
            dirty = true;
        }

        if dirty && last_draw.elapsed() >= REDRAW_MIN {
            terminal.draw(|f| render(f, ctx))?;
            last_draw = Instant::now();
            dirty = false;
        }
    }
}

fn apply_message(ctx: &mut AppContext, msg: SourceMessage) {
    match msg {
        SourceMessage::Connected { label } => {
            ctx.connection = ConnectionStatus::Connected;
            ctx.source_label = label;
            ctx.last_disconnect = None;
            ctx.parse_errors.clear();
        }
        SourceMessage::Disconnected { reason } => {
            ctx.connection = ConnectionStatus::Disconnected;
            ctx.last_disconnect = Some(reason);
        }
        SourceMessage::Closed { reason } => {
            ctx.connection = ConnectionStatus::Closed;
            ctx.last_disconnect = Some(reason);
        }
        SourceMessage::Finished => {
            ctx.connection = ConnectionStatus::Finished;
        }
        SourceMessage::Event(ev) => {
            let outcome = ctx.state.apply(ev);
            ctx.last_outcome = Some(outcome);
        }
        SourceMessage::ParseError {
            line_preview,
            error,
        } => {
            if ctx.parse_errors.len() >= 20 {
                ctx.parse_errors.pop_front();
            }
            ctx.parse_errors.push_back((line_preview, error));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::SourceMessage;
    use std::time::Instant;

    fn test_context() -> AppContext {
        let (pace, _pacer) = pacing::channel(0, false);
        AppContext {
            state: State::new(),
            pace,
            connection: ConnectionStatus::Pending,
            source_label: "demo://test".into(),
            last_disconnect: None,
            parse_errors: VecDeque::new(),
            last_outcome: None,
            started_at: Instant::now(),
        }
    }

    #[test]
    fn parse_errors_are_buffered_and_cleared_on_reconnect() {
        let mut ctx = test_context();

        apply_message(
            &mut ctx,
            SourceMessage::ParseError {
                line_preview: "{\"kind\":\"oops\"}".into(),
                error: "unknown variant".into(),
            },
        );

        assert_eq!(ctx.parse_errors.len(), 1);
        assert_eq!(ctx.parse_errors.back().unwrap().1, "unknown variant");

        apply_message(
            &mut ctx,
            SourceMessage::Connected {
                label: "http://127.0.0.1:8085/api/v1/tap/stream".into(),
            },
        );

        assert!(ctx.parse_errors.is_empty());
        assert_eq!(ctx.connection, ConnectionStatus::Connected);
        assert_eq!(
            ctx.source_label,
            "http://127.0.0.1:8085/api/v1/tap/stream"
        );
    }

    #[test]
    fn clean_remote_close_is_not_treated_as_disconnect() {
        let mut ctx = test_context();

        apply_message(
            &mut ctx,
            SourceMessage::Closed {
                reason: "server closed connection".into(),
            },
        );

        assert_eq!(ctx.connection, ConnectionStatus::Closed);
        assert_eq!(
            ctx.last_disconnect.as_deref(),
            Some("server closed connection")
        );
    }
}

/// Returns `true` if the app should exit.
fn handle_key(ctx: &mut AppContext, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => return true,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return true,
        KeyCode::Char(' ') => {
            if ctx.pace.step_mode() {
                ctx.pace.advance();
            } else {
                ctx.pace.toggle_step();
            }
        }
        KeyCode::Char('s') => ctx.pace.toggle_step(),
        // Pace keys: `n` slows (longer delay), `m` speeds up — they sit
        // side-by-side on the home row with n left of m, mirroring a
        // left=slower / right=faster mental model.
        KeyCode::Char('n') => ctx.pace.bump_delay(STEP_MS as i64),
        KeyCode::Char('m') => ctx.pace.bump_delay(-(STEP_MS as i64)),
        KeyCode::Char('f') => ctx.pace.set_delay_ms(0),
        _ => {}
    }
    false
}

// ---------------------------------------------------------------------------
// Render helpers — shared between the two bins
// ---------------------------------------------------------------------------

pub mod render_util {
    use super::*;
    use chrono::{DateTime, Utc};

    /// "00:07:24.512"-style display of a `Duration`. Used for uptime.
    pub fn format_uptime(d: Duration) -> String {
        let total_ms = d.as_millis();
        let h = total_ms / 3_600_000;
        let m = (total_ms / 60_000) % 60;
        let s = (total_ms / 1_000) % 60;
        let ms = total_ms % 1_000;
        format!("{:02}:{:02}:{:02}.{:03}", h, m, s, ms)
    }

    /// "t+1.234" relative to `anchor`. Negative values render with a leading `-`.
    pub fn format_rel(ts: DateTime<Utc>, anchor: DateTime<Utc>) -> String {
        let delta = ts.signed_duration_since(anchor).num_milliseconds();
        let sign = if delta < 0 { "-" } else { "" };
        let total = delta.unsigned_abs();
        let s = total / 1_000;
        let ms = total % 1_000;
        format!("{}t+{}.{:03}", sign, s, ms)
    }

    /// Render a `run_id` in a space that fits on one title-bar line.
    ///
    /// A naive prefix truncation works for UUIDs (block 1 is already a
    /// clean 8-hex segment) but truncates hostname-style ids mid-segment
    /// — two unrelated runs can look identical. This form keeps the
    /// first 8 and last 8 characters and joins them with an ellipsis,
    /// so the run is locally identifiable even with long ids.
    pub fn short_run_id(run_id: &str) -> String {
        const HEAD: usize = 8;
        const TAIL: usize = 8;
        let total: usize = run_id.chars().count();
        if total <= HEAD + TAIL + 1 {
            return run_id.to_string();
        }
        let head: String = run_id.chars().take(HEAD).collect();
        let tail: String = run_id.chars().skip(total - TAIL).collect();
        format!("{head}…{tail}")
    }

    /// Display LSN or "—" if empty.
    pub fn lsn_display(lsn: &str) -> &str {
        if lsn.is_empty() {
            "—"
        } else {
            lsn
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn short_run_id_preserves_short_ids_untouched() {
            assert_eq!(short_run_id(""), "");
            assert_eq!(short_run_id("run-1"), "run-1");
            assert_eq!(short_run_id("0123456789abcdef"), "0123456789abcdef"); // 16 chars
        }

        #[test]
        fn short_run_id_joins_head_and_tail_with_ellipsis() {
            // UUID (36 chars) — keep the leading block and the trailing 8
            // hex digits so two unrelated UUIDs never collide to the same
            // display form.
            assert_eq!(
                short_run_id("a1b2c3d4-e5f6-4890-abcd-ef1234567890"),
                "a1b2c3d4…34567890"
            );
            // Hostname-style id where naive prefix truncation would lose
            // the trailing disambiguator (`-001`).
            assert_eq!(
                short_run_id("my-host-prod-dblog-mysql-001"),
                "my-host-…ysql-001"
            );
        }
    }
}
