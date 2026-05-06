//! Hydroscope — live pipeline view.
//!
//! Main area is split 72/28 horizontally. The left column stacks
//! `SOURCE LOG` (tap stream) on top of `SINK STREAM` (post-reconciliation
//! output) 50/50. The right column is the `RECONCILER` — a vertical
//! flow of the active chunk's state in algorithm time-order:
//!
//!   chunk identity → LW → in-window CDC → chunk buffer → HW → emit
//!
//! The reconciler is a fixed sidebar rather than an inline lane, so the
//! source/sink panels don't resize as the chunk lifecycle advances.
//!
//! Rendering reads from the shared `AppContext`; the runner owns state
//! updates, pacing, keyboard handling, and the event loop.

use clap::Parser;
use hydroscope::event::{Origin, SinkEventBody, WatermarkLevel};
use hydroscope::render_util::{lsn_display, short_run_id};
use hydroscope::state::{ActiveChunk, SourceEntry, State};
use hydroscope::{AppContext, Args, ConnectionStatus};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Cell, Padding, Paragraph, Row, Table},
    Frame,
};

// ---- Palette (Steel Navy) --------------------------------------------------

const BG:    Color = Color::Rgb(6, 13, 28);
const BLUE:  Color = Color::Rgb(106, 177, 255);
const BDIM:  Color = Color::Rgb(58, 119, 184);
const BXDIM: Color = Color::Rgb(20, 48, 84);
const WHITE: Color = Color::Rgb(228, 237, 250);
const GRAY:  Color = Color::Rgb(111, 126, 149);
const YEL:   Color = Color::Rgb(234, 185, 92);
const CYAN:  Color = Color::Rgb(102, 211, 192);
const RED:   Color = Color::Rgb(240, 132, 112);
const ORANGE: Color = Color::Rgb(240, 168, 80);

fn body()  -> Style { Style::default().bg(BG).fg(WHITE) }
fn dim_s() -> Style { body().fg(BDIM) }
fn gray_s()-> Style { body().fg(GRAY) }
fn head()  -> Style { body().fg(BLUE).add_modifier(Modifier::BOLD) }

fn lane(title: String, subtitle: String, badge: Option<(String, Color)>, area_width: u16) -> Block<'static> {
    // Reserve 6 cells for the title rail's chrome: 2 corners (`╭`/`╮`),
    // 1 leading space, 1 trailing space, and ~2 cells of `─` fill so
    // the corner doesn't sit flush against the title. What's left is
    // available for the title text + the `  ·  subtitle` extension.
    let room = (area_width as usize).saturating_sub(6);
    // Title gets first dibs — clip with ellipsis if it alone exceeds
    // room. In practice all current titles are ≤ 13 chars (short enough
    // to fit any sensible terminal), but a wider future title would
    // otherwise be silently clipped by ratatui without an indicator.
    let title = if title.chars().count() > room && room >= 4 {
        clip_right(&title, room)
    } else {
        title
    };
    let title_len = title.chars().count();
    let separator_len = 5; // "  ·  "
    let subtitle_room = room.saturating_sub(title_len + separator_len);
    let subtitle_spans = if subtitle.is_empty() {
        Vec::new()
    } else if subtitle.chars().count() <= subtitle_room {
        vec![Span::styled(subtitle, body().fg(BDIM))]
    } else if subtitle_room >= 8 {
        // Tight but still useful — clip with ellipsis.
        vec![Span::styled(clip_right(&subtitle, subtitle_room), body().fg(BDIM))]
    } else {
        // No usable room — drop the subtitle entirely so the title
        // bar isn't a hard-cut fragment of an English sentence.
        Vec::new()
    };
    lane_spans(title, subtitle_spans, badge)
}

/// Variant of `lane` that lets the caller colour individual subtitle
/// segments. An empty subtitle collapses the separator so the title
/// sits alone (used by the reconciler).
fn lane_spans(title: String, subtitle: Vec<Span<'static>>, badge: Option<(String, Color)>) -> Block<'static> {
    let mut spans = vec![
        Span::styled(" ", body()),
        Span::styled(title, head()),
    ];
    if !subtitle.is_empty() {
        spans.push(Span::styled("  ·  ", body().fg(BXDIM)));
        spans.extend(subtitle);
    }
    spans.push(Span::styled(" ", body()));
    if let Some((b, c)) = badge {
        spans.push(Span::styled(format!("┤ {} ├", b), body().fg(c).add_modifier(Modifier::BOLD)));
    }
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(BLUE).bg(BG))
        .title(Line::from(spans))
        .style(body())
        .padding(Padding::new(1, 1, 0, 0))
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    hydroscope::run_app(args, render)
}

fn render(f: &mut Frame, ctx: &AppContext) {
    f.render_widget(Block::default().style(body()), f.area());

    // Layout: title + banner on top, footer on bottom, main area split
    // horizontally 65/35. Left column stacks SOURCE LOG (top) and SINK
    // STREAM (bottom) 50/50; right column is the RECONCILER full height.
    // Putting the reconciler on a stable sidebar (rather than between
    // lane 1 and lane 3) keeps the source/sink panels from resizing when
    // the reconciler collapses to "no active chunk".
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // title bar
            Constraint::Length(1), // connection / standby banner
            Constraint::Min(0),    // main area
            Constraint::Length(1), // footer
        ])
        .split(f.area());

    let main_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(72), Constraint::Percentage(28)])
        .split(rows[2]);

    let left_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(main_cols[0]);

    title_bar(f, rows[0], ctx);
    banner(f, rows[1], ctx);
    source_lane(f, left_rows[0], ctx);
    sink_lane(f, left_rows[1], ctx);
    reconciler_lane(f, main_cols[1], ctx);
    footer(f, rows[3], ctx);
}

fn title_bar(f: &mut Frame, area: Rect, ctx: &AppContext) {
    let run_display = ctx
        .state
        .run_id
        .as_deref()
        .map(short_run_id)
        .unwrap_or_else(|| "«waiting»".into());
    let source_id = ctx.state.source_id.as_deref().unwrap_or("—");
    // Window state (OPEN / CLOSED / passthrough) used to live here but it
    // strobed the title bar red on every chunk transition. The reconciler
    // sidebar already shows the same information via the LW/HW triangles
    // (`▽`/`▼` and `△`/`▲`), so the title bar no longer carries it.
    let l = Line::from(vec![
        Span::styled(" ", body()),
        Span::styled("DBLOG · HYDROSCOPE", head()),
        Span::styled("   · ", body().fg(BDIM)),
        Span::styled("run ", gray_s()), Span::styled(run_display, body().fg(WHITE)),
        Span::styled(" · source ", gray_s()), Span::styled(source_id, body().fg(WHITE)),
    ]);
    f.render_widget(Paragraph::new(l).style(body()), area);
}

fn banner(f: &mut Frame, area: Rect, ctx: &AppContext) {
    // Priority: standby > error > parse error > disconnect > connection status
    if let Some(s) = &ctx.state.standby {
        // queue_full_ms is a snapshot from the moment the tap emitted
        // the stream.standby event — not a live counter. Label it as
        // such so the value doesn't read like a ticking stopwatch.
        let l = Line::from(vec![
            Span::styled(" ⚠ STANDBY ", body().fg(Color::Black).bg(YEL).add_modifier(Modifier::BOLD)),
            Span::styled(" DBLog pump blocked on full queue ", body().fg(YEL).add_modifier(Modifier::BOLD)),
            Span::styled(format!("· queue_full_ms@onset {} · capacity {} ", s.queue_full_ms, s.queue_capacity), body().fg(YEL)),
            Span::styled(format!("· {}", s.message), dim_s()),
        ]);
        f.render_widget(Paragraph::new(l).style(body()), area);
        return;
    }
    if let Some(err) = &ctx.state.latest_error {
        // Keep the red ERROR banner on-screen for ~30s of stream time after
        // the error arrived; after that, fall back to the regular connection
        // banner so a single transient fault doesn't pin the top line forever.
        // The error stays in state — only its banner takeover is time-boxed.
        let fresh = match ctx.state.last_ts {
            Some(last_ts) => (last_ts - err.ts).num_seconds() < 30,
            None => true,
        };
        if fresh {
            let l = Line::from(vec![
                Span::styled(" ✗ ERROR ", body().fg(WHITE).bg(RED).add_modifier(Modifier::BOLD)),
                Span::styled(format!(" {} ", err.class), body().fg(RED).add_modifier(Modifier::BOLD)),
                Span::styled(err.message.chars().take(120).collect::<String>(), body().fg(WHITE)),
            ]);
            f.render_widget(Paragraph::new(l).style(body()), area);
            return;
        }
    }
    if let Some((line_preview, error)) = ctx.parse_errors.back() {
        let l = Line::from(vec![
            Span::styled(
                " ⚠ PARSE ERROR ",
                body().fg(Color::Black).bg(YEL).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!(" {} ", error.chars().take(72).collect::<String>()),
                body().fg(YEL).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                line_preview.chars().take(96).collect::<String>(),
                body().fg(WHITE),
            ),
        ]);
        f.render_widget(Paragraph::new(l).style(body()), area);
        return;
    }
    match ctx.connection {
        ConnectionStatus::Pending => {
            let l = Line::from(vec![
                Span::styled(" ◌ connecting  ", body().fg(BDIM)),
                Span::styled(&ctx.source_label, body().fg(WHITE)),
            ]);
            f.render_widget(Paragraph::new(l).style(body()), area);
        }
        ConnectionStatus::Disconnected => {
            let l = Line::from(vec![
                Span::styled(" ⚠ DISCONNECTED ", body().fg(Color::Black).bg(RED).add_modifier(Modifier::BOLD)),
                Span::styled(
                    format!(" retrying · {} ", ctx.last_disconnect.as_deref().unwrap_or("")),
                    body().fg(RED),
                ),
            ]);
            f.render_widget(Paragraph::new(l).style(body()), area);
        }
        ConnectionStatus::Closed => {
            // Clean server close: the TUI does NOT auto-reconnect here (that
            // path only fires on transient transport errors). Tell the
            // operator to restart hydroscope rather than suggesting the
            // binary will pick the stream back up on its own.
            let l = Line::from(vec![
                Span::styled(" ◌ STREAM CLOSED ", body().fg(BDIM).add_modifier(Modifier::BOLD)),
                Span::styled(
                    format!(" {} ", ctx.last_disconnect.as_deref().unwrap_or("remote closed stream")),
                    body().fg(BDIM),
                ),
                Span::styled("   press q to exit; re-run hydroscope to attach again", dim_s()),
            ]);
            f.render_widget(Paragraph::new(l).style(body()), area);
        }
        ConnectionStatus::Connected => {
            let (pace_label, pace_color) = pace_label(ctx);
            let req = request_banner_label(ctx);
            // Reserve the connection dot and `   pace XXX` suffix; clip
            // the middle (`connected · {source}` + request label) to fit.
            // Source label is shown verbatim in the title bar already,
            // so [`clip_two_part`] yields its budget to the request
            // label first.
            let dot = " ● ";
            let pace_prefix = "   pace ";
            let reserved = dot.chars().count()
                + pace_prefix.chars().count()
                + pace_label.chars().count();
            let middle_budget = (area.width as usize).saturating_sub(reserved);
            let source_full = format!("connected · {}   ", &ctx.source_label);
            let (source_part, req_part) = clip_two_part(&source_full, &req, middle_budget);
            let l = Line::from(vec![
                Span::styled(dot.to_string(), body().fg(BLUE).add_modifier(Modifier::BOLD)),
                Span::styled(source_part, body().fg(BDIM)),
                Span::styled(req_part, body().fg(CYAN)),
                Span::styled(pace_prefix.to_string(), gray_s()),
                Span::styled(pace_label, body().fg(pace_color).add_modifier(Modifier::BOLD)),
            ]);
            f.render_widget(Paragraph::new(l).style(body()), area);
        }
        ConnectionStatus::Finished => {
            let l = Line::from(vec![
                Span::styled(" ■ finished ", body().fg(BDIM)),
                Span::styled(&ctx.source_label, body().fg(WHITE)),
                Span::styled("   press q to exit", dim_s()),
            ]);
            f.render_widget(Paragraph::new(l).style(body()), area);
        }
    }
}

fn request_banner_label(ctx: &AppContext) -> String {
    let active_request_id = ctx
        .state
        .active_chunk
        .as_ref()
        .and_then(|chunk| chunk.request_id.as_deref());
    match (active_request_id, ctx.state.latest_request.as_ref()) {
        (Some(active), Some(latest)) if latest.request_id == active => {
            format!("request {} · {} · {}", latest.request_id, scope_label(latest), state_str(latest.state))
        }
        (Some(active), _) => format!("request {} · active chunk", active),
        (None, Some(latest)) => {
            format!("request {} · {} · {}", latest.request_id, scope_label(latest), state_str(latest.state))
        }
        (None, None) => "request —".into(),
    }
}

/// Scope label for the request banner. For scopes that target a specific
/// table (`TABLE`, `PRIMARY_KEYS`), append the table name so the banner
/// shows *which* table is being dumped without forcing the operator into
/// the reconciler sidebar. `ALL_TABLES` has no target table and uses the
/// bare scope string. Table names are short-formatted (stripping the
/// `app.` default schema prefix) for consistency with the source /
/// sink / reconciler lanes.
fn scope_label(t: &hydroscope::event::RequestTransitionBody) -> String {
    let scope = scope_str(t.scope);
    match t.table.as_deref() {
        Some(table) if !table.is_empty() => format!("{} {}", scope, short_table(table)),
        _ => scope.to_string(),
    }
}

/// Wire-form display for a `RequestScope`. Matches the enum values emitted
/// on the tap (`TABLE` / `PRIMARY_KEYS` / `ALL_TABLES`) so operators see
/// the same spelling in the banner as in the NDJSON.
fn scope_str(scope: hydroscope::event::RequestScope) -> &'static str {
    use hydroscope::event::RequestScope as S;
    match scope {
        S::Table => "TABLE",
        S::PrimaryKeys => "PRIMARY_KEYS",
        S::AllTables => "ALL_TABLES",
    }
}

/// Wire-form display for a `RequestState` — same rationale as `scope_str`.
fn state_str(state: hydroscope::event::RequestState) -> &'static str {
    use hydroscope::event::RequestState as S;
    match state {
        S::Active => "ACTIVE",
        S::Completed => "COMPLETED",
        S::Failed => "FAILED",
    }
}

fn pace_label(ctx: &AppContext) -> (String, Color) {
    if ctx.pace.step_mode() {
        ("STEP (space=next)".into(), YEL)
    } else {
        let ms = ctx.pace.delay_ms();
        if ms == 0 {
            ("full".into(), BLUE)
        } else {
            (format!("{} ms/event", ms), BDIM)
        }
    }
}

// ---------------------------------------------------------------------------
// Lane 1 — SOURCE LOG
// ---------------------------------------------------------------------------

/// Content-carrying per-row record. Extracted from `SourceEntry`
/// without any width-dependent clipping so the lane can measure the
/// natural widths of the `table / pk` and `note` cells before
/// deciding how much room to give each column.
struct SourceRowParts {
    seq: String,
    lsn: String,
    op: String,
    op_color: Color,
    table: String,
    pk: String,
    table_color: Color,
    kind: String,
    kind_color: Color,
    note: String,
}

fn source_lane(f: &mut Frame, area: Rect, ctx: &AppContext) {
    let state = &ctx.state;
    let block = lane(
        "SOURCE EVENTS".to_string(),
        "tap stream, seq-ordered — cdc + watermark + chunk".to_string(),
        None,
        area.width,
    );
    let inner = block.inner(area);
    f.render_widget(block, area);

    // `table / pk` is combined into a single column — the slash
    // visually groups the two parts of the row's subject ("orders / 1002")
    // and avoids a narrow, often-empty pk column sitting next to
    // the table column. The relative-time `t` column was dropped —
    // the LSN already orders events monotonically and `seq` already
    // gives a per-stream ordinal; the duplicate wall-clock offset
    // was noise.
    let header = Row::new(vec![
        Cell::from(Span::styled(" seq",        dim_s())),
        Cell::from(Span::styled("LSN",         dim_s())),
        Cell::from(Span::styled("op",          dim_s())),
        Cell::from(Span::styled("table / pk",  dim_s())),
        Cell::from(Span::styled("kind",        dim_s())),
        Cell::from(Span::styled("note",        dim_s())),
    ]).height(1);

    // Show the newest entries that fit the inner area (minus header row).
    // `chunk.completed` is filtered out: the ChunkSummary already lands on
    // `chunks_history`, the reconciler sidebar's closure animates the same
    // transition, and a dedicated source row added noise without telling
    // the reader anything new.
    let rows_capacity = inner.height.saturating_sub(1) as usize;
    let take_n = rows_capacity.max(1);
    let iter = state
        .source_log
        .iter()
        .filter(|e| !matches!(
            e,
            SourceEntry::ChunkCompleted(_) | SourceEntry::ChunkCollision(_)
        ))
        .rev()
        .take(take_n);
    let entries: Vec<&SourceEntry> = iter.collect::<Vec<_>>().into_iter().rev().collect();

    // Pre-compute per-row content so we can measure natural cell widths
    // before picking column constraints. This is the ratatui-idiomatic
    // path for variable-width content (cf. `helix`, `zoxide`): measure,
    // then `Length(computed)` — the layout solver then won't distribute
    // phantom slack into cells that have nothing to show.
    let parts: Vec<SourceRowParts> = entries
        .iter()
        .map(|e| source_row_parts(e, state))
        .collect();

    // LSN column is content-aware (hugs the widest LSN in view) and
    // budget-aware (yields cells back when both data columns need their
    // floors). Fixed-overhead minus LSN: seq(5) + op(4) + kind(12) + 5
    // gaps = 26.
    let max_lsn = parts.iter().map(|p| p.lsn.chars().count()).max().unwrap_or(0);
    let lsn_w = compute_lsn_w(max_lsn, inner.width, 5 + 4 + 12 + 5);

    // Fixed columns: seq(5) + LSN(lsn_w) + op(4) + kind(12);
    // plus `column_spacing(1)` * 5 gaps = 5.
    let fixed_cols_width: usize = 5 + lsn_w + 4 + 12 + 5;
    let available = (inner.width as usize).saturating_sub(fixed_cols_width);
    let (tpk_w, note_w) = compute_lane_widths(
        &parts,
        |p| table_pk_full_len(&p.table, &p.pk),
        |p| p.note.chars().count(),
        available,
    );

    let body_rows: Vec<Row> = parts
        .into_iter()
        .map(|p| source_row_render(p, lsn_w, tpk_w, note_w))
        .collect();

    let widths = [
        Constraint::Length(5),              // seq
        Constraint::Length(lsn_w as u16),   // LSN — content-aware
        Constraint::Length(4),              // op
        Constraint::Length(tpk_w as u16),   // table / pk — content-aware
        Constraint::Length(12),             // kind
        Constraint::Length(note_w as u16),  // note — content-aware
    ];
    let table = Table::new(body_rows, widths).header(header).style(body()).column_spacing(1);
    f.render_widget(table, inner);
}

fn source_row_parts(
    entry: &SourceEntry,
    state: &State,
) -> SourceRowParts {
    let seq = entry.seq().map(|s| format!(" {}", s)).unwrap_or_else(|| "  —".into());

    // `table_color` lets the watermark rows tint their table/pk cells
    // in GRAY (they live on the private `watermarks` meta table, not on
    // business data) while data-table rows keep WHITE.
    let (kind, kind_color, op, table, pk, table_color, note): (
        String,
        Color,
        String,
        String,
        String,
        Color,
        String,
    ) = match entry {
        SourceEntry::Cdc(b) => {
            // CDC rows whose LSN appears as a `chunk.collision.cause_lsn`
            // in any trackable window tint YELLOW and carry a "collision
            // in chunk N" note; the paired `chunk.collision` row is
            // filtered out. Non-collision CDCs stay BLUE with no note.
            let (color, note) = match state.collision_chunk_for_lsn(&b.lsn) {
                Some(chunk_id) => (YEL, format!("collision in chunk {}", chunk_id)),
                None => (BLUE, String::new()),
            };
            (
                "cdc".into(), color,
                b.op.short().into(),
                short_table(&b.table),
                b.pk.clone(),
                WHITE,
                note,
            )
        }
        SourceEntry::WatermarkWritten(b) => (
            "wm-written".into(), RED,
            match b.level { WatermarkLevel::Low => "LW ".into(), WatermarkLevel::High => "HW ".into() },
            // Watermarks live on a meta table, not on business data;
            // leave the table / pk columns blank so the eye associates
            // them with the watermark row itself rather than a data table.
            String::new(),
            String::new(),
            GRAY,
            format!("chunk {}", b.chunk_id),
        ),
        SourceEntry::WatermarkReceived(b) => (
            "wm-received".into(), RED,
            match b.level { WatermarkLevel::Low => "LW ".into(), WatermarkLevel::High => "HW ".into() },
            String::new(),
            String::new(),
            GRAY,
            format!("chunk {}", b.chunk_id),
        ),
        SourceEntry::ChunkSelected(b) => {
            // PK range moves into the note column so the main `pk`
            // column can stay tight (short numeric ids). Row count
            // dropped — the reconciler and chunk-completed row both
            // surface it already.
            let note = match (&b.pk_min, &b.pk_max) {
                (Some(lo), Some(hi)) => {
                    format!("chunk {} · pk [{} … {}]", b.chunk_id, lo, hi)
                }
                _ => format!("chunk {}", b.chunk_id),
            };
            (
                "chunk-sel".into(), CYAN,
                "SEL".into(),
                short_table(&b.table),
                String::new(),
                WHITE,
                note,
            )
        }
        SourceEntry::ChunkCollision(b) => (
            "chunk-drop".into(), YEL,
            "DROP".into(),
            String::new(),
            b.excluded_pk.clone().unwrap_or_else(|| "—".into()),
            WHITE,
            format!("chunk {} · {}", b.chunk_id, b.cause_op.short()),
        ),
        SourceEntry::ChunkCompleted(b) => (
            "chunk-done".into(), CYAN,
            "DONE".into(),
            short_table(&b.table),
            String::new(),
            WHITE,
            format!("chunk {} · emitted {} · excluded {}", b.chunk_id, b.emitted, b.excluded),
        ),
    };

    let lsn = match entry {
        SourceEntry::Cdc(b) => b.lsn.clone(),
        SourceEntry::WatermarkReceived(b) => b.lsn.clone(),
        SourceEntry::ChunkCollision(b) => b.cause_lsn.clone(),
        SourceEntry::ChunkCompleted(b) => b.lsn.clone(),
        SourceEntry::WatermarkWritten(_) | SourceEntry::ChunkSelected(_) => String::new(),
    };

    SourceRowParts {
        seq,
        lsn: lsn_display(&lsn).to_string(),
        op,
        op_color: kind_color,
        table,
        pk,
        table_color,
        kind,
        kind_color,
        note,
    }
}

fn source_row_render(p: SourceRowParts, lsn_w: usize, tpk_w: usize, note_w: usize) -> Row<'static> {
    Row::new(vec![
        Cell::from(Span::styled(p.seq, body().fg(BDIM))),
        Cell::from(Span::styled(clip_right(&p.lsn, lsn_w), body().fg(BDIM))),
        Cell::from(Span::styled(p.op, body().fg(p.op_color))),
        Cell::from(Span::styled(
            join_table_pk(&p.table, &p.pk, tpk_w),
            body().fg(p.table_color),
        )),
        Cell::from(Span::styled(
            p.kind,
            body().fg(p.kind_color).add_modifier(Modifier::BOLD),
        )),
        Cell::from(Span::styled(
            clip_right(&p.note, note_w),
            body().fg(p.kind_color),
        )),
    ])
}

// ---------------------------------------------------------------------------
// Lane 2 — RECONCILER
// ---------------------------------------------------------------------------

fn reconciler_lane(f: &mut Frame, area: Rect, ctx: &AppContext) {
    let state = &ctx.state;
    let Some(chunk) = state.active_chunk.as_ref() else {
        let block = lane(
            "RECONCILER".into(),
            "live chunk state".into(),
            None,
            area.width,
        );
        let inner = block.inner(area);
        f.render_widget(block, area);
        let empty = vec![
            Line::from(""),
            Line::from(Span::styled(" no chunk active", body().fg(BDIM))),
            Line::from(""),
            Line::from(Span::styled(" CDC events flow", dim_s())),
            Line::from(Span::styled(" directly to the sink", dim_s())),
        ];
        f.render_widget(Paragraph::new(empty).style(body()), inner);
        return;
    };

    let pk_min = chunk.pk_min.as_deref().unwrap_or("—");
    let pk_max = chunk.pk_max.as_deref().unwrap_or("—");
    // Header keeps a single-line explainer so an operator landing on
    // the sidebar knows what it's showing. Chunk id + table live in
    // the body so they can change per chunk without rebuilding the
    // title string.
    let block = lane(
        "RECONCILER".into(),
        "live chunk state".into(),
        None,
        area.width,
    );
    let inner = block.inner(area);
    f.render_widget(block, area);

    // Single-column vertical flow — content stacked in time-order of
    // the algorithm:
    //
    //   chunk identity → LW → chunk buffer → in-window CDC → HW →
    //   emit queue.
    //
    // Longer lines (tokens, LSNs) are broken onto their own indented
    // follow-up line so the narrow column doesn't truncate the label.
    // Body leads with chunk / table / pk range so the operator sees
    // what chunk this is on, what table it's against, and what PK span
    // it covers — in that reading order. The RED on `chunk {N}` ties
    // it to the LW/HW markers below.
    // Keep the full `schema.table` identity in the sidebar — this is
    // the one place an operator checks to confirm which object a
    // chunk is against — but wrap it onto a continuation line when
    // the name exceeds the sidebar inner width. The conventional
    // `app.` prefix is stripped (as in the lanes); other schemas are
    // preserved verbatim so the operator can still disambiguate.
    let raw_table = chunk.table.as_deref().unwrap_or("—");
    let table_display = raw_table.strip_prefix("app.").unwrap_or(raw_table);
    let sidebar_width = inner.width as usize;
    let mut lines: Vec<Line<'static>> = vec![
        Line::from(vec![
            Span::styled(" chunk ", body().fg(RED)),
            Span::styled(
                format!("{}", chunk.chunk_id),
                body().fg(RED).add_modifier(Modifier::BOLD),
            ),
        ]),
    ];
    lines.extend(render_table_line(table_display, sidebar_width));
    lines.extend(render_pk_range(pk_min, pk_max, sidebar_width));
    lines.push(Line::from(""));
    // LW section — label only; the UUID token is wire bookkeeping
    // and adds no narrative value to the reconciler column. Glyph
    // is an empty triangle (`▽`) while we're still awaiting the
    // LW echo on the CDC log, and fills to `▼` once it lands —
    // same for HW below.
    lines.push(Line::from(vec![
        Span::styled(
            if chunk.lw_received_lsn.is_some() { " ▼ Low Watermark" } else { " ▽ Low Watermark" },
            body().fg(RED).add_modifier(Modifier::BOLD),
        ),
    ]));
    match (chunk.lw_received_lsn.as_deref(), chunk.lw_latency_ms) {
        (Some(lsn), Some(lat)) => {
            lines.push(Line::from(vec![
                Span::styled("    lsn ", gray_s()),
                Span::styled(lsn.to_string(), body().fg(WHITE)),
            ]));
            lines.push(Line::from(vec![
                Span::styled("    latency ", gray_s()),
                Span::styled(format!("{} ms", lat), body().fg(WHITE)),
            ]));
        }
        (Some(lsn), None) => {
            lines.push(Line::from(vec![
                Span::styled("    lsn ", gray_s()),
                Span::styled(lsn.to_string(), body().fg(WHITE)),
            ]));
        }
        _ => {
            lines.push(Line::from(Span::styled(
                "    « awaiting CDC event »",
                body().fg(BDIM),
            )));
        }
    }
    lines.push(Line::from(""));

    // Chunk buffer first — it's the snapshot the SELECT produced; the
    // in-window CDC summary below describes what has happened to that
    // buffer since LW. Visually it also reads top-down as: "here's what
    // came in, here's what changed it while the window was open."
    lines.push(Line::from(vec![
        Span::styled(" chunk buffer ", gray_s()),
        Span::styled(format!("({} rows)", chunk.row_count.unwrap_or(0)), dim_s()),
    ]));
    lines.extend(render_buffer_lines(chunk, inner.width as usize));
    lines.push(Line::from(""));

    // In-window CDC activity — collisions listed, passthroughs summarised.
    // "In-window" means ts is inside [lw_received_ts, hw_received_ts_or_now].
    // Gating by ts keeps previous-chunk CDCs and pre-window heartbeat traffic
    // out of the passthrough count. If LW hasn't been received yet, there is
    // no window to report — skip the CDC summary entirely.
    lines.push(Line::from(Span::styled(" in-window cdc", gray_s())));
    let mut collision_listed = 0usize;
    let mut passthrough = 0usize;
    const COLLISION_ROWS: usize = 3;
    if let Some(lw_rx) = chunk.lw_received_ts {
        for entry in &state.source_log {
            if let SourceEntry::Cdc(b) = entry {
                if b.ts < lw_rx {
                    continue;
                }
                if let Some(hw_rx) = chunk.hw_received_ts {
                    if b.ts > hw_rx {
                        continue;
                    }
                }
                let is_hit = chunk.cause_lsns.contains(&b.lsn);
                if is_hit {
                    collision_listed += 1;
                    if collision_listed <= COLLISION_ROWS {
                        lines.push(Line::from(vec![
                            Span::styled("  ✗ ", body().fg(YEL).add_modifier(Modifier::BOLD)),
                            Span::styled(
                                clip_sidebar_pk(b.op.short(), &b.pk, sidebar_width),
                                body().fg(YEL),
                            ),
                        ]));
                    }
                } else {
                    passthrough += 1;
                }
            }
        }
    }
    if collision_listed > COLLISION_ROWS {
        lines.push(Line::from(vec![
            Span::styled("  ✗ ", body().fg(YEL).add_modifier(Modifier::BOLD)),
            Span::styled(format!("+{} more", collision_listed - COLLISION_ROWS), body().fg(YEL)),
        ]));
    }
    if collision_listed == 0 && passthrough == 0 {
        lines.push(Line::from(Span::styled("  —", dim_s())));
    }
    if passthrough > 0 {
        lines.push(Line::from(vec![
            Span::styled("  · ", body().fg(BLUE)),
            Span::styled(
                format!("{} passthrough", passthrough),
                dim_s(),
            ),
        ]));
    }
    lines.push(Line::from(""));

    // HW section — label only, UUID suppressed (see LW above).
    lines.push(Line::from(vec![
        Span::styled(
            if chunk.hw_received_lsn.is_some() { " ▲ High Watermark" } else { " △ High Watermark" },
            body().fg(RED).add_modifier(Modifier::BOLD),
        ),
    ]));
    match (chunk.hw_received_lsn.as_deref(), chunk.hw_latency_ms) {
        (Some(lsn), Some(lat)) => {
            lines.push(Line::from(vec![
                Span::styled("    lsn ", gray_s()),
                Span::styled(lsn.to_string(), body().fg(WHITE)),
            ]));
            lines.push(Line::from(vec![
                Span::styled("    latency ", gray_s()),
                Span::styled(format!("{} ms", lat), body().fg(WHITE)),
            ]));
        }
        (Some(lsn), None) => {
            lines.push(Line::from(vec![
                Span::styled("    lsn ", gray_s()),
                Span::styled(lsn.to_string(), body().fg(WHITE)),
            ]));
        }
        _ => {
            lines.push(Line::from(Span::styled(
                "    « awaiting CDC event »",
                body().fg(BDIM),
            )));
        }
    }
    lines.push(Line::from(""));

    // Emit queue — replaced-by-cdc first so the eye reads "here's what
    // lost" before "here's what survives". Same gray label style as
    // emit-ready; the count itself uses ORANGE to flag it as the "loss"
    // side of the ledger against emit-ready's CYAN.
    lines.push(Line::from(vec![
        Span::styled(" replaced-by-cdc ", gray_s()),
        Span::styled(
            format!("{} rows", chunk.excluded),
            body().fg(ORANGE).add_modifier(Modifier::BOLD),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled(" emit-ready ", gray_s()),
        Span::styled(
            format!("{} rows", chunk.emit_queue()),
            body().fg(CYAN).add_modifier(Modifier::BOLD),
        ),
    ]));

    // Paragraph has no scroll; ratatui will silently clip overflow. Detect
    // that here and swap the final row for a hint, so a short terminal
    // can't hide the HW/emit-ready tail without the operator noticing.
    let total = lines.len();
    let capacity = inner.height as usize;
    if capacity > 0 && total > capacity {
        let visible = capacity.saturating_sub(1).max(1);
        lines.truncate(visible);
        let hidden = total - visible;
        let full = format!(" … {} more line(s) hidden", hidden);
        // Pre-clip so narrow sidebars get a coherent message rather
        // than a hard-cut fragment mid-word.
        let short = format!(" … +{} hidden", hidden);
        let inner_w = inner.width as usize;
        let line_text = if full.chars().count() <= inner_w {
            full
        } else if short.chars().count() <= inner_w {
            short
        } else {
            clip_right(&short, inner_w)
        };
        lines.push(Line::from(Span::styled(line_text, body().fg(YEL))));
    }
    f.render_widget(Paragraph::new(lines).style(body()), inner);
}

/// Strip the default `app.` schema prefix from a display table name.
/// All other schema prefixes pass through untouched — width-aware
/// ellipsis in [`join_table_pk`] is responsible for fitting long FQNs
/// to the available cell width, without losing identity information
/// through a hard global heuristic.
fn short_table(name: &str) -> String {
    name.strip_prefix("app.").unwrap_or(name).to_string()
}

/// Natural (unclipped) character width of the combined `table / pk`
/// cell. Mirrors the layout in [`join_table_pk`] so the lane can
/// measure what each row *wants* before deciding what to give it.
fn table_pk_full_len(table: &str, pk: &str) -> usize {
    match (table.is_empty(), pk.is_empty()) {
        (true, true) => 0,
        (false, true) => table.chars().count(),
        (true, false) => pk.chars().count(),
        (false, false) => table.chars().count() + 3 + pk.chars().count(),
    }
}

/// Choose the `table / pk` and `note`/`badge` column widths from
/// the natural content sizes and the total flexible budget. The
/// policy is:
///
/// - each column gets *at most* its natural max (no phantom slack),
/// - clamped to a per-column minimum so headers still fit,
/// - capped at a per-column ceiling so one runaway long row can't
///   starve the other column.
///
/// With this, short scenarios keep the cells compact (no "huge gap"
/// between columns) and long scenarios expand the cells up to the
/// cap. Any remaining budget is deliberately left as panel-trailing
/// space rather than stuffed into a cell that doesn't need it.
/// Per-cell floor widths used by [`compute_lane_widths`]. Exposed at
/// module scope so call sites that pre-allocate budget for the LSN
/// column can subtract `LANE_FLOORS_SUM` instead of hardcoding 28.
const TPK_FLOOR: usize = 14;
const NOTE_FLOOR: usize = 14;
const LANE_FLOORS_SUM: usize = TPK_FLOOR + NOTE_FLOOR;
/// Hard minimum below which a cell stops being legible. Used only on
/// pathologically narrow terminals (≤80 cols) where the regular floors
/// can't be met simultaneously — cells still render with ellipsis
/// rather than getting silently clipped by the layout solver.
const MIN_CELL: usize = 6;
/// Lane LSN column ceiling (real DBs occasionally produce long-prefixed
/// formats; this caps content-aware sizing so a single freak LSN can't
/// starve the data columns) and floor (a 4-char `bin…` is the smallest
/// useful breadcrumb at narrow widths).
const LSN_CEIL: usize = 22;
const LSN_MIN: usize = 4;

/// Pick the LSN column width given the lane's inner width and the
/// fixed-column overhead excluding LSN. Caps at [`LSN_MIN`, `LSN_CEIL`]
/// and reserves [`LANE_FLOORS_SUM`] cells for the table/pk + note (or
/// badge) columns first, so a 80-col terminal trims the LSN with
/// ellipsis instead of pushing the rightmost data column off-frame.
fn compute_lsn_w(max_lsn: usize, inner_w: u16, fixed_minus_lsn: usize) -> usize {
    let max_lsn = max_lsn.max(3).min(LSN_CEIL);
    let after_fixed = (inner_w as usize).saturating_sub(fixed_minus_lsn);
    let lsn_budget = after_fixed.saturating_sub(LANE_FLOORS_SUM);
    max_lsn.min(lsn_budget.max(LSN_MIN))
}

fn compute_lane_widths<T>(
    rows: &[T],
    tpk_len_of: impl Fn(&T) -> usize,
    note_len_of: impl Fn(&T) -> usize,
    available: usize,
) -> (usize, usize) {
    let max_tpk = rows.iter().map(&tpk_len_of).max().unwrap_or(0);
    let max_note = rows.iter().map(&note_len_of).max().unwrap_or(0);

    // Each column wants at least its content size (clamped up to the
    // floor for empty cells). No artificial ceiling — if the content
    // fits the budget, both columns get their natural width.
    let tpk_pref = max_tpk.max(TPK_FLOOR);
    let note_pref = max_note.max(NOTE_FLOOR);

    if tpk_pref + note_pref <= available {
        // Happy path: both fit. Give any leftover slack to tpk so the
        // panel's right edge stays full and the `table / pk` cell can
        // use tier 1 (no ellipsis) more often — the three-tier
        // budgeter in `join_table_pk` stops truncating once the cell
        // is wide enough to show the full identifier.
        let slack = available - (tpk_pref + note_pref);
        let tpk_w = tpk_pref.saturating_add(slack).min(max_tpk.max(tpk_pref));
        return (tpk_w, note_pref);
    }

    // Sub-floor: budget can't even seat both column floors. Give each
    // column at least MIN_CELL chars and split the remainder 60/40.
    // Without this branch the function would return widths summing to
    // more than `available`, which the ratatui layout solver silently
    // clips — hiding text without an ellipsis indicator.
    if available < LANE_FLOORS_SUM {
        // Degenerate: not enough room for two MIN_CELL cells. Split
        // `available` directly (60/40) so the returned widths still
        // sum to ≤ available — at extreme narrow widths the cells
        // are unreadable either way, but at least the column
        // constraints don't overflow the panel inner width.
        if available < 2 * MIN_CELL {
            let tpk_w = available * 60 / 100;
            let note_w = available - tpk_w;
            return (tpk_w, note_w);
        }
        let usable = available - 2 * MIN_CELL;
        let tpk_extra = usable * 60 / 100;
        let note_extra = usable - tpk_extra;
        return (MIN_CELL + tpk_extra, MIN_CELL + note_extra);
    }

    // Over budget. Distribute the post-floors budget with a 60/40
    // split favouring `table / pk` — it's the more identity-bearing
    // cell. Each column's allocation is capped at its actual demand,
    // so a short note gives its share back to tpk and vice versa.
    let budget = available.saturating_sub(LANE_FLOORS_SUM);
    let tpk_extra = (budget * 60 / 100).min(tpk_pref - TPK_FLOOR);
    let note_extra = budget.saturating_sub(tpk_extra).min(note_pref - NOTE_FLOOR);
    (TPK_FLOOR + tpk_extra, NOTE_FLOOR + note_extra)
}

/// Render the combined `table / pk` cell clipped to `cell_width`.
/// Three-tier fallback:
///
/// 1. Full `table / pk` if it fits — no ellipsis.
/// 2. Bare-table form `table / pk` (with schema stripped) if that
///    fits — short but still complete identity.
/// 3. Two-sided ellipsis — both halves get proportional budgets so
///    the row shows `tablename… / pkvalue…` with both parts visibly
///    truncated. This is the form operators expect from wide tables
///    in other TUIs: keep enough of each half to identify the row,
///    not lose one half entirely.
fn join_table_pk(table: &str, pk: &str, cell_width: usize) -> String {
    match (table.is_empty(), pk.is_empty()) {
        (true, true) => String::new(),
        (false, true) => clip_right(table, cell_width),
        (true, false) => clip_right(pk, cell_width),
        (false, false) => {
            // Tier 1 — the full form fits.
            let full = format!("{} / {}", table, pk);
            if full.chars().count() <= cell_width {
                return full;
            }
            // Tier 2 — strip schema and retry.
            let bare = table.rsplit_once('.').map(|(_, t)| t).unwrap_or(table);
            if bare != table {
                let attempt = format!("{} / {}", bare, pk);
                if attempt.chars().count() <= cell_width {
                    return attempt;
                }
            }
            // Tier 3 — ellipsize both halves.
            let sep_chars = 3; // " / "
            let usable = cell_width.saturating_sub(sep_chars).max(6);
            // Table prefix is slightly higher-signal than the tail of
            // a composite PK literal, so give it a bit more of the
            // budget — 55/45.
            let table_budget = (usable * 55 / 100).max(4);
            let pk_budget = usable.saturating_sub(table_budget).max(3);
            let t = clip_right(bare, table_budget);
            let p = clip_right(pk, pk_budget);
            format!("{} / {}", t, p)
        }
    }
}

/// Truncate `s` to at most `max` characters, appending `…` when any
/// chars are dropped. `max` is a char budget, not a byte budget — the
/// composite keys and unicode arrows the TUI carries make char-wise
/// the only safe measure.
fn clip_right(s: &str, max: usize) -> String {
    let count = s.chars().count();
    if count <= max {
        return s.to_string();
    }
    if max == 0 {
        // No room for even an ellipsis; callers passing a saturated-down
        // budget rely on the result fitting `max` exactly.
        return String::new();
    }
    let keep = max - 1;
    let mut out: String = s.chars().take(keep).collect();
    out.push('…');
    out
}

/// Clip two strings to fit a combined `budget`, preserving more of
/// `right` than `left` when both must shrink. Used to budget multi-
/// span lines (e.g. the connection banner's `connected · {source}` +
/// `request {n} · TABLE …`) so identity-bearing trailing content
/// survives at narrow widths. Either side may collapse to the empty
/// string when the remaining budget is too tight to hold even a
/// 4-char fragment.
fn clip_two_part(left: &str, right: &str, budget: usize) -> (String, String) {
    let left_len = left.chars().count();
    let right_len = right.chars().count();
    if left_len + right_len <= budget {
        return (left.to_string(), right.to_string());
    }
    // Give `right` up to 60% of the budget (clamped to its actual
    // length and a hard 8-char minimum so we don't end up with just
    // an ellipsis); `left` takes whatever's left.
    let right_take = (budget * 60 / 100).max(8).min(right_len);
    let left_take = budget.saturating_sub(right_take);
    let left_clipped = if left_take >= 4 {
        clip_right(left, left_take)
    } else {
        String::new()
    };
    let right_budget = budget.saturating_sub(left_clipped.chars().count());
    let right_clipped = clip_right(right, right_budget);
    (left_clipped, right_clipped)
}

/// Sidebar-safe `"OP pk=…"` label. Composite-key literals
/// (`{user_id=42,order_id=9999}`) exceed the 28%-width reconciler column;
/// clip the pk portion so the line never wraps or truncates mid-character.
/// `width` is the sidebar inner width so we use as much room as we've
/// got — on a wide terminal composite keys get to breathe, on a narrow
/// one they still never overflow.
fn clip_sidebar_pk(op: &str, pk: &str, width: usize) -> String {
    // Prefix: "  ✗ " (4) + op (≈3) + " pk=" (4) = 11.
    const PREFIX_RESERVE: usize = 11;
    let budget = width
        .saturating_sub(PREFIX_RESERVE)
        .max(12);
    let clipped = clip_right(pk, budget);
    format!("{} pk={}", op, clipped)
}

/// Render the reconciler `table ...` line. Wraps the table name onto
/// a continuation line when `" table <name>"` exceeds the sidebar
/// width so fully-qualified identifiers stay visible instead of being
/// silently clipped at the paragraph edge.
fn render_table_line(name: &str, width: usize) -> Vec<Line<'static>> {
    let label_chars = " table ".chars().count();
    let available = width.saturating_sub(label_chars);
    let name_chars = name.chars().count();
    if name_chars <= available {
        return vec![Line::from(vec![
            Span::styled(" table ", gray_s()),
            Span::styled(name.to_string(), body().fg(WHITE)),
        ])];
    }
    // Split the name across lines at the inner width. Chunks land on
    // indented continuation lines so they visually read as part of
    // the same field. Prefer to break at a `.` when one falls in the
    // first available slice — this keeps `schema.table` readable as
    // schema-then-bare-table instead of a mid-word cut.
    let mut out = vec![Line::from(Span::styled(" table", gray_s()))];
    let chars: Vec<char> = name.chars().collect();
    let indent = "   ";
    let indent_chars = indent.chars().count();
    let slice = width.saturating_sub(indent_chars).max(8);
    let mut i = 0;
    while i < chars.len() {
        let end = (i + slice).min(chars.len());
        // Prefer a trailing `.` boundary for the first slice of an FQN.
        let end = if out.len() == 1 {
            chars[i..end]
                .iter()
                .rposition(|c| *c == '.')
                .map(|p| i + p + 1)
                .unwrap_or(end)
        } else {
            end
        };
        let piece: String = chars[i..end].iter().collect();
        out.push(Line::from(vec![
            Span::styled(indent.to_string(), body()),
            Span::styled(piece, body().fg(WHITE)),
        ]));
        i = end;
    }
    out
}

/// Render the reconciler `pk [min … max]` line. Short bounds stay on
/// a single line (current form); long composite-key bounds split
/// across three lines so each bound is visible in full, or clipped
/// with a trailing `…` if even a single bound exceeds the column.
fn render_pk_range(pk_min: &str, pk_max: &str, width: usize) -> Vec<Line<'static>> {
    let inline = format!(" pk    [{} … {}]", pk_min, pk_max);
    if inline.chars().count() <= width {
        return vec![Line::from(vec![
            Span::styled(" pk    [", gray_s()),
            Span::styled(pk_min.to_string(), body().fg(CYAN)),
            Span::styled(" … ", gray_s()),
            Span::styled(pk_max.to_string(), body().fg(CYAN)),
            Span::styled("]", gray_s()),
        ])];
    }
    // Leave 4-char indent plus a `[…]` bracket pair for each bound.
    let indent = "     ";
    let indent_chars = indent.chars().count();
    let bound_budget = width.saturating_sub(indent_chars + 2).max(8);
    vec![
        Line::from(Span::styled(" pk", gray_s())),
        Line::from(vec![
            Span::styled(format!("{}[", indent), gray_s()),
            Span::styled(clip_right(pk_min, bound_budget), body().fg(CYAN)),
        ]),
        Line::from(Span::styled(format!("{}  …", indent), gray_s())),
        Line::from(vec![
            Span::styled(indent.to_string(), body()),
            Span::styled(clip_right(pk_max, bound_budget), body().fg(CYAN)),
            Span::styled("]", gray_s()),
        ]),
    ]
}

fn render_buffer_lines(chunk: &ActiveChunk, width: usize) -> Vec<Line<'static>> {
    let (lo, hi) = match (
        chunk.pk_min.as_deref().and_then(|s| s.parse::<u64>().ok()),
        chunk.pk_max.as_deref().and_then(|s| s.parse::<u64>().ok()),
    ) {
        (Some(lo), Some(hi)) if hi >= lo => (lo, hi),
        // Non-numeric (composite) or non-parseable bounds — switch to
        // list mode: show bounds on separate lines so the literals stay
        // legible, then enumerate any excluded pks so the eye can still
        // see what the log evicted from the chunk. Without this, a
        // single `[min .. max]` line exceeds the sidebar width and the
        // paragraph silently clips the tail.
        _ => return render_buffer_list_mode(chunk, width),
    };

    let excluded_set: std::collections::HashSet<u64> = chunk
        .excluded_pks
        .iter()
        .filter_map(|s| s.parse::<u64>().ok())
        .collect();

    // Surviving buffer entries use CYAN — same hue as `chunk-sel` rows
    // in the source pane, so the eye links the chunk's SELECT origin to
    // its in-memory buffer representation. Excluded PKs stay YEL with a
    // CROSSED_OUT modifier (the "log won" eviction).
    let pk_span = |pk: u64| -> Span<'static> {
        let text = format!("{}", pk);
        if excluded_set.contains(&pk) {
            Span::styled(text, body().fg(YEL).add_modifier(Modifier::CROSSED_OUT))
        } else {
            Span::styled(text, body().fg(CYAN))
        }
    };

    // Show the first HEAD_COUNT consecutive pks of the chunk. If the
    // range is longer than HEAD + TAIL, insert "…" on its own line then
    // show the last TAIL pks (hi itself bolded cyan). Per-line pk count
    // is computed from the available panel width so the buffer fills
    // the column rather than leaving empty cells on the right.
    const HEAD_COUNT: u64 = 25;
    const TAIL_COUNT: u64 = 3;

    // Each pk occupies (max-pk-digits) + 2 separator chars. Reserve 4
    // cols for the "   [" prefix and 1 col for the trailing "]".
    let max_pk_chars = format!("{}", hi).chars().count().max(
        format!("{}", lo).chars().count(),
    );
    let pk_cell = max_pk_chars + 2;
    let usable = width.saturating_sub(5);
    let per_line = (usable / pk_cell.max(1)).clamp(3, 40);

    let total = hi - lo + 1;
    let show_tail = total > HEAD_COUNT + TAIL_COUNT;
    let head_end = if show_tail { lo + HEAD_COUNT - 1 } else { hi };

    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut cur: Vec<Span<'static>> = vec![Span::styled("   [", body().fg(WHITE))];
    let mut pks_on_line = 0usize;

    for pk in lo..=head_end {
        if pks_on_line >= per_line {
            lines.push(Line::from(std::mem::take(&mut cur)));
            cur.push(Span::styled("    ", body()));
            pks_on_line = 0;
        }
        cur.push(pk_span(pk));
        pks_on_line += 1;
        if pk < head_end || show_tail {
            cur.push(Span::styled("  ", body()));
        }
    }

    if show_tail {
        lines.push(Line::from(std::mem::take(&mut cur)));
        cur.push(Span::styled("    …  ", body().fg(BDIM)));
        pks_on_line = 0;
        let tail_lo = hi - TAIL_COUNT + 1;
        for pk in tail_lo..=hi {
            if pks_on_line >= per_line {
                lines.push(Line::from(std::mem::take(&mut cur)));
                cur.push(Span::styled("    ", body()));
                pks_on_line = 0;
            }
            cur.push(pk_span(pk));
            pks_on_line += 1;
            if pk < hi {
                cur.push(Span::styled("  ", body()));
            }
        }
    }

    cur.push(Span::styled("]", body().fg(WHITE)));
    lines.push(Line::from(cur));
    lines
}

/// List-mode buffer rendering — used when the chunk's pks aren't
/// parseable as numeric (e.g. UUIDs or composite-key literals). The
/// grid packing the numeric path uses assumes compact pks; with
/// 50-char composite literals the grid degenerates to a clipped
/// smear. List mode prints the range bounds on their own indented
/// lines and enumerates any excluded pks with a strikethrough so the
/// "log won" eviction is still visible. Each line is clipped to the
/// sidebar width so long literals don't silently overflow.
fn render_buffer_list_mode(chunk: &ActiveChunk, width: usize) -> Vec<Line<'static>> {
    let indent = "   ";
    let indent_chars = indent.chars().count();
    let budget = width.saturating_sub(indent_chars + 2).max(8);
    let pk_min = chunk.pk_min.as_deref().unwrap_or("—");
    let pk_max = chunk.pk_max.as_deref().unwrap_or("—");
    let mut out = vec![
        Line::from(vec![
            Span::styled(format!("{}[", indent), body().fg(WHITE)),
            Span::styled(clip_right(pk_min, budget), body().fg(CYAN)),
        ]),
        Line::from(Span::styled(format!("{}  …", indent), body().fg(BDIM))),
        Line::from(vec![
            Span::styled(indent.to_string(), body()),
            Span::styled(clip_right(pk_max, budget), body().fg(CYAN)),
            Span::styled("]", body().fg(WHITE)),
        ]),
    ];
    // Surface the excluded pks one-per-line so the "log won" evictions
    // are visible even when the whole buffer can't be enumerated. Cap
    // at the usual collision head count — the tail is summarised.
    const EXCLUDED_HEAD: usize = 3;
    if !chunk.excluded_pks.is_empty() {
        out.push(Line::from(Span::styled(
            format!("{}excluded ({}):", indent, chunk.excluded_pks.len()),
            body().fg(ORANGE),
        )));
        for pk in chunk.excluded_pks.iter().take(EXCLUDED_HEAD) {
            out.push(Line::from(vec![
                Span::styled(format!("{}  ✗ ", indent), body().fg(YEL)),
                Span::styled(
                    clip_right(pk, budget.saturating_sub(4)),
                    body().fg(YEL).add_modifier(Modifier::CROSSED_OUT),
                ),
            ]));
        }
        if chunk.excluded_pks.len() > EXCLUDED_HEAD {
            out.push(Line::from(Span::styled(
                format!("{}  ✗ +{} more", indent, chunk.excluded_pks.len() - EXCLUDED_HEAD),
                body().fg(YEL),
            )));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Lane 3 — SINK STREAM
// ---------------------------------------------------------------------------

/// Sink-lane per-row content. Mirrors [`SourceRowParts`] — same
/// measure-then-render strategy so the `table / pk` column is sized
/// to the natural content rather than a fixed constraint.
struct SinkRowParts {
    seq: String,
    lsn: String,
    op: String,
    table: String,
    pk: String,
    origin: &'static str,
    origin_color: Color,
    badge: String,
    badge_color: Color,
}

fn sink_lane(f: &mut Frame, area: Rect, ctx: &AppContext) {
    let state = &ctx.state;
    let block = lane(
        "SINK STREAM".into(),
        "post-reconciliation · what downstream consumers see".into(),
        None,
        area.width,
    );
    let inner = block.inner(area);
    f.render_widget(block, area);

    // `table / pk` combined into a single column for the same reason
    // as in `source_lane`: the slash visually groups the subject of
    // the event. The relative-time `t` column was dropped for the
    // same reason as on the source — LSN already orders events.
    let header = Row::new(vec![
        Cell::from(Span::styled(" seq",        dim_s())),
        Cell::from(Span::styled("LSN",         dim_s())),
        Cell::from(Span::styled("op",          dim_s())),
        Cell::from(Span::styled("table / pk",  dim_s())),
        Cell::from(Span::styled("origin",      dim_s())),
        Cell::from(Span::styled("badge",       dim_s())),
    ]).height(1);

    let rows_capacity = inner.height.saturating_sub(1) as usize;
    let take_n = rows_capacity.max(1);
    let entries: Vec<&SinkEventBody> = state
        .sink_output
        .iter()
        .rev()
        .take(take_n)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    let parts: Vec<SinkRowParts> = entries
        .iter()
        .map(|b| sink_row_parts(b, state))
        .collect();

    // LSN column is content- and budget-aware (see [`compute_lsn_w`]).
    // Fixed-overhead minus LSN: seq(5) + op(4) + origin(8) + 5 gaps = 22.
    let max_lsn = parts.iter().map(|p| p.lsn.chars().count()).max().unwrap_or(0);
    let lsn_w = compute_lsn_w(max_lsn, inner.width, 5 + 4 + 8 + 5);

    // Fixed columns: seq(5) + LSN(lsn_w) + op(4) + origin(8);
    // plus `column_spacing(1)` * 5 gaps = 5.
    let fixed_cols_width: usize = 5 + lsn_w + 4 + 8 + 5;
    let available = (inner.width as usize).saturating_sub(fixed_cols_width);
    let (tpk_w, badge_w) = compute_lane_widths(
        &parts,
        |p| table_pk_full_len(&p.table, &p.pk),
        |p| p.badge.chars().count(),
        available,
    );

    let body_rows: Vec<Row> = parts
        .into_iter()
        .map(|p| sink_row_render(p, lsn_w, tpk_w, badge_w))
        .collect();

    let widths = [
        Constraint::Length(5),               // seq
        Constraint::Length(lsn_w as u16),    // LSN — content-aware
        Constraint::Length(4),               // op
        Constraint::Length(tpk_w as u16),    // table / pk — content-aware
        Constraint::Length(8),               // origin
        Constraint::Length(badge_w as u16),  // badge — content-aware
    ];
    let table = Table::new(body_rows, widths).header(header).style(body()).column_spacing(1);
    f.render_widget(table, inner);
}

fn sink_row_parts(b: &SinkEventBody, state: &State) -> SinkRowParts {
    // LOG events whose LSN appears on a chunk.collision "won" the race
    // against a refresh row the chunk buffer was holding for the same
    // PK — i.e. this LOG row is what the sink saw in place of the
    // evicted SELECT. Mark them so the replacement is visible on the
    // sink pane, not only implied by the chunk-drop row on the source.
    let is_log_won = matches!(b.origin, Origin::Log) && state.is_collision_cause_lsn(&b.lsn);
    let (origin, origin_color) = match (b.origin, is_log_won) {
        (Origin::Log, true)  => ("LOG", YEL),
        (Origin::Log, false) => ("LOG", BLUE),
        (Origin::Select, _)  => ("SELECT", CYAN),
    };
    // IN-WINDOW takes the same BLUE as the LOG origin cell — high-contrast
    // enough to read as the same font weight as the LOG WON yellow, and
    // the matching hue ties the badge to its own origin column.
    let badge_color = match (b.origin, is_log_won) {
        (Origin::Log, true)  => YEL,
        (Origin::Log, false) => BLUE,
        (Origin::Select, _)  => CYAN,
    };
    // Both in-window cases lead with `chunk {id}` and use the same-width
    // uppercase classifier (IN-WINDOW / LOG WON) so their leading tokens
    // line up visually row-to-row and read as siblings of one family.
    let badge = match (b.origin, is_log_won) {
        (Origin::Log, true) => match b.chunk_id {
            Some(id) => format!("chunk {} · LOG WON", id),
            None => "LOG WON".to_string(),
        },
        (Origin::Log, false) => match b.chunk_id {
            Some(id) => format!("chunk {} · IN-WINDOW", id),
            None => String::new(),
        },
        // SELECT refresh rows carry their originating chunk id — same
        // leading `chunk {N}` token as the LOG siblings so the column
        // stays aligned and the operator can match refresh rows back to
        // the chunk that produced them without reaching into the reconciler.
        (Origin::Select, _) => match b.chunk_id {
            Some(id) => format!("chunk {}", id),
            None => String::new(),
        },
    };
    let seq = b.seq.map(|s| format!(" {}", s)).unwrap_or_else(|| "  —".into());

    SinkRowParts {
        seq,
        lsn: b.lsn.clone(),
        op: b.op.short().to_string(),
        table: short_table(&b.table),
        pk: b.pk.clone(),
        origin,
        origin_color,
        badge,
        badge_color,
    }
}

fn sink_row_render(p: SinkRowParts, lsn_w: usize, tpk_w: usize, badge_w: usize) -> Row<'static> {
    Row::new(vec![
        Cell::from(Span::styled(p.seq, body().fg(BDIM))),
        Cell::from(Span::styled(clip_right(&p.lsn, lsn_w), body().fg(BDIM))),
        Cell::from(Span::styled(
            p.op,
            body().fg(WHITE).add_modifier(Modifier::BOLD),
        )),
        Cell::from(Span::styled(
            join_table_pk(&p.table, &p.pk, tpk_w),
            body().fg(WHITE),
        )),
        Cell::from(Span::styled(
            p.origin,
            body().fg(p.origin_color).add_modifier(Modifier::BOLD),
        )),
        Cell::from(Span::styled(
            clip_right(&p.badge, badge_w),
            body().fg(p.badge_color),
        )),
    ])
}

#[cfg(test)]
mod tests {
    use super::{
        clip_right, clip_sidebar_pk, join_table_pk, render_pk_range, render_table_line,
        request_banner_label, short_table,
    };
    use chrono::Utc;
    use hydroscope::event::{RequestScope, RequestState, RequestTransitionBody};
    use hydroscope::pacing;
    use hydroscope::state::{ActiveChunk, State};
    use hydroscope::{AppContext, ConnectionStatus};
    use std::collections::VecDeque;
    use std::time::Instant;

    // ---- width-aware helpers ---------------------------------------------

    #[test]
    fn short_table_strips_app_prefix() {
        assert_eq!(short_table("app.orders"), "orders");
        assert_eq!(short_table("app.x"), "x");
    }

    #[test]
    fn short_table_preserves_foreign_schemas() {
        // Width-aware truncation in `join_table_pk` is responsible
        // for fitting FQNs to the cell — `short_table` no longer
        // applies a global heuristic that could drop schema
        // information when it'd have fit.
        assert_eq!(short_table("erp.jobs"), "erp.jobs");
        assert_eq!(
            short_table("production_analytics.customer_order_line_items"),
            "production_analytics.customer_order_line_items"
        );
    }

    #[test]
    fn clip_right_noops_on_short_input() {
        assert_eq!(clip_right("hi", 8), "hi");
        assert_eq!(clip_right("abcdefgh", 8), "abcdefgh");
    }

    #[test]
    fn clip_right_truncates_with_ellipsis() {
        assert_eq!(clip_right("abcdefghi", 5), "abcd…");
        // Ellipsis counts as one char in the budget.
        assert_eq!(clip_right("abcdefghi", 1), "…");
    }

    #[test]
    fn clip_right_returns_empty_at_zero_budget() {
        // Budget of 0 must yield "" (0 chars) — emitting "…" would
        // overshoot by 1 char, which `clip_two_part` and any other
        // caller passing a saturated-down budget would propagate
        // into the final rendered line.
        assert_eq!(clip_right("anything", 0), "");
        assert_eq!(clip_right("", 0), "");
        assert!(clip_right("foo", 0).chars().count() <= 0);
    }

    #[test]
    fn join_table_pk_tier1_full_form_fits() {
        // Plenty of room — no truncation at all.
        assert_eq!(join_table_pk("orders", "1002", 40), "orders / 1002");
    }

    #[test]
    fn join_table_pk_drops_empty_halves() {
        assert_eq!(join_table_pk("", "", 20), "");
        assert_eq!(join_table_pk("orders", "", 20), "orders");
        assert_eq!(join_table_pk("", "1002", 20), "1002");
    }

    #[test]
    fn join_table_pk_tier2_strips_schema_when_it_saves_room() {
        // 46-char table + " / 1002" = 53 chars. Target: 30.
        // Bare table + " / 1002" = 34 chars — still > 30, so we fall
        // through to tier 3. Use a smaller schema to exercise tier 2.
        let out = join_table_pk("foo.bar", "1002", 12);
        // "foo.bar / 1002" = 14, too long for 12. Bare "bar / 1002" = 10,
        // fits in 12 — so the function must produce the bare form.
        assert_eq!(out, "bar / 1002");
    }

    #[test]
    fn join_table_pk_tier3_ellipsizes_both_halves() {
        let out = join_table_pk(
            "customer_order_line_items",
            "{tenant_id=17001234,order_id=00090000000001001,line_id=2}",
            30,
        );
        // Output fits the cell budget exactly.
        assert!(
            out.chars().count() <= 30,
            "expected ≤30 chars, got {}: {:?}",
            out.chars().count(),
            out
        );
        // Has the " / " separator so both halves are visible.
        assert!(out.contains(" / "), "lost the separator: {:?}", out);
        // Both halves truncated with trailing ellipsis.
        let parts: Vec<&str> = out.split(" / ").collect();
        assert_eq!(parts.len(), 2);
        assert!(
            parts[0].ends_with('…'),
            "table half should end in ellipsis: {:?}",
            parts[0]
        );
        assert!(
            parts[1].ends_with('…'),
            "pk half should end in ellipsis: {:?}",
            parts[1]
        );
    }

    // ---- compute_lane_widths --------------------------------------------

    #[test]
    fn compute_lane_widths_sticks_to_content_sizes_when_possible() {
        // Short content: columns sized to content, not to available.
        let rows = vec![("orders / 1002", "chunk 42 · IN-WINDOW")];
        let (tpk, note) = super::compute_lane_widths(
            &rows,
            |r| r.0.chars().count(),
            |r| r.1.chars().count(),
            100, // lots of budget
        );
        // tpk content is 13 chars; floor is 14 → expect 14.
        assert_eq!(tpk, 14);
        // note content is 20 chars; no phantom slack — stays 20.
        assert_eq!(note, 20);
    }

    #[test]
    fn compute_lane_widths_respects_ceilings_under_pressure() {
        // Very long content should clamp at the 60% / 45% ceilings
        // rather than growing unbounded.
        let long = "x".repeat(200);
        let rows = vec![(long.as_str(), long.as_str())];
        let (tpk, note) = super::compute_lane_widths(
            &rows,
            |r| r.0.chars().count(),
            |r| r.1.chars().count(),
            100,
        );
        assert!(tpk <= 60, "tpk exceeded ceiling: {}", tpk);
        assert!(note <= 45, "note exceeded ceiling: {}", note);
    }

    #[test]
    fn compute_lane_widths_never_exceeds_available_at_any_budget() {
        // Property: for every budget from 0..=100, the returned widths
        // sum to ≤ available. The original sub-floor branch hard-coded
        // a (6, 6) lower bound and silently overshot when available < 12,
        // letting the layout solver clip without ellipsis. Sweeping the
        // full range here guards every degenerate budget at once.
        let long = "x".repeat(200);
        let rows = vec![(long.as_str(), long.as_str())];
        for available in 0..=100 {
            let (tpk, note) = super::compute_lane_widths(
                &rows,
                |r| r.0.chars().count(),
                |r| r.1.chars().count(),
                available,
            );
            assert!(
                tpk + note <= available,
                "tpk({}) + note({}) = {} > available({})",
                tpk,
                note,
                tpk + note,
                available,
            );
        }
    }

    #[test]
    fn clip_sidebar_pk_respects_dynamic_width() {
        // Narrow sidebar: pk gets a small budget.
        let narrow = clip_sidebar_pk("UPD", "{tenant_id=17001234,order_id=9}", 30);
        assert!(narrow.starts_with("UPD pk="));
        assert!(narrow.ends_with('…') || narrow.chars().count() <= 30);
        // Wide sidebar: more of the pk is visible.
        let wide = clip_sidebar_pk("UPD", "{tenant_id=17001234,order_id=9}", 120);
        assert_eq!(wide, "UPD pk={tenant_id=17001234,order_id=9}");
    }

    #[test]
    fn render_table_line_fits_short_names_on_one_line() {
        let lines = render_table_line("orders", 40);
        assert_eq!(lines.len(), 1);
    }

    #[test]
    fn render_table_line_wraps_long_fqn() {
        // A 46-char name in a 40-char sidebar wraps onto continuation lines.
        let lines = render_table_line(
            "production_analytics.customer_order_line_items",
            40,
        );
        assert!(lines.len() >= 2, "expected at least 2 lines, got {}", lines.len());
    }

    #[test]
    fn render_pk_range_stays_inline_for_short_bounds() {
        let lines = render_pk_range("1001", "1040", 40);
        assert_eq!(lines.len(), 1);
    }

    #[test]
    fn render_pk_range_wraps_long_bounds() {
        let lines = render_pk_range(
            "{tenant_id=17001234,order_id=00090000000001001,line_id=2}",
            "{tenant_id=17001234,order_id=00090000000001040,line_id=5}",
            40,
        );
        // Wrapped form: label + [min / … / max]
        assert!(lines.len() >= 4);
    }

    fn test_context() -> AppContext {
        let (pace, _pacer) = pacing::channel(0, false);
        AppContext {
            state: State::new(),
            pace,
            connection: ConnectionStatus::Connected,
            source_label: "http://127.0.0.1:8085/api/v1/tap/stream".into(),
            last_disconnect: None,
            parse_errors: VecDeque::new(),
            last_outcome: None,
            started_at: Instant::now(),
        }
    }

    #[test]
    fn banner_falls_back_to_active_chunk_request_when_no_transition_was_seen() {
        let mut ctx = test_context();
        ctx.state.active_chunk = Some(ActiveChunk {
            chunk_id: 42,
            request_id: Some("99".into()),
            ..ActiveChunk::default()
        });

        assert_eq!(request_banner_label(&ctx), "request 99 · active chunk");
    }

    #[test]
    fn banner_prefers_matching_request_transition_details_when_available() {
        let mut ctx = test_context();
        ctx.state.active_chunk = Some(ActiveChunk {
            chunk_id: 42,
            request_id: Some("99".into()),
            ..ActiveChunk::default()
        });
        ctx.state.latest_request = Some(RequestTransitionBody {
            v: 1,
            seq: Some(7),
            ts: Utc::now(),
            run_id: "run-1".into(),
            source_id: "source-1".into(),
            request_id: "99".into(),
            scope: RequestScope::Table,
            table: Some("app.orders".into()),
            state: RequestState::Active,
            prev_state: None,
            reason: None,
        });

        assert_eq!(
            request_banner_label(&ctx),
            "request 99 · TABLE orders · ACTIVE"
        );
    }

    // ---- Snapshot tests ----------------------------------------------------
    //
    // These render the TUI into a `TestBackend` buffer at a fixed geometry,
    // rasterise the cell grid to plain text, and hand the text to `insta` for
    // snapshot comparison. Style (color, bold) is intentionally dropped from
    // the snapshot so theme tweaks don't churn the files — only *layout* or
    // *content* drift flips a test red.
    //
    // The canonical regression these would catch: the "fixed `Length(20)`"
    // build that silently clipped `production_analytics` with no ellipsis
    // would produce a visibly different character grid for the long-name
    // scenario and fail the `snap_long_wide_after_lw` snapshot.

    use hydroscope::demo::{chunk42_long_scenario, chunk42_scenario};
    use hydroscope::event::TapEvent;
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;
    use ratatui::Terminal;

    /// Apply the first `take_n` events of a scenario into a fresh `State`.
    /// Picking a deterministic event count (rather than a wall-clock time
    /// cutoff) is what makes the snapshots reproducible across machines.
    fn state_after(events: Vec<TapEvent>, take_n: usize) -> State {
        let mut s = State::new();
        for e in events.into_iter().take(take_n) {
            s.apply(e);
        }
        s
    }

    /// Rasterise the buffer's cell grid to a single plain-text string — one
    /// row per line, no styling. Using `symbol()` (rather than a color-coded
    /// representation) keeps the snapshot stable under palette changes.
    fn buffer_to_text(buffer: &Buffer) -> String {
        let area = buffer.area;
        let mut out = String::new();
        for y in 0..area.height {
            for x in 0..area.width {
                out.push_str(buffer[(x, y)].symbol());
            }
            // Trim trailing whitespace per row so "end of row" is unambiguous
            // in the snapshot and accidental trailing-space drift doesn't
            // trip a diff.
            while out.ends_with(' ') {
                out.pop();
            }
            out.push('\n');
        }
        out
    }

    /// Drive `render` once into a `TestBackend` of the given size and return
    /// the rasterised text. A small wrapper so each snapshot test is a
    /// two-liner.
    fn render_to_text(width: u16, height: u16, ctx: &AppContext) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        terminal
            .draw(|f| super::render(f, ctx))
            .expect("render should not fail");
        buffer_to_text(terminal.backend().buffer())
    }

    /// A long-enough prefix of `chunk42_scenario` to exercise the main
    /// rendering paths in one frame: LW received, a handful of CDCs (incl.
    /// two collisions), HW received, and the first few SELECT refresh rows.
    const CHUNK42_RENDER_PREFIX: usize = 32;

    #[test]
    fn snap_short_wide_after_hw() {
        let mut ctx = test_context();
        ctx.source_label = "demo://chunk-42".into();
        ctx.state = state_after(chunk42_scenario(), CHUNK42_RENDER_PREFIX);
        let text = render_to_text(160, 40, &ctx);
        insta::assert_snapshot!(text);
    }

    #[test]
    fn snap_long_wide_after_hw() {
        let mut ctx = test_context();
        ctx.source_label = "demo://chunk-42-long".into();
        ctx.state = state_after(chunk42_long_scenario(), CHUNK42_RENDER_PREFIX);
        let text = render_to_text(160, 40, &ctx);
        insta::assert_snapshot!(text);
    }

    #[test]
    fn snap_short_narrow_after_hw() {
        let mut ctx = test_context();
        ctx.source_label = "demo://chunk-42".into();
        ctx.state = state_after(chunk42_scenario(), CHUNK42_RENDER_PREFIX);
        let text = render_to_text(100, 32, &ctx);
        insta::assert_snapshot!(text);
    }

    #[test]
    fn snap_long_narrow_after_hw() {
        let mut ctx = test_context();
        ctx.source_label = "demo://chunk-42-long".into();
        ctx.state = state_after(chunk42_long_scenario(), CHUNK42_RENDER_PREFIX);
        let text = render_to_text(100, 32, &ctx);
        insta::assert_snapshot!(text);
    }

    #[test]
    fn snap_empty_state_no_chunk_active() {
        // Exercises the "no chunk active" branch of the reconciler and
        // the connection banner with no request info — the blank-canvas
        // state a freshly-launched hydroscope shows before any events.
        let ctx = test_context();
        let text = render_to_text(160, 30, &ctx);
        insta::assert_snapshot!(text);
    }

    // ---- Anatomy-section PNG generator -----------------------------------
    //
    // Renders the showcase scenario into a `TestBackend` buffer and
    // rasterises each anatomy region (title bar, banner, source log, sink
    // stream, reconciler, footer) to its own PNG, straight from ratatui's
    // `Cell` grid — no real terminal, no VHS, no crop step. Cell colours
    // (fg/bg/bold) survive untouched, so the page screenshots match the
    // live TUI palette exactly.
    //
    // Run on demand:
    //
    //   cargo test --release generate_anatomy_screenshots -- --ignored --nocapture
    //
    // Loads `/System/Library/Fonts/SFNSMono.ttf` at runtime — macOS-only by
    // design (this is a maintainer tool, not a build-time dep).

    use fontdue::{Font, FontSettings};
    use image::{Rgb, RgbImage};
    use ratatui::style::{Color as RColor, Modifier};
    use std::path::PathBuf;

    /// Pixel size of one terminal cell in the rasterised PNG.
    const CELL_W: u32 = 12;
    const CELL_H: u32 = 24;
    /// Font size in points fed to fontdue. ~85 % of CELL_H gives one row of
    /// breathing room above descenders.
    const FONT_PX: f32 = 20.0;
    /// Baseline y-offset within a cell, top-down.
    const BASELINE_Y: i32 = 19;
    /// Hydroscope BG fallback when a cell has no explicit background.
    const DEFAULT_BG: [u8; 3] = [6, 13, 28];
    const DEFAULT_FG: [u8; 3] = [228, 237, 250];

    fn load_sf_mono() -> Font {
        let bytes = std::fs::read("/System/Library/Fonts/SFNSMono.ttf")
            .expect("SF Mono not at /System/Library/Fonts/SFNSMono.ttf — run on macOS or vendor a TTF");
        Font::from_bytes(bytes, FontSettings::default()).expect("invalid SF Mono TTF")
    }

    fn rgb_from(c: Option<RColor>, fallback: [u8; 3]) -> [u8; 3] {
        match c {
            Some(RColor::Rgb(r, g, b)) => [r, g, b],
            Some(RColor::Black) => [0, 0, 0],
            Some(RColor::White) => [255, 255, 255],
            _ => fallback,
        }
    }

    /// Slice a region of the buffer and rasterise it to an `RgbImage`.
    /// `(x0, y0)` is the top-left cell; `(w, h)` the cell-grid size.
    fn rasterise(
        buffer: &ratatui::buffer::Buffer,
        x0: u16,
        y0: u16,
        w: u16,
        h: u16,
        font: &Font,
    ) -> RgbImage {
        let img_w = w as u32 * CELL_W;
        let img_h = h as u32 * CELL_H;
        let mut img = RgbImage::from_pixel(img_w, img_h, Rgb(DEFAULT_BG));

        for y in 0..h {
            for x in 0..w {
                let cell = &buffer[(x0 + x, y0 + y)];
                let style = cell.style();
                let bg = rgb_from(style.bg, DEFAULT_BG);
                let fg = rgb_from(style.fg, DEFAULT_FG);
                let bold = style.add_modifier.contains(Modifier::BOLD);

                let cell_x0 = x as u32 * CELL_W;
                let cell_y0 = y as u32 * CELL_H;

                // Fill cell background.
                for cy in 0..CELL_H {
                    for cx in 0..CELL_W {
                        img.put_pixel(cell_x0 + cx, cell_y0 + cy, Rgb(bg));
                    }
                }

                // Rasterise glyphs in the cell symbol (usually one char,
                // occasionally a combining sequence). We pack them
                // left-to-right within the cell width.
                let mut pen_x: i32 = 0;
                for ch in cell.symbol().chars() {
                    if ch == ' ' {
                        pen_x += CELL_W as i32;
                        continue;
                    }
                    let (m, bitmap) = font.rasterize(ch, FONT_PX);
                    if m.width == 0 || m.height == 0 {
                        pen_x += m.advance_width.round() as i32;
                        continue;
                    }
                    // fontdue convention: xmin is left-bearing from cursor,
                    // ymin is offset of glyph bottom from baseline (positive
                    // = above baseline, negative = descender below).
                    let glyph_left = pen_x + m.xmin;
                    let glyph_top = BASELINE_Y - m.height as i32 - m.ymin;
                    for (i, &alpha) in bitmap.iter().enumerate() {
                        if alpha == 0 {
                            continue;
                        }
                        let bx = (i % m.width) as i32;
                        let by = (i / m.width) as i32;
                        let px = cell_x0 as i32 + glyph_left + bx;
                        let py = cell_y0 as i32 + glyph_top + by;
                        if px < 0 || py < 0 || px >= img_w as i32 || py >= img_h as i32 {
                            continue;
                        }
                        // Bold: gamma-bias the alpha so strokes thicken
                        // slightly without needing a separate font face.
                        let a = if bold {
                            ((alpha as f32 / 255.0).powf(0.55) * 255.0).min(255.0) as u8
                        } else {
                            alpha
                        };
                        let af = a as f32 / 255.0;
                        let bg_px = img.get_pixel(px as u32, py as u32).0;
                        let blended = [
                            (fg[0] as f32 * af + bg_px[0] as f32 * (1.0 - af)) as u8,
                            (fg[1] as f32 * af + bg_px[1] as f32 * (1.0 - af)) as u8,
                            (fg[2] as f32 * af + bg_px[2] as f32 * (1.0 - af)) as u8,
                        ];
                        img.put_pixel(px as u32, py as u32, Rgb(blended));
                    }
                    pen_x += m.advance_width.round() as i32;
                }
            }
        }
        img
    }

    #[test]
    #[ignore = "writes PNGs to docs/img — run via: cargo test --release generate_anatomy_screenshots -- --ignored --nocapture"]
    fn generate_anatomy_screenshots() {
        let font = load_sf_mono();

        // chunk42 prefix at 32 events leaves the chunk mid-life: LW
        // received, two collisions, HW received, first refresh rows on the
        // sink — same state the snap_short_wide_after_hw test snapshots,
        // so every anatomy region renders meaningful content.
        let mut ctx = test_context();
        ctx.source_label = "demo://chunk-42".into();
        ctx.state = state_after(chunk42_scenario(), CHUNK42_RENDER_PREFIX);

        // Geometry mirrors render() in this file: title (1) + banner (1) +
        // main (rest) + footer (1), with main split 72/28 horizontally and
        // the left column split 50/50 vertically.
        let cols: u16 = 160;
        let rows: u16 = 40;
        let backend = TestBackend::new(cols, rows);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        terminal
            .draw(|f| super::render(f, &ctx))
            .expect("render should not fail");
        let buffer = terminal.backend().buffer().clone();

        let main_h: u16 = rows - 3; // 1 title + 1 banner + 1 footer
        let left_w: u16 = cols * 72 / 100;
        let right_w: u16 = cols - left_w;
        let top_h: u16 = main_h / 2;
        let bot_h: u16 = main_h - top_h;
        let main_y0: u16 = 2;

        // `cargo test` runs from the crate root (ops/tap-tui), so a relative
        // `docs/img/` is exactly where the page expects them.
        let dest: PathBuf = PathBuf::from("docs/img");
        std::fs::create_dir_all(&dest).expect("create docs/img");

        let regions: &[(&str, u16, u16, u16, u16)] = &[
            ("anat_titlebar.png", 0, 0, cols, 1),
            ("anat_banner.png", 0, 1, cols, 1),
            ("anat_sourcelog.png", 0, main_y0, left_w, top_h),
            ("anat_sinkstream.png", 0, main_y0 + top_h, left_w, bot_h),
            ("anat_reconciler.png", left_w, main_y0, right_w, main_h),
            ("anat_footer.png", 0, rows - 1, cols, 1),
        ];

        for (name, x, y, w, h) in regions {
            let img = rasterise(&buffer, *x, *y, *w, *h, &font);
            let path = dest.join(name);
            img.save(&path).expect("write png");
            println!(
                "wrote {} ({}x{} cells -> {}x{} px)",
                path.display(),
                w,
                h,
                img.width(),
                img.height()
            );
        }
    }

}

// ---------------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------------

fn footer(f: &mut Frame, area: Rect, _ctx: &AppContext) {
    let key_style = body().fg(BLUE).reversed();
    // Build both forms; pick whichever fits `area.width`. Measuring the
    // full form rather than hardcoding a threshold keeps the choice in
    // sync with future edits to either span list.
    let full_spans = vec![
        Span::styled(" q ",     key_style), Span::styled(" quit   ",         gray_s()),
        Span::styled(" space ", key_style), Span::styled(" step / advance ", gray_s()),
        Span::styled(" s ",     key_style), Span::styled(" toggle step ",    gray_s()),
        Span::styled(" n/m ",   key_style), Span::styled(" slower / faster ", gray_s()),
        Span::styled(" f ",     key_style), Span::styled(" full speed ",     gray_s()),
    ];
    let full_len: usize = full_spans.iter().map(|s| s.content.chars().count()).sum();
    let l = if (area.width as usize) >= full_len {
        Line::from(full_spans)
    } else {
        Line::from(vec![
            Span::styled(" q ",     key_style), Span::styled(" quit ",  gray_s()),
            Span::styled(" space ", key_style), Span::styled(" step ",  gray_s()),
            Span::styled(" s ",     key_style), Span::styled(" stop ",  gray_s()),
            Span::styled(" n/m ",   key_style), Span::styled(" pace ",  gray_s()),
            Span::styled(" f ",     key_style), Span::styled(" fast ",  gray_s()),
        ])
    };
    f.render_widget(Paragraph::new(l).style(body()), area);
}
