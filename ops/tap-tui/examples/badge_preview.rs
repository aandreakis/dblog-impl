//! Side-by-side preview of four ways to render the in-window vs LOG WON
//! sink badges. Addresses the "in-window looks thinner than LOG WON"
//! visual mismatch. Each panel shows an interleaved mini-stream of sink
//! rows so you can compare the pair at a glance.
//!
//! Run:
//!   /Users/.../ops/tap-tui/target/release/examples/badge_preview

use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Cell, Paragraph, Row, Table},
    Frame, Terminal,
};
use std::io::{self, Write};

// Steel Navy palette — mirrors hydroscope.rs so preview matches live.
const BG: Color = Color::Rgb(6, 13, 28);
const BLUE: Color = Color::Rgb(106, 177, 255);
const BDIM: Color = Color::Rgb(58, 119, 184);
const WHITE: Color = Color::Rgb(228, 237, 250);
const GRAY: Color = Color::Rgb(111, 126, 149);
const YEL: Color = Color::Rgb(234, 185, 92);
const CYAN: Color = Color::Rgb(102, 211, 192);
// Dim-yellow for option C — lower-saturation member of the YEL family.
const YEL_DIM: Color = Color::Rgb(180, 150, 90);

fn body() -> Style { Style::default().bg(BG).fg(WHITE) }
fn dim_s() -> Style { body().fg(BDIM) }
fn head() -> Style { body().fg(BLUE).add_modifier(Modifier::BOLD) }

#[derive(Clone, Copy)]
enum Variant {
    /// Current: GRAY foreground, no bold.
    Current,
    /// A: BDIM blue, no bold.
    Bdim,
    /// B: GRAY + BOLD.
    GrayBold,
    /// C: dim-yellow family (same hue as LOG WON, lower saturation).
    YelDim,
}

fn main() -> io::Result<()> {
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout.lock());
    let mut terminal = Terminal::new(backend)?;

    terminal.clear()?;
    terminal.draw(|f| {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),  // header
                Constraint::Length(12), // current
                Constraint::Length(1),
                Constraint::Length(12), // A
                Constraint::Length(1),
                Constraint::Length(12), // B
                Constraint::Length(1),
                Constraint::Length(12), // C
                Constraint::Min(0),
            ])
            .split(f.area());

        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(" SINK BADGE WEIGHT — in-window vs LOG WON, variant preview ", head()),
            ]))
            .style(body()),
            rows[0],
        );

        render_panel(f, rows[1], "CURRENT — GRAY, no bold (the problem)", Variant::Current);
        render_panel(f, rows[3], "A — BDIM blue, no bold", Variant::Bdim);
        render_panel(f, rows[5], "B — GRAY + BOLD", Variant::GrayBold);
        render_panel(f, rows[7], "C — dim-yellow (same hue family as LOG WON)", Variant::YelDim);
    })?;

    std::thread::sleep(std::time::Duration::from_millis(50));
    let mut out = io::stdout();
    writeln!(out)?;
    Ok(())
}

fn render_panel(f: &mut Frame, area: Rect, title: &str, variant: Variant) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(BLUE).bg(BG))
        .title(Line::from(vec![Span::styled(format!(" {} ", title), head())]))
        .style(body());
    let inner = block.inner(area);
    f.render_widget(block, area);

    let header = Row::new(vec![
        Cell::from(Span::styled(" seq", dim_s())),
        Cell::from(Span::styled("op", dim_s())),
        Cell::from(Span::styled("table", dim_s())),
        Cell::from(Span::styled("pk", dim_s())),
        Cell::from(Span::styled("origin", dim_s())),
        Cell::from(Span::styled("badge", dim_s())),
    ])
    .height(1);

    // Interleaved rows roughly matching the user's screenshot: several
    // in-window LOGs mixed with LOG WON, closing with two SELECTs.
    let rows_out: Vec<Row<'static>> = vec![
        sink_row("  9", "INS", "app.orders", "2044", Origin::Log, None, variant),
        sink_row(" 11", "UPD", "app.orders", "1002", Origin::Log, Some(true), variant),
        sink_row(" 14", "UPD", "app.orders", "1004", Origin::Log, Some(true), variant),
        sink_row(" 16", "UPD", "app.accounts", "77", Origin::Log, Some(false), variant),
        sink_row(" 18", "INS", "app.orders", "2045", Origin::Log, Some(false), variant),
        sink_row(" 19", "UPD", "app.orders", "1006", Origin::Log, Some(true), variant),
        sink_row(" 21", "UPD", "app.accounts", "77", Origin::Log, Some(false), variant),
        sink_row(" 22", "UPD", "app.orders", "1007", Origin::Log, Some(true), variant),
        sink_row(" 40", "UPD", "app.orders", "1001", Origin::Select, None, variant),
        sink_row(" 41", "UPD", "app.orders", "1003", Origin::Select, None, variant),
    ];

    let widths = [
        Constraint::Length(5),
        Constraint::Length(5),
        Constraint::Length(12),
        Constraint::Length(6),
        Constraint::Length(7),
        Constraint::Min(40),
    ];
    let table = Table::new(rows_out, widths).header(header).style(body()).column_spacing(1);
    f.render_widget(table, inner);
}

#[derive(Clone, Copy)]
enum Origin { Log, Select }

fn sink_row(
    seq: &str,
    op: &str,
    table: &str,
    pk: &str,
    origin: Origin,
    log_won: Option<bool>,
    variant: Variant,
) -> Row<'static> {
    let (origin_text, origin_color) = match (origin, log_won) {
        (Origin::Log, Some(true))  => ("LOG", YEL),
        (Origin::Log, _)           => ("LOG", BLUE),
        (Origin::Select, _)        => ("SELECT", CYAN),
    };

    // Badge text + style depend on row and variant.
    let (badge_text, badge_style) = match (origin, log_won) {
        (Origin::Log, Some(true)) => (
            "chunk 19 · LOG WON · evicted SELECT".to_string(),
            body().fg(YEL),
        ),
        (Origin::Log, Some(false)) => {
            let text = "chunk 19 · IN-WINDOW".to_string();
            let style = match variant {
                Variant::Current  => body().fg(GRAY),
                Variant::Bdim     => body().fg(BDIM),
                Variant::GrayBold => body().fg(GRAY).add_modifier(Modifier::BOLD),
                Variant::YelDim   => body().fg(YEL_DIM),
            };
            (text, style)
        }
        (Origin::Log, None) => (String::new(), body().fg(GRAY)),
        (Origin::Select, _) => (String::new(), body().fg(CYAN)),
    };

    Row::new(vec![
        Cell::from(Span::styled(format!(" {}", seq), body().fg(BDIM))),
        Cell::from(Span::styled(op.to_string(), body().fg(WHITE).add_modifier(Modifier::BOLD))),
        Cell::from(Span::styled(table.to_string(), body().fg(WHITE))),
        Cell::from(Span::styled(format!("pk={}", pk), body().fg(WHITE))),
        Cell::from(Span::styled(origin_text, body().fg(origin_color).add_modifier(Modifier::BOLD))),
        Cell::from(Span::styled(badge_text, badge_style)),
    ])
}
