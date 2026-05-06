//! Side-by-side preview of three ways to render the CDC + chunk-drop pair
//! in the source pane. Run with `cargo run --example row_preview --release`
//! or capture via vhs. Not wired into hydroscope; pick a variant and I'll
//! port it into the real source_row.

#![allow(dead_code)]

use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Cell, Paragraph, Row, Table},
    Terminal,
};
use std::io::{self, Write};

// Steel Navy palette, copied from hydroscope.rs so the preview
// matches what you'd see live.
const BG: Color = Color::Rgb(6, 13, 28);
const BLUE: Color = Color::Rgb(106, 177, 255);
const BDIM: Color = Color::Rgb(58, 119, 184);
const WHITE: Color = Color::Rgb(228, 237, 250);
const GRAY: Color = Color::Rgb(111, 126, 149);
const YEL: Color = Color::Rgb(234, 185, 92);
const RED: Color = Color::Rgb(240, 132, 112);

fn body() -> Style { Style::default().bg(BG).fg(WHITE) }
fn dim_s() -> Style { body().fg(BDIM) }
fn head() -> Style { body().fg(BLUE).add_modifier(Modifier::BOLD) }

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
                Constraint::Length(8),  // current (2 rows — baseline)
                Constraint::Length(1),
                Constraint::Length(8),  // A1
                Constraint::Length(1),
                Constraint::Length(8),  // A2
                Constraint::Length(1),
                Constraint::Length(8),  // B1
                Constraint::Length(1),
                Constraint::Length(8),  // B2
                Constraint::Length(1),
                Constraint::Length(8),  // D
                Constraint::Min(0),
            ])
            .split(f.area());

        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(" SOURCE-PANE COLLISION RENDERING — 1-line variants ", head()),
            ]))
            .style(body()),
            rows[0],
        );

        render_variant(f, rows[1], "CURRENT — two rows (seq 9 = cdc, seq 10 = chunk-drop)", |rows_out| {
            rows_out.push(build_row(
                "  9", "t+0.021", "binlog.000042:1152", ("cdc", YEL), ("UPD", YEL),
                "app.orders pk=1002", "",
                WHITE,
            ));
            rows_out.push(build_row(
                " 10", "t+0.022", "binlog.000042:1152", ("chunk-drop", YEL), ("DROP", YEL),
                "chunk 42 · pk=1002", "UPD",
                WHITE,
            ));
        });

        render_variant(f, rows[3], "A1 — one row, kind=`excluded`, narrative note (seq shows the cdc's)", |rows_out| {
            rows_out.push(build_row(
                "  9", "t+0.021", "binlog.000042:1152", ("excluded", YEL), ("UPD", YEL),
                "app.orders pk=1002", "evicted from chunk 42 buffer",
                WHITE,
            ));
        });

        render_variant(f, rows[5], "A2 — one row, kind=`excluded`, terser note (chunk id in table col)", |rows_out| {
            rows_out.push(build_row(
                "  9", "t+0.021", "binlog.000042:1152", ("excluded", YEL), ("UPD", YEL),
                "app.orders pk=1002 · chunk 42", "LOG won",
                WHITE,
            ));
        });

        render_variant(f, rows[7], "B1 — one row, kind stays `cdc`, YEL tint + evicted note", |rows_out| {
            rows_out.push(build_row(
                "  9", "t+0.021", "binlog.000042:1152", ("cdc", YEL), ("UPD", YEL),
                "app.orders pk=1002", "evicted from chunk 42 buffer",
                YEL,
            ));
        });

        render_variant(f, rows[9], "B2 — one row, kind=`cdc`, short note", |rows_out| {
            rows_out.push(build_row(
                "  9", "t+0.021", "binlog.000042:1152", ("cdc", YEL), ("UPD", YEL),
                "app.orders pk=1002", "→ chunk 42 evict",
                YEL,
            ));
        });

        render_variant(f, rows[11], "D — one row, compound kind `log-wins` (outcome-first)", |rows_out| {
            rows_out.push(build_row(
                "  9", "t+0.021", "binlog.000042:1152", ("log-wins", YEL), ("UPD", YEL),
                "app.orders pk=1002", "vs chunk 42 SELECT refresh",
                WHITE,
            ));
        });
    })?;

    // Leave the rendering on-screen for the screenshot tool, then exit cleanly.
    std::thread::sleep(std::time::Duration::from_millis(50));
    let mut out = io::stdout();
    writeln!(out)?;
    Ok(())
}

fn render_variant<F>(f: &mut ratatui::Frame, area: ratatui::layout::Rect, title: &str, populate: F)
where
    F: FnOnce(&mut Vec<Row<'static>>),
{
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
        Cell::from(Span::styled(" t", dim_s())),
        Cell::from(Span::styled("LSN", dim_s())),
        Cell::from(Span::styled("kind", dim_s())),
        Cell::from(Span::styled("op", dim_s())),
        Cell::from(Span::styled("table / pk", dim_s())),
        Cell::from(Span::styled("note", dim_s())),
    ])
    .height(1);

    let widths = [
        Constraint::Length(5),
        Constraint::Length(9),
        Constraint::Length(22),
        Constraint::Length(12),
        Constraint::Length(5),
        Constraint::Length(38),
        Constraint::Min(20),
    ];

    let mut rows_out: Vec<Row<'static>> = Vec::new();
    populate(&mut rows_out);

    let table = Table::new(rows_out, widths)
        .header(header)
        .style(body())
        .column_spacing(1);
    f.render_widget(table, inner);
}

fn build_row(
    seq: &str,
    t: &str,
    lsn: &str,
    kind: (&str, Color),
    op: (&str, Color),
    table_pk: &str,
    note: &str,
    row_fg: Color,
) -> Row<'static> {
    let (kind_text, kind_color) = kind;
    let (op_text, op_color) = op;
    Row::new(vec![
        Cell::from(Span::styled(format!(" {}", seq), body().fg(BDIM))),
        Cell::from(Span::styled(format!(" {}", t), body().fg(WHITE))),
        Cell::from(Span::styled(lsn.to_string(), body().fg(BDIM))),
        Cell::from(Span::styled(kind_text.to_string(), body().fg(kind_color).add_modifier(Modifier::BOLD))),
        Cell::from(Span::styled(op_text.to_string(), body().fg(op_color))),
        Cell::from(Span::styled(table_pk.to_string(), body().fg(row_fg))),
        Cell::from(Span::styled(note.to_string(), body().fg(kind_color))),
    ])
}
