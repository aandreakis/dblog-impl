# DBLog TUI: Hydroscope

Rust/ratatui consumer for the DBLog educational observability tap.
Renders the watermark-based chunked-CDC algorithm live: source tap
stream on the top-left, sink stream (post-reconciliation) on the
bottom-left, and an active-chunk reconciler sidebar on the right.

> 📖 **Full walkthrough with annotated screenshots and a live
> animation:** [Hydroscope walkthrough](https://aandreakis.github.io/dblog-impl/ops/tap-tui/docs/).

Layout (72% / 28% horizontal split):

```
┌─────────────────────────────────┬──────────────┐
│ SOURCE EVENTS (tap stream)      │              │
│                                 │              │
├─────────────────────────────────┤  RECONCILER  │
│ SINK STREAM (post-reconciled)   │              │
│                                 │              │
└─────────────────────────────────┴──────────────┘
```

The reconciler is a fixed sidebar (doesn't collapse when chunks
complete), flowing in algorithm time-order: `chunk identity → LW →
chunk buffer → in-window CDC → HW → replaced-by-cdc → emit-ready`.

Consumes the live tap at `GET /api/v1/tap/stream` or a built-in
procedural demo (`--scenario`) for offline teaching / verification.

## Build

```
cargo build --release --bins
```

Output: `target/release/hydroscope`.

## Demo scenarios

Because key exclusions from a chunk are rare to reproduce against a real
DB, the demo source ships with curated scenarios that exercise them on
demand. All scenarios emit the same `TapEvent`s the live wire produces,
so the UI rendering path is identical.

| scenario            | shape                                                                                                        |
|---------------------|--------------------------------------------------------------------------------------------------------------|
| `chunk42`           | **Finite.** Single chunk, 2 collisions, 38 refresh rows. Good introductory scenario (~65 events). Default when you pass `--demo`. |
| `chunk42-standby`   | **Finite.** `chunk42` plus a `stream.standby` + `stream.resumed` pair at the tail.                           |
| `chunk42-long`      | **Finite.** `chunk42` with fully-qualified table names and composite primary-key literals for layout stress. |
| `showcase`          | **Infinite.** Scripted intro (4 chunks, increasing contention, standby, final-chunk + request COMPLETED) followed by procedural continuous traffic that keeps going until you quit. Default pace: 100 ms/event. |
| `showcase-long`     | **Infinite.** `showcase` with fully-qualified table names and composite primary-key literals.                |

The `showcase` intro walks through, in order:

1. Pre-request warmup: heartbeat + plain CDC + a checkpoint
2. Request 42 submitted (scope TABLE, app.orders) → ACTIVE
3. **Chunk 1**: 15 rows, 0 collisions (baseline pass without exclusions)
4. Plain-CDC gap with a DELETE and an INSERT
5. **Chunk 2**: 15 rows, **3 collisions** (including one DELETE hit)
6. `stream.standby` + `stream.resumed` (backpressure interlude)
7. **Chunk 3**: 20 rows, **5 collisions** (heavy contention)
8. **Chunk 4**: 8 rows, 1 collision, `final_chunk=true`
9. Request 42 COMPLETED
10. Post-request plain CDC
11. Final heartbeat before the continuous loop starts

After the intro, the continuous loop randomly emits: plain CDC bursts
(orders + accounts, mixed INS/UPD/DEL), chunks (10–30 rows, 0–5
collisions each), heartbeats, checkpoints, and occasional standby/resumed
pairs. The RNG is deterministic (xorshift seeded from a fixed constant) so
replays are reproducible.

### Running the demos

```
# Infinite demo for an open-ended session
target/release/hydroscope --scenario showcase

# Finite single-chunk teaching scenario: the core algorithm in miniature
target/release/hydroscope --scenario chunk42

# chunk42 with a standby pair at the tail
target/release/hydroscope --scenario chunk42-standby

# Finite layout-stress variant
target/release/hydroscope --scenario chunk42-long

# Infinite layout-stress variant
target/release/hydroscope --scenario showcase-long

# Shorthand: --demo == --scenario chunk42
target/release/hydroscope --demo
```

## Live mode (against a running DBLog)

### 1. Enable the tap on DBLog

In the DBLog runtime's `application.properties`:

```properties
dblog.control-plane.enabled=true
dblog.tap.enabled=true
# dblog.tap.queue-capacity=65536
# dblog.tap.standby-threshold-ms=1000
# dblog.tap.heartbeat-interval=2s
```

See `docs/OPERATION.md § 5.1.2` and `docs/CONTROL_PLANE.md § 5.4` in the
DBLog repo.

### 2. Start DBLog, then attach

```
# No scenario flag = live mode. Default URL is 127.0.0.1:8085.
target/release/hydroscope
target/release/hydroscope --url http://host:port/api/v1/tap/stream
```

`503 tap_not_enabled` shows up as a clear disconnect banner with
reconnect backoff. Run-id changes (process restart) reset UI state.

> **Never enable the tap in production.** The tap deliberately blocks
> the DBLog pump when the subscriber can't keep up. That is the whole
> point. Teaching machines only.

## Pacing: event rate control

The tap is TCP-flow-controlled, so a slow reader backpressures DBLog.
The pacer is the reader-side throttle.

### Command-line flags

| flag                  | default (demo / live)             | effect                                               |
|-----------------------|-----------------------------------|------------------------------------------------------|
| `--scenario <name>`   | none                              | `chunk42` \| `chunk42-standby` \| `chunk42-long` \| `showcase` \| `showcase-long` |
| `--demo`              | off                               | shorthand for `--scenario chunk42`                   |
| `--slowdown <ms>`     | 100 (showcase) / 350 (chunk42) / 0 (live) | sleep N ms between events (higher = slower)   |
| `--step`              | off                               | start in step mode: one event per `space` keypress  |
| `--url <URL>`         | `http://127.0.0.1:8085/api/v1/tap/stream` | tap endpoint (live mode)                             |

### Runtime keys

| key         | effect                                                                     |
|-------------|----------------------------------------------------------------------------|
| `q` / `Esc` | quit                                                                       |
| `Ctrl+C`    | quit                                                                       |
| `space`     | in step mode, advance one event. Otherwise, enter step mode                     |
| `s`         | toggle step mode                                                           |
| `n`         | slower: pace by +100 ms                                                   |
| `m`         | faster: pace by −100 ms (clamped to 0)                                    |
| `f`         | full speed (delay = 0)                                                     |

Pace state is shown in the banner: `full` / `N ms/event` / `STEP (space=next)`.

## Test

```  
cargo test
cargo build --release --bins
```

Tests cover: envelope parsing (all 13 kinds), lenient-line parsing
(forward-compat skip of unknown kinds, contract enforcement for `seq` on
OOB vs non-OOB), state-machine apply (new-run reset, duplicate-seq drop,
gap detection, OOB non-advancement), collision-tint retention past the
32-chunk history cap, the chunk42 replay (exactly 38 SELECT refresh
rows, matching `chunk.completed.emitted`), replay idempotence under
duplicates, the full showcase intro (4 chunks with excluded counts
`[0, 3, 5, 1]`, one standby, one request ACTIVE→COMPLETED, a DELETE
cdc, and a final chunk), the continuous-loop's forever-until-shutdown
invariant, plus integration-level fixtures in `tests/` that pin the
reconciler's in-window CDC gating, the excluded counter/list agreement,
and the out-of-order chunk.completed recovery.

## Architecture

```
src/
├── event.rs      : wire types (serde-tagged enum on `kind`, 13 variants)
├── state.rs      : consumer state machine (apply events → derived view)
├── demo.rs       : finite chunk42 scenarios (standard, standby, long-name)
├── showcase.rs   : procedural infinite generator (standard and long-name)
├── source.rs     : live HTTP reader + demo/showcase thread runners
├── pacing.rs     : atomic delay + bounded-channel step gate
├── lib.rs        : shared runner (`run_app`), CLI, terminal guard
└── bin/
    └── hydroscope.rs : three-pane rendering (source, sink, reconciler)
```

Layering: reader thread → channel → UI thread owns `State` → the bin
renders from an `AppContext` view. Zero `unsafe`, zero async. Panic
hook restores the terminal before the message prints.

## Screenshots

`screenshots/` contains baseline PNGs captured via `vhs`:

- `2_hydroscope.png`: chunk42 mid-refresh (window CLOSING)
- `4_hydroscope_open.png`: chunk42 window OPEN, HW pending
- `6_hydroscope_showcase.png`: showcase scenario mid-chunk-3

Regenerate via `vhs screenshots/<name>.tape`.

## Known limitations

- Seq dedup is active but there's no server-side peek-then-commit fix
  yet (see `CONTROL_PLANE.md § 5.4` delivery-semantics note).
- Multi-sink fan-out (one CDC → N `sink.event` rows) renders per-row, with
  dedupe / per-sink split as future work.
- Collision marking matches rows by LSN. The source pane tints the `cdc`
  row that caused a collision, and the sink pane marks the matching `LOG`
  row as `LOG WON`. The index of collision LSNs keeps the most recent
  2000 entries.
