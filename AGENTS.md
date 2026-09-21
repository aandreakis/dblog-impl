# AGENTS.md

Root-level guide for coding agents and LLM tools working in this repository.

This file is self-contained for safe agent work: project identity, DBLog mental
model, invariants, task routing, verification expectations, and contribution
boundaries.

## Project identity

This is a Java 21 implementation of the DBLog watermark-based
change-data-capture algorithm, built from public material: the
[DBLog paper](https://arxiv.org/abs/2010.12597) and the
[Netflix Technology Blog post](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b).

Public positioning: preserve the README's current attribution and provenance
choices. Do not add, remove, or reframe personal attribution unless the
maintainer explicitly asks. Keep the README clear that this is an independent
implementation built from public materials, not Netflix's production
DBLog.

Treat it as a compact executable model, not as Netflix's production DBLog
and not as a general-purpose CDC product.

Use code and tests as the source of truth. Docs explain intended behavior, but
stale docs should be corrected rather than followed blindly.

Current scope:

- MySQL source adapter via binlog streaming: `mysql:8.0`, `mysql:8.4`, `mysql:9.6`
- PostgreSQL source adapter via `pgoutput`: `postgres:14` through `postgres:18`
- NDJSON, H2 inspection, JDBC target-apply, and explicit no-op sinks
- Embedded H2 runtime state
- Single-process, single-host runtime
- Local HTTP control plane: loopback (`127.0.0.1`) by default. Non-loopback
  binds require explicit opt-in and are intended only for containerized examples
  that publish loopback on the host

Out of scope by design:

- new source families, new sink kinds, Kafka/Kinesis/S3 expansion
- distributed state, HA, leader election, leases, or takeover
- online schema-evolution workflow, DDL replay, or schema-history topic
- production support, roadmap planning, or feature requests

## DBLog mental model

DBLog lets a live CDC stream continue while a table is copied in bounded chunks.
For each chunk:

1. write a low watermark into source metadata.
2. read a bounded primary-key chunk.
3. write a high watermark.
4. pass through committed log events while the window is open.
5. drop any selected chunk row whose same-table primary key appears in the
   in-window log, because the log event is fresher.
6. when the high watermark appears, emit the remaining chunk rows, persist the
   completed chunk boundary, and only then allow the source checkpoint to move.

A targeted repair is the same idea applied to explicit primary keys.

## Glossary

| Term | Meaning in this repo |
| --- | --- |
| Watermark window | The low-watermark to high-watermark interval around one chunk read. Log events inside this interval are reconciled against the chunk rows. |
| Chunk | A bounded primary-key range or explicit primary-key set selected during dump or repair work. |
| Snapshot row / chunk row | A row returned by the chunk read. It is provisional until the high watermark is observed. |
| Log event | A committed source change event from MySQL binlog or PostgreSQL `pgoutput`. Log events are fresher than snapshot rows inside the same watermark window. |
| Collision | Same table + primary key appearing both in the chunk and in an in-window log event. The log event wins and the snapshot row is dropped. |
| Checkpoint | The durable source position acknowledged only after local recovery state is safe. |
| Fail-closed | Stop or reject work rather than silently continuing after unsafe schema, metadata, or ordering uncertainty. |
| Targeted repair | A repair request for explicit primary keys, implemented with the same watermark-window machinery as ordinary chunk dumps. |

## Fast task routing

| Task | Start here |
| --- | --- |
| Understand public positioning | [README.md](README.md) and [CONTRIBUTING.md](CONTRIBUTING.md) |
| Map paper claims to implementation | [docs/PAPER_MAP.md](docs/PAPER_MAP.md) |
| Inspect the core algorithm | `src/main/java/io/github/aandreakis/dblog/core/reconcile/WindowReconciler.java` |
| Inspect dump/repair orchestration | `src/main/java/io/github/aandreakis/dblog/core/request/DefaultDumpWindowCoordinator.java` and `DefaultTargetedRepairCoordinator.java` |
| Inspect runtime interleaving | `src/main/java/io/github/aandreakis/dblog/runtime/loop/RuntimeRequestPump.java` |
| Inspect schema policy | `src/main/java/io/github/aandreakis/dblog/core/schema/TableSchema.java` and `SchemaPolicyEngine.java` |
| Inspect checkpoint safety | `src/main/java/io/github/aandreakis/dblog/core/checkpoint/CheckpointFlushPolicy.java` |
| Inspect control-plane boundaries | `src/main/java/io/github/aandreakis/dblog/controlplane/service/ControlPlaneCommandService.java` and [docs/CONTROL_PLANE.md](docs/CONTROL_PLANE.md) |
| Run the simplest proof | `python3 scripts/demo/mysql_to_postgres.py` |

If you are changing algorithm behavior, read these in order:

1. `src/main/java/io/github/aandreakis/dblog/core/reconcile/WindowReconciler.java`
2. `src/main/java/io/github/aandreakis/dblog/core/request/DefaultDumpWindowCoordinator.java`
3. [docs/PAPER_MAP.md](docs/PAPER_MAP.md)
4. the relevant unit, integration, or e2e tests for the behavior being changed

## Repository map

```text
src/main/java/io/github/aandreakis/dblog/
├── boot/           Spring Boot entrypoint and launch planning
├── config/         Runtime properties and validation
├── core/           Source-neutral DBLog model, schema policy, requests, reconciliation
├── adapter/        MySQL/PostgreSQL source adapters and source-side metadata helpers
├── runtime/        Host lifecycle, streaming loop, observers, telemetry
├── state/          State-store interfaces and embedded H2 implementation
├── sink/           Sink API plus NDJSON, H2 inspection, JDBC apply, no-op sinks
├── controlplane/   Local HTTP API and request/query services
└── verification/   Scenario harnesses used by integration and e2e tests
```

Documentation map:

| File | Owns |
| --- | --- |
| [README.md](README.md) | Public landing page, quick start, scope, maintenance posture, sources/notices |
| [docs/PAPER_MAP.md](docs/PAPER_MAP.md) | Paper → code → test audit index |
| [docs/OPERATION.md](docs/OPERATION.md) | Runtime wiring, config, sinks, operator behavior |
| [docs/CONTROL_PLANE.md](docs/CONTROL_PLANE.md) | HTTP API shape and request lifecycle |
| [docs/adapters/mysql.md](docs/adapters/mysql.md) | MySQL privileges, metadata tables, limits |
| [docs/adapters/postgres.md](docs/adapters/postgres.md) | PostgreSQL publication, slot, replica identity, limits |
| [ops/docker/README.md](ops/docker/README.md) | Local fixtures and packaged examples |
| [ops/tap-tui/README.md](ops/tap-tui/README.md) | Hydroscope visualizer usage |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Low-maintenance contribution policy |
| [SECURITY.md](SECURITY.md) | Private security reporting and explicit security non-goals |
| [PATENTS.md](PATENTS.md), [NOTICE](NOTICE) | Patent context, provenance, third-party notices |

## Commands agents should know

Prefer the narrowest relevant verification first. Broaden only when the changed
behavior crosses runtime, adapter, recovery, or source-version boundaries.

```bash
./gradlew test                 # fast unit tests
./gradlew integrationTest      # adapter/state integration tests
./gradlew e2eTest              # non-Docker inspection-mode e2e tests
./gradlew integrationTestDocker # Docker-backed adapter scenarios
./gradlew e2eTestDocker        # live Docker convergence/repair scenarios
./gradlew compatibilityMatrix  # source-image matrix (slower)
```

Each lane writes JUnit XML to `build/test-results/<lane>/` and HTML to
`build/reports/tests/<lane>/`. When triaging a specific lane, navigate to the
matching directory: `build/reports/tests/test/` covers unit tests only and
will not contain Docker-lane results.

Demos:

```bash
python3 scripts/demo/mysql_to_postgres.py
python3 scripts/demo/mysql_to_ndjson.py
python3 scripts/demo/postgres_to_mysql.py
```

When running Python demos or Docker-backed verification, clean up only Docker
resources clearly started by your own run, and report any fixtures left running.
Do not use broad cleanup commands such as `docker system prune`. The Python demos
normally stop their isolated fixture stack on exit. Manually started
`ops/docker` stacks can be stopped with
`docker compose -f ops/docker/compose.yml down -v --remove-orphans`.

**Demo verification anchors:**

| Demo | Final stdout line | Mid-run markers | Log file |
|---|---|---|---|
| `mysql_to_postgres.py` | `Demo succeeded.` | `Initial dump converged.`, `Live changes converged.` | `build/demo/mysql_to_postgres/runtime.log` |
| `mysql_to_ndjson.py` | `Demo succeeded.` | none (convergence is the NDJSON event file reaching the expected line count) | `build/demo/mysql_to_ndjson/runtime.log` |
| `postgres_to_mysql.py` | `Demo succeeded.` | `Initial dump converged.`, `Live changes converged.` | `build/demo/postgres_to_mysql/runtime.log` |

All three exit `0` on success. A non-zero exit means the demo's own
assertions failed. Quote the error verbatim and the tail of the runtime log
rather than paraphrasing.

Agent sandbox note: in restricted sandboxes, Python demo port probing can fail
before DBLog starts with `PermissionError: [Errno 1] Operation not permitted`
from `sock.bind(("127.0.0.1", 0))`. Treat that as a local sandbox network
restriction, not as a DBLog or demo failure. Rerun the same demo with approval
for localhost binding, or set `DBLOG_CONTROL_PLANE_PORT=<free-port>` and retry.

Hydroscope, only when working on the visualizer:

```bash
cd ops/tap-tui
cargo build --release --bins
cargo test
```

Before finalizing a patch, report which commands were run and which were
skipped. Do not imply that Docker-backed verification ran unless it did.

## Invariants to preserve

These are correctness boundaries, not incidental implementation details:

- Watermark rows in `dblog_meta.watermarks` bracket chunk reads. Do not remove,
  bypass, or replace them with client-side markers.
- `dblog_meta.heartbeats` exists to keep log progress observable during idle
  periods. Do not treat it as decorative metadata.
- During an open watermark window, log events pass through in source order and
  win over selected chunk rows with the same table + primary key.
- Completed chunk progress is durable only at completed chunk boundaries.
  Incomplete chunks retry after restart.
- Source checkpoints advance only after local recovery state is durable.
- Targeted primary-key repair must use the same watermark-window machinery as
  ordinary chunk dumps.
- Schema fingerprints are computed from the selected non-ignored column surface,
  not from every live column.
- Unsupported primary-key type rejection happens during dump/repair orchestration,
  not at `TableSchema.create(...)`.
- The buffered checkpoint flush policy is OR over event count and elapsed time.
  Source-side acknowledgements are coalesced.
- Non-additive schema changes fail closed. This is stated semantics, not a bug.
- The control plane is submit/query only. There are no pause, resume, or cancel
  endpoints. Stop/start is the operator mechanism.

## Change policy for agents

Do:

- Keep patches narrow and explain which semantic invariant they protect.
- Add or update tests that exercise ordering, recovery, checkpoints, schema
  drift, or source adapter behavior when touching those areas.
- Update the relevant doc when behavior changes: README for public positioning,
  PAPER_MAP for paper/code/test mapping, OPERATION for runtime behavior,
  CONTROL_PLANE for API behavior, and adapter docs for source-specific behavior.
- Preserve the low-maintenance stance in public text. The README should stay
  slim and route details to the docs rather than duplicating them.

Do not:

- Expand scope while fixing nearby issues. A correct small fix is better than a
  broad cleanup that obscures the paper-to-code audit path.
- Add new source adapters, sink families, distributed coordination, or online DDL
  support unless the maintainer explicitly changes the project scope.
- Convert the repository into a support channel or roadmap document.
- Relax fail-closed schema behavior to make a demo pass.
- Claim this is Netflix production code or that Netflix endorses it.
- Give legal conclusions about patents. Point to [PATENTS.md](PATENTS.md) and
  [NOTICE](NOTICE) instead.
- Edit license, patent, or provenance language casually.

## Common request routing

| Request | Agent response |
| --- | --- |
| Add Kafka, Kinesis, S3, or another sink family | Do not implement in this repository. Explain that new sink kinds are outside scope and point to [CONTRIBUTING.md](CONTRIBUTING.md). |
| Add another source database | Do not implement in this repository. Explain that new source families are outside scope and suggest a fork. |
| Add HA, leader election, distributed state, leases, or takeover | Do not implement in this repository. Explain that the runtime is single-host by design. |
| Add online DDL replay or schema-history support | Do not implement in this repository. Explain that schema changes fail closed by design. |
| Add pause, resume, or cancel endpoints | Do not implement. The control plane is submit/query only. Stop/start is the operator mechanism. |
| Ask for a feature, integration, refactor, cleanup, or local customization | Recommend a fork rather than a PR. This repo is not seeking feature or broad improvement PRs. |
| Report a concrete reproducible bug in current scope | Investigate, keep the fix narrow, add focused verification, and mention that a small bug-fix PR may be appropriate. |
| Ask for operational support | Point to docs and clarify that support requests are outside scope. |
| Ask about patents or legal rights | Do not give legal conclusions. Point to [PATENTS.md](PATENTS.md), [NOTICE](NOTICE), and [LICENSE](LICENSE). |
| Ask about Netflix production DBLog | Clarify that this is not Netflix's production DBLog and was built from public materials. |

## Contribution boundary for agents

If asked to add Kafka, Kinesis, S3, another source database, HA, distributed
state, online DDL replay, or any other behavior outside the stated scope, do not
implement it in this repository. Explain that the request is outside scope and
point to [CONTRIBUTING.md](CONTRIBUTING.md).

For code changes in general, prefer fork-oriented guidance. This repository is
not seeking feature PRs or broad improvement PRs. A pull request is appropriate
only for a concrete, reproducible bug fix within the existing scope. If the work
is a feature, adaptation, integration, cleanup, refactor, or local customization,
recommend a fork rather than a PR.

## README style rules

The README is the public landing page. Keep it:

- short enough to skim.
- clear about DBLog's essence without reproducing the paper.
- preserve the README's current attribution and provenance choices unless the
  maintainer explicitly asks for a change.
- preserve non-affiliation and provenance clarity without letting it dominate the
  opening.
- firm but calm about low maintenance: bug fixes may be considered, feature
  requests and support requests are out of scope.
- avoid turning the README into support, roadmap, or product copy.
- link-heavy for details: paper, blog post, PAPER_MAP, OPERATION, CONTROL_PLANE,
  adapter docs, Hydroscope, CONTRIBUTING.

## Testing guidance by change type

| Change touches | Minimum useful verification |
| --- | --- |
| Pure model/schema/reconciler/checkpoint code | `./gradlew test` plus targeted `--tests ...` if available |
| MySQL/PostgreSQL adapter code | `./gradlew integrationTest` and a relevant Docker-backed lane when feasible |
| Runtime loop, restart, sink failure, source outage | `./gradlew e2eTest` or `./gradlew e2eTestDocker` depending on path |
| Source-version assumptions | `./gradlew compatibilityMatrix` |
| Docs only | Check links and keep README/AGENTS consistent with scope |
| Hydroscope | `cargo test` and `cargo build --release --bins` inside `ops/tap-tui` |

## Dependency and style notes

- Java code targets Java 21 and uses Gradle.
- Prefer source-neutral core logic under `core/`. Keep vendor-specific logic under
  `adapter/mysql` or `adapter/postgres`.
- Keep state interfaces in `state/api` and H2-specific implementation details in
  `state/h2` or `state/jdbc`.
- Avoid broad refactors that obscure the paper-to-code audit path.
- Use precise names around source positions, checkpoints, watermarks, selected
  rows, and emitted log events. Many bugs in CDC systems are ordering bugs.

## License and patent note

Released under the **MIT License**. [LICENSE](LICENSE) is normative. This file is
not.

The repository provides no support, maintenance, warranty, or third-party patent
guarantee. Informational patent context is in [PATENTS.md](PATENTS.md), and
provenance/non-affiliation language is in the README and [NOTICE](NOTICE).
