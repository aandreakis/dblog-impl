# Local Host-Run Example Configs

These example configs are designed for the common evaluation flow where:

- the source/target databases run from `ops/docker/compose.yml`, and
- DBLog itself runs on the host via `./gradlew bootRun`.

For first-contact validation, prefer one of these local examples before
running the longer Gradle verification suites.

Use them with:

macOS/Linux:

```bash
./gradlew bootRun --args="--spring.config.additional-location=file:./PATH/TO/application.properties"
```

Windows PowerShell / CMD:

```powershell
.\gradlew.bat bootRun --args="--spring.config.additional-location=file:./PATH/TO/application.properties"
```

Current shipped local example configs:

- `mysql-to-ndjson/application.properties`
- `mysql-to-postgres/application.properties`
- `postgres-to-mysql/application.properties`

The companion `scripts/demo/*.py` helpers automatically choose a free localhost
control-plane port when their default port is already occupied. Set
`DBLOG_CONTROL_PLANE_PORT` to pin a specific port. Those helpers stop their
isolated Docker database fixture stack on exit; set
`DBLOG_DEMO_KEEP_CONTAINERS=1` to keep it around for inspection.

These host-run configs bind the control plane directly to `127.0.0.1`. The
separate packaged Docker example still keeps host access local by publishing
`127.0.0.1:8085` from Docker.

These are intentionally example-sized and biased toward observability:

- local control plane enabled,
- small chunk size to make request lifecycle easier to observe,
- retry enabled for transient source connection loss,
- state persisted under `build/example-state/...`.

The source reconnect retry covers availability failures such as short-lived
database connection loss. Contract and correctness boundaries, including
unsupported DDL and schema incompatibility, still fail closed by design.

If you recreate the disposable Docker database fixtures from scratch, clear the
matching `build/example-state/...` files before restarting DBLog. H2 state files
survive Docker volume resets, and stale checkpoints can point at source history
that no longer exists:

```bash
rm -f build/example-state/mysql-to-postgres/runtime-state.mv.db \
      build/example-state/mysql-to-postgres/runtime-state.trace.db \
      build/example-state/mysql-to-ndjson/runtime-state.mv.db \
      build/example-state/mysql-to-ndjson/runtime-state.trace.db \
      build/example-state/mysql-to-ndjson/events.ndjson \
      build/example-state/postgres-to-mysql/runtime-state.mv.db \
      build/example-state/postgres-to-mysql/runtime-state.trace.db
```

See the repo's top-level [README](../../../../README.md) and [AGENTS.md](../../../../AGENTS.md) for the broader context.
