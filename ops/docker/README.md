# Docker Assets

This directory contains the Docker pieces used by the demos, the
integration-test fixtures, and the packaged example deployment.

These are example assets — they are not a production deployment blueprint.

## Database fixtures

`compose.yml` is the base local database stack used by the shipped demos and
by the Docker-backed integration / end-to-end tests.

Current images:

- MySQL 8.4
- PostgreSQL 18

Start the databases only:

```bash
docker compose -f ops/docker/compose.yml up -d
```

The shipped fixture compose publishes MySQL and PostgreSQL on `127.0.0.1`
only, so the disposable demo credentials are not exposed on non-loopback host
interfaces by default.

Stop and remove them:

```bash
docker compose -f ops/docker/compose.yml down -v
```

> **Note on MySQL auth.** The fixture starts MySQL 8.4 with
> `--mysql-native-password=ON` and creates the `dblog` user via
> `IDENTIFIED WITH mysql_native_password`. This keeps the deprecated auth
> plugin available for compatibility with the binlog tooling used by the
> integration tests. MySQL 9.x removes the plugin entirely — if you bump the
> image, switch `ops/docker/mysql/init/01-create-users.sql` to
> `caching_sha2_password` and drop the server flag.

## Local host-run examples

The recommended evaluation flow is to keep the databases in Docker and run
DBLog itself on the host via `./gradlew bootRun`.

Example application properties:

- [`examples/local/mysql-to-ndjson/application.properties`](./examples/local/mysql-to-ndjson/application.properties)
- [`examples/local/mysql-to-postgres/application.properties`](./examples/local/mysql-to-postgres/application.properties)
- [`examples/local/postgres-to-mysql/application.properties`](./examples/local/postgres-to-mysql/application.properties)

Launch one of them:

```bash
./gradlew bootRun --args="--spring.config.additional-location=file:./ops/docker/examples/local/mysql-to-postgres/application.properties"
```

The control plane binds `127.0.0.1` in all host-run examples. Remote access
is out of scope.

The host-run examples persist H2 state under `build/example-state/...`. If you
recreate the MySQL or PostgreSQL fixture containers from scratch, first clear
the matching host-run state so DBLog does not resume from a stale checkpoint
against a fresh source history:

```bash
rm -f build/example-state/mysql-to-postgres/runtime-state.mv.db \
      build/example-state/mysql-to-postgres/runtime-state.trace.db \
      build/example-state/mysql-to-ndjson/runtime-state.mv.db \
      build/example-state/mysql-to-ndjson/runtime-state.trace.db \
      build/example-state/mysql-to-ndjson/events.ndjson \
      build/example-state/postgres-to-mysql/runtime-state.mv.db \
      build/example-state/postgres-to-mysql/runtime-state.trace.db
```

## Packaged example deployment

`compose.runtime.yml` overlays a containerized DBLog built from the root
`Dockerfile` on top of the database fixtures. It demonstrates the
`MySQL -> DBLog -> PostgreSQL` path in a single `docker compose` command.

Start the packaged example:

```bash
docker compose -f ops/docker/compose.yml -f ops/docker/compose.runtime.yml up -d --build
```

Start the packaged example with the additional NDJSON inspection sink:

```bash
docker compose \
  -f ops/docker/compose.yml \
  -f ops/docker/compose.mysql-to-postgres-with-ndjson.yml \
  up -d --build
```

That variant uses `examples/mysql-to-postgres-with-ndjson/application.properties`
and writes recent emitted events to
`ops/docker/example-state/mysql-to-postgres-with-ndjson/mysql-to-postgres-events.ndjson`
on the host.

End-to-end packaged proof:

```bash
docker compose -f ops/docker/compose.yml -f ops/docker/compose.runtime.yml down -v
rm -f ops/docker/example-state/mysql-to-postgres/runtime-state.mv.db \
      ops/docker/example-state/mysql-to-postgres/runtime-state.trace.db \
      ops/docker/example-state/mysql-to-postgres/mysql-to-postgres-events.ndjson

docker compose -f ops/docker/compose.yml up -d
until docker compose -f ops/docker/compose.yml exec -T mysql mysqladmin ping -uroot -proot --silent; do sleep 1; done
until docker compose -f ops/docker/compose.yml exec -T postgres pg_isready -U postgres -d app; do sleep 1; done

docker compose -f ops/docker/compose.yml exec -T mysql \
  mysql -udblog -pdblog -e "USE app; TRUNCATE TABLE sample_orders; INSERT INTO sample_orders (id, customer_name, status) VALUES (1, 'packaged-one', 'PENDING'), (2, 'packaged-two', 'READY');"
docker compose -f ops/docker/compose.yml exec -T postgres \
  psql -U postgres -d app -c "TRUNCATE TABLE app.sample_orders;"

docker compose -f ops/docker/compose.yml -f ops/docker/compose.runtime.yml up -d --build
until curl -fsS http://127.0.0.1:8085/api/v1/runtime/status; do sleep 1; done
curl -sS -X POST http://127.0.0.1:8085/api/v1/requests \
  -H 'Content-Type: application/json' \
  -d '{"scope":"ALL_TABLES"}'

until [ "$(docker compose -f ops/docker/compose.yml exec -T postgres psql -U postgres -d app -tAc "SELECT COUNT(*) FROM app.sample_orders")" = "2" ]; do sleep 1; done

docker compose -f ops/docker/compose.yml exec -T mysql \
  mysql -udblog -pdblog -e "USE app; UPDATE sample_orders SET status='SYNCED' WHERE id=1; INSERT INTO sample_orders (id, customer_name, status) VALUES (3, 'packaged-three', 'NEW'); DELETE FROM sample_orders WHERE id=2;"

until [ "$(docker compose -f ops/docker/compose.yml exec -T postgres psql -U postgres -d app -tAc "SELECT status FROM app.sample_orders WHERE id=1")" = "SYNCED" ]; do sleep 1; done
until [ "$(docker compose -f ops/docker/compose.yml exec -T postgres psql -U postgres -d app -tAc "SELECT COUNT(*) FROM app.sample_orders WHERE id=3")" = "1" ]; do sleep 1; done
until [ "$(docker compose -f ops/docker/compose.yml exec -T postgres psql -U postgres -d app -tAc "SELECT COUNT(*) FROM app.sample_orders WHERE id=2")" = "0" ]; do sleep 1; done

docker compose -f ops/docker/compose.yml exec -T postgres \
  psql -U postgres -d app -c "TABLE app.sample_orders;"
```

That proves the packaged container boots, accepts an `ALL_TABLES` request, copies
the initial MySQL rows into PostgreSQL, and then applies later live MySQL
changes into PostgreSQL.

Stop and remove it:

```bash
docker compose -f ops/docker/compose.yml -f ops/docker/compose.runtime.yml down -v
```

If you recreate the MySQL or PostgreSQL fixture containers from scratch,
first clear the bind-mounted runtime state so DBLog does not resume from a
stale checkpoint against a fresh source history:

```bash
rm -f ops/docker/example-state/mysql-to-postgres/runtime-state.mv.db \
      ops/docker/example-state/mysql-to-postgres/runtime-state.trace.db \
      ops/docker/example-state/mysql-to-postgres/mysql-to-postgres-events.ndjson \
      ops/docker/example-state/mysql-to-postgres-with-ndjson/runtime-state.mv.db \
      ops/docker/example-state/mysql-to-postgres-with-ndjson/runtime-state.trace.db \
      ops/docker/example-state/mysql-to-postgres-with-ndjson/mysql-to-postgres-events.ndjson
```

Inside the container DBLog binds `0.0.0.0:8085` so Docker port publishing
works; the overlay publishes it on `127.0.0.1:8085` on the host.

Inspect the control plane:

```bash
curl -sS http://127.0.0.1:8085/api/v1/runtime/status
```

Run the packaged MySQL startup-check example:

```bash
docker compose \
  -f ops/docker/compose.yml \
  -f ops/docker/compose.mysql-startup-check.yml \
  up --build --abort-on-container-exit --exit-code-from dblog dblog
```

This uses `examples/mysql-startup-check/application.properties` and validates
the fixture-backed MySQL source plus PostgreSQL target preflight without
launching the runtime request loop.

Run the packaged PostgreSQL startup-check example:

```bash
docker compose \
  -f ops/docker/compose.yml \
  -f ops/docker/compose.postgres-startup-check.yml \
  up --build --abort-on-container-exit --exit-code-from dblog dblog
```

This uses `examples/postgres-startup-check/application.properties` and verifies
the fixture-backed PostgreSQL source contract with the dedicated `dblog` role,
including replica identity, publication, and logical-slot readiness. It uses an
explicit no-op sink because startup-check does not launch the runtime request
loop or emit change events.

The packaged `Dockerfile` sets production-oriented JVM defaults via
`JAVA_TOOL_OPTIONS` (G1GC, container-aware memory sizing, heap-dump on OOM,
rolling JFR to the mounted state directory). Override by providing your own
`JAVA_TOOL_OPTIONS` in a compose override file. The host-run `./gradlew
bootRun` path does **not** apply these flags automatically.

## Layout

| Path                                                 | Purpose                                                   |
| ---------------------------------------------------- | --------------------------------------------------------- |
| `compose.yml`                                        | MySQL and PostgreSQL fixture services                      |
| `compose.runtime.yml`                                | Packaged DBLog container overlay                           |
| `compose.mysql-to-postgres-with-ndjson.yml`          | Packaged runtime overlay with NDJSON inspection sink        |
| `compose.mysql-startup-check.yml`                    | Packaged MySQL startup-check override                       |
| `compose.postgres-startup-check.yml`                 | Packaged PostgreSQL startup-check override                  |
| `examples/local/`                                    | Host-run `application.properties` templates                |
| `examples/mysql-to-postgres/`                        | Packaged-deploy `application.properties`                   |
| `examples/mysql-to-postgres-with-ndjson/`            | Packaged-deploy with NDJSON sink                           |
| `examples/mysql-startup-check/`                      | Packaged-deploy in startup-check boot mode                 |
| `examples/postgres-startup-check/`                   | Packaged PostgreSQL startup-check `application.properties` |
| `example-state/mysql-to-postgres/`                   | Bind-mount target for the packaged example's H2 state      |
| `example-state/mysql-to-postgres-with-ndjson/`       | Bind-mount target for packaged NDJSON state and events      |
| `example-state/mysql-startup-check/`                 | Bind-mount target for MySQL startup-check state             |
| `example-state/postgres-startup-check/`              | Bind-mount target for PostgreSQL startup-check state        |
| `mysql/init/`                                        | MySQL initialization SQL                                   |
| `postgres/init/`                                     | PostgreSQL initialization SQL                              |
