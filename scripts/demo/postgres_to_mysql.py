#!/usr/bin/env python3
"""Demo: PostgreSQL -> DBLog -> MySQL.

Port of the previous ``postgres_to_mysql.sh``. Runs on Linux, macOS, and
Windows with a Docker daemon and Python 3.9+.

Invoke from the repository root:

    python scripts/demo/postgres_to_mysql.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import lib  # noqa: E402


DEMO_NAME = "postgres_to_mysql"


def main() -> None:
    demo_dir = lib.prepare_demo_dir(DEMO_NAME)
    log_file = demo_dir / "runtime.log"
    state_path = demo_dir / "state"
    control_plane_port = lib.choose_control_plane_port(18087)
    control_plane_url = lib.control_plane_url(control_plane_port)
    source_id = "postgres_to_mysql_demo"
    source_table = "demo.sample_orders"
    publication_name = "pg_to_mysql_demo_pub"
    slot_name = "pg_to_mysql_demo_slot"

    # Example deployment shape for this demo:
    # - boot mode: runtime
    # - source: PostgreSQL demo.sample_orders
    # - target: MySQL demo.sample_orders
    # - control plane enabled for request submission and status inspection
    dblog_args = [
        "--dblog.boot-mode=runtime",
        f"--dblog.runtime.state-path={state_path}",
        "--dblog.control-plane.enabled=true",
        f"--dblog.control-plane.port={control_plane_port}",
        "--dblog.source.adapter=postgres",
        f"--dblog.source.id={source_id}",
        f"--dblog.source.tables[0]={source_table}",
        "--dblog.source.postgres.jdbc-url=jdbc:postgresql://127.0.0.1:5432/app",
        "--dblog.source.postgres.replication-jdbc-url=jdbc:postgresql://127.0.0.1:5432/app",
        "--dblog.source.postgres.database-name=app",
        "--dblog.source.postgres.username=dblog",
        "--dblog.source.postgres.password=dblog",
        f"--dblog.source.postgres.publication-name={publication_name}",
        f"--dblog.source.postgres.slot-name={slot_name}",
        "--dblog.source.postgres.retry-log-connection-loss=true",
        "--dblog.source.postgres.reconnect-backoff=PT2S",
        "--dblog.target.enabled=true",
        "--dblog.target.dialect=MYSQL",
        "--dblog.target.jdbc-url=jdbc:mysql://127.0.0.1:3306/demo",
        "--dblog.target.username=dblog",
        "--dblog.target.password=dblog",
        "--dblog.target.maximum-pool-size=4",
    ]

    lib.print_demo_header("PostgreSQL -> MySQL")
    with lib.demo_databases("postgres", "mysql"):
        lib.wait_for_demo_databases("postgres", "mysql")
        lib.reset_postgres_replication_resources(publication_name, slot_name)
        lib.ensure_postgres_dblog_runtime_privileges()
        lib.reset_postgres_demo_sample_orders()
        lib.ensure_postgres_demo_replica_identity_full()
        lib.reset_mysql_demo_sample_orders()

        # Seed PostgreSQL before DBLog starts so this demo also proves the
        # explicit dump bootstrap path before exercising live replication into MySQL.
        lib.postgres_exec(
            "INSERT INTO demo.sample_orders (id, customer_name, status) "
            "VALUES (1, 'seed-pg-one', 'PENDING');"
        )
        lib.postgres_exec(
            "INSERT INTO demo.sample_orders (id, customer_name, status) "
            "VALUES (2, 'seed-pg-two', 'READY');"
        )

        with lib.background_runtime(log_file, dblog_args) as runtime_proc:
            lib.wait_for_http_endpoint(
                f"{control_plane_url}/api/v1/runtime",
                timeout=120,
                runtime_proc=runtime_proc,
                log_path=log_file,
            )
            print("Runtime started.")
            print(f"Control plane reachable at {control_plane_url}.")

            # Use the control plane for the bootstrap request so the demo
            # exercises the same operator-facing path as the mysql->postgres demo.
            request_response = lib.submit_all_tables_request(control_plane_port)
            request_id = lib.extract_request_id(request_response)
            print(f"ALL_TABLES dump request submitted: {request_id}.")

            lib.wait_for_sql_value(
                lambda: lib.mysql_exec("USE demo; SELECT COUNT(*) FROM sample_orders"),
                expected="2",
                timeout=60,
            )
            print("Initial dump converged.")

            # Apply later live changes on PostgreSQL and wait for the MySQL target
            # to reach the matching final row state.
            lib.postgres_exec("DELETE FROM demo.sample_orders WHERE id=3;")
            lib.postgres_exec(
                "UPDATE demo.sample_orders SET status='SYNCED' WHERE id=1;"
            )
            lib.postgres_exec(
                "INSERT INTO demo.sample_orders (id, customer_name, status) "
                "VALUES (3, 'live-pg-three', 'NEW');"
            )
            lib.postgres_exec("DELETE FROM demo.sample_orders WHERE id=2;")

            lib.wait_for_sql_value(
                lambda: lib.mysql_exec(
                    "USE demo; SELECT status FROM sample_orders WHERE id = 1"
                ),
                expected="SYNCED",
                timeout=60,
            )
            lib.wait_for_sql_value(
                lambda: lib.mysql_exec(
                    "USE demo; SELECT COUNT(*) FROM sample_orders WHERE id = 3"
                ),
                expected="1",
                timeout=60,
            )
            lib.wait_for_sql_value(
                lambda: lib.mysql_exec(
                    "USE demo; SELECT COUNT(*) FROM sample_orders WHERE id = 2"
                ),
                expected="0",
                timeout=60,
            )
            print("Live changes converged.")

            target_rows = lib.mysql_exec(
                "USE demo; SELECT id, customer_name, status FROM sample_orders ORDER BY id;"
            )

            print("Demo succeeded.")
            lib.print_structured_summary(
                demo_name=DEMO_NAME,
                runtime_log=log_file,
                control_plane_url_=control_plane_url,
                request_id=request_id,
                section_name="target_rows",
                section_content=target_rows,
            )


if __name__ == "__main__":
    main()
