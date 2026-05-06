#!/usr/bin/env python3
"""Demo: MySQL -> DBLog -> PostgreSQL.

Port of the previous ``mysql_to_postgres.sh``. Runs on Linux, macOS, and
Windows with a Docker daemon and Python 3.9+.

Invoke from the repository root:

    python scripts/demo/mysql_to_postgres.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import lib  # noqa: E402


DEMO_NAME = "mysql_to_postgres"


def main() -> None:
    demo_dir = lib.prepare_demo_dir(DEMO_NAME)
    log_file = demo_dir / "runtime.log"
    state_path = demo_dir / "state"
    control_plane_port = lib.choose_control_plane_port(18086)
    control_plane_url = lib.control_plane_url(control_plane_port)
    source_id = "mysql_to_postgres_demo"
    source_table = "app.sample_orders"

    # Example deployment shape for this demo:
    # - boot mode: runtime
    # - source: MySQL app.sample_orders
    # - target: PostgreSQL app.sample_orders
    # - control plane enabled for request submission and status inspection
    dblog_args = [
        "--dblog.boot-mode=runtime",
        f"--dblog.runtime.state-path={state_path}",
        "--dblog.control-plane.enabled=true",
        f"--dblog.control-plane.port={control_plane_port}",
        "--dblog.source.adapter=mysql",
        f"--dblog.source.id={source_id}",
        f"--dblog.source.tables[0]={source_table}",
        "--dblog.source.mysql.jdbc-url=jdbc:mysql://127.0.0.1:3306/app",
        "--dblog.source.mysql.username=dblog",
        "--dblog.source.mysql.password=dblog",
        "--dblog.source.mysql.hostname=127.0.0.1",
        "--dblog.source.mysql.port=3306",
        "--dblog.source.mysql.server-id=223402",
        "--dblog.source.mysql.retry-log-connection-loss=true",
        "--dblog.source.mysql.reconnect-backoff=PT2S",
        "--dblog.target.enabled=true",
        "--dblog.target.dialect=POSTGRES",
        "--dblog.target.jdbc-url=jdbc:postgresql://127.0.0.1:5432/app",
        "--dblog.target.username=postgres",
        "--dblog.target.password=postgres",
        "--dblog.target.maximum-pool-size=4",
    ]

    lib.print_demo_header("MySQL -> PostgreSQL")
    with lib.demo_databases("mysql", "postgres"):
        lib.wait_for_demo_databases("mysql", "postgres")
        lib.reset_mysql_sample_orders()
        lib.reset_postgres_app_sample_orders()

        # Seed source rows before startup so the demo can prove the explicit dump
        # path first and then prove later live-stream convergence separately.
        lib.mysql_exec(
            "USE app; INSERT INTO sample_orders (id, customer_name, status) "
            "VALUES (1, 'seed-one', 'PENDING');"
        )
        lib.mysql_exec(
            "USE app; INSERT INTO sample_orders (id, customer_name, status) "
            "VALUES (2, 'seed-two', 'READY');"
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

            # Submit the dump over the real local HTTP control plane so the demo
            # mirrors the intended operator path rather than a hidden direct
            # state-store write.
            request_response = lib.submit_all_tables_request(control_plane_port)
            request_id = lib.extract_request_id(request_response)
            print(f"ALL_TABLES dump request submitted: {request_id}.")

            lib.wait_for_sql_value(
                lambda: lib.postgres_exec("SELECT COUNT(*) FROM app.sample_orders"),
                expected="2",
                timeout=60,
            )
            print("Initial dump converged.")

            # After bootstrap succeeds, mutate MySQL live and wait for the
            # Postgres target to converge to the new row state.
            lib.mysql_exec("USE app; DELETE FROM sample_orders WHERE id=3;")
            lib.mysql_exec(
                "USE app; UPDATE sample_orders SET status='SYNCED' WHERE id=1;"
            )
            lib.mysql_exec(
                "USE app; INSERT INTO sample_orders (id, customer_name, status) "
                "VALUES (3, 'live-three', 'NEW');"
            )
            lib.mysql_exec("USE app; DELETE FROM sample_orders WHERE id=2;")

            lib.wait_for_sql_value(
                lambda: lib.postgres_exec(
                    "SELECT status FROM app.sample_orders WHERE id = 1"
                ),
                expected="SYNCED",
                timeout=60,
            )
            lib.wait_for_sql_value(
                lambda: lib.postgres_exec(
                    "SELECT COUNT(*) FROM app.sample_orders WHERE id = 3"
                ),
                expected="1",
                timeout=60,
            )
            lib.wait_for_sql_value(
                lambda: lib.postgres_exec(
                    "SELECT COUNT(*) FROM app.sample_orders WHERE id = 2"
                ),
                expected="0",
                timeout=60,
            )
            print("Live changes converged.")

            target_rows = lib.postgres_exec(
                "SELECT id, customer_name, status FROM app.sample_orders ORDER BY id;"
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
