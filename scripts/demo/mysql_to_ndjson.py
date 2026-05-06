#!/usr/bin/env python3
"""Demo: MySQL -> DBLog -> NDJSON file.

Port of the previous ``mysql_to_ndjson.sh``. Runs on Linux, macOS, and
Windows with a Docker daemon and Python 3.9+.

Invoke from the repository root:

    python scripts/demo/mysql_to_ndjson.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import lib  # noqa: E402  (path hack above)


DEMO_NAME = "mysql_to_ndjson"


def main() -> None:
    demo_dir = lib.prepare_demo_dir(DEMO_NAME)
    log_file = demo_dir / "runtime.log"
    event_file = demo_dir / "events.ndjson"
    state_path = demo_dir / "state"
    control_plane_port = lib.choose_control_plane_port(18085)
    control_plane_url = lib.control_plane_url(control_plane_port)
    source_id = "mysql_ndjson_demo"
    source_table = "app.sample_orders"

    # Example deployment shape for this demo:
    # - boot mode: runtime
    # - source: MySQL app.sample_orders
    # - sink: NDJSON file
    dblog_args = [
        "--dblog.boot-mode=runtime",
        f"--dblog.runtime.state-path={state_path}",
        f"--dblog.sink.ndjson.path={event_file}",
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
        "--dblog.source.mysql.server-id=223401",
        "--dblog.source.mysql.retry-log-connection-loss=true",
        "--dblog.source.mysql.reconnect-backoff=PT2S",
    ]

    lib.print_demo_header("MySQL -> NDJSON")
    with lib.demo_databases("mysql"):
        lib.wait_for_demo_databases("mysql")
        lib.reset_mysql_sample_orders()

        # Seed one row before startup so the demo shows that runtime mode begins
        # streaming from the current source position instead of auto-dumping history.
        lib.mysql_exec(
            "USE app; INSERT INTO sample_orders (id, customer_name, status) "
            "VALUES (1, 'seed-ndjson', 'PENDING');"
        )

        with lib.background_runtime(log_file, dblog_args) as runtime_proc:
            lib.wait_for_http_endpoint(
                f"{control_plane_url}/api/v1/runtime",
                timeout=120,
                runtime_proc=runtime_proc,
                log_path=log_file,
            )
            print(f"Control plane reachable at {control_plane_url}.")

            # Drive three live mutations after the runtime is already up so the
            # file shows the neutral DBLog event model directly.
            lib.mysql_exec("USE app; DELETE FROM sample_orders WHERE id=2;")
            lib.mysql_exec(
                "USE app; INSERT INTO sample_orders (id, customer_name, status) "
                "VALUES (2, 'alice-demo', 'NEW');"
            )
            lib.mysql_exec(
                "USE app; UPDATE sample_orders SET status='SHIPPED' WHERE id=2;"
            )
            lib.mysql_exec("USE app; DELETE FROM sample_orders WHERE id=2;")

            lib.wait_for_file_lines(event_file, expected_count=3)

            recent_events = "\n".join(event_file.read_text().splitlines()[-5:])

            print("Demo succeeded.")
            lib.print_structured_summary(
                demo_name=DEMO_NAME,
                runtime_log=log_file,
                control_plane_url_=control_plane_url,
                artifact_name="event_file",
                artifact_path=event_file,
                section_name="recent_events",
                section_content=recent_events,
            )


if __name__ == "__main__":
    main()
