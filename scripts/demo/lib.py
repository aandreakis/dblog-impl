"""Helpers shared by the shipped DBLog demos.

Targets Linux, macOS, and Windows with Python 3.9+ and a Docker daemon.
Nothing here depends on a POSIX shell: ``subprocess`` invocations go
straight to the Docker CLI, port probing uses the ``socket`` stdlib,
and HTTP calls use ``urllib.request``. All paths are ``pathlib.Path``
so Windows path handling is correct.
"""

from __future__ import annotations

import atexit
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterable, Iterator, List, Optional, Sequence

# ---------------------------------------------------------------------------
# Repository layout
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent.parent
DOCKER_DIR = ROOT / "ops" / "docker"
DEMO_ROOT = ROOT / "build" / "demo"


# ---------------------------------------------------------------------------
# Compose project name (deterministic per-checkout)
# ---------------------------------------------------------------------------

_PROJECT_NAME_CACHE: Optional[str] = None
_DEFAULT_COMPOSE_PROJECT_NAME = DOCKER_DIR.name
_ALLOW_RUNNING_RUNTIME_ENV = "DBLOG_DEMO_ALLOW_RUNNING_RUNTIME"
_KEEP_CONTAINERS_ENV = "DBLOG_DEMO_KEEP_CONTAINERS"


def compose_project_name() -> str:
    """Deterministic ``docker compose -p`` name derived from the repo path."""
    global _PROJECT_NAME_CACHE
    override = os.environ.get("DBLOG_COMPOSE_PROJECT_NAME")
    if override:
        return override
    if _PROJECT_NAME_CACHE is not None:
        return _PROJECT_NAME_CACHE
    root = ROOT.resolve()
    slug = re.sub(r"[^a-z0-9]+", "-", root.name.lower()).strip("-") or "workspace"
    slug = slug[:30].strip("-") or "workspace"
    digest = hashlib.sha1(str(root).encode("utf-8")).hexdigest()[:8]
    _PROJECT_NAME_CACHE = f"dblog-{slug}-{digest}"
    return _PROJECT_NAME_CACHE


def _set_compose_project_name(project_name: str) -> None:
    global _PROJECT_NAME_CACHE
    _PROJECT_NAME_CACHE = project_name


# ---------------------------------------------------------------------------
# Demo workspace directories
# ---------------------------------------------------------------------------


def demo_root(name: str) -> Path:
    return DEMO_ROOT / name


def prepare_demo_dir(name: str) -> Path:
    path = demo_root(name)
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


# ---------------------------------------------------------------------------
# Docker Compose wrappers
# ---------------------------------------------------------------------------


def _compose_cmd_for_project(project_name: str, *args: str) -> List[str]:
    return ["docker", "compose", "-p", project_name, *args]


def _compose_cmd(*args: str) -> List[str]:
    return _compose_cmd_for_project(compose_project_name(), *args)


def docker_compose_run(
    *args: str, check: bool = True, quiet: bool = False
) -> subprocess.CompletedProcess:
    """Invoke ``docker compose`` with the demo project, cwd=ops/docker."""
    return _docker_compose_run_for_project(
        compose_project_name(), *args, check=check, quiet=quiet
    )


def _docker_compose_run_for_project(
    project_name: str, *args: str, check: bool = True, quiet: bool = False
) -> subprocess.CompletedProcess:
    """Invoke ``docker compose`` with an explicit project, cwd=ops/docker."""
    stdout = subprocess.DEVNULL if quiet else None
    stderr = subprocess.DEVNULL if quiet else None
    return subprocess.run(
        _compose_cmd_for_project(project_name, *args),
        cwd=str(DOCKER_DIR),
        check=check,
        stdout=stdout,
        stderr=stderr,
    )


def docker_compose_capture(*args: str) -> str:
    """Run ``docker compose`` and return stdout (text)."""
    result = subprocess.run(
        _compose_cmd(*args),
        cwd=str(DOCKER_DIR),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def _docker_compose_capture_for_project(project_name: str, *args: str) -> str:
    result = subprocess.run(
        _compose_cmd_for_project(project_name, *args),
        cwd=str(DOCKER_DIR),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout


_COMPOSE_ALIAS = {
    "dblog-mysql": "mysql",
    "mysql": "mysql",
    "dblog-postgres": "postgres",
    "postgres": "postgres",
    "dblog-runtime-example": "dblog",
    "dblog": "dblog",
}


def compose_service_name(svc: str) -> str:
    return _COMPOSE_ALIAS.get(svc, svc)


# ---------------------------------------------------------------------------
# Port probing
# ---------------------------------------------------------------------------


def port_is_available(port: int) -> bool:
    """Can we bind 127.0.0.1:<port> right now?"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def port_has_listener(port: int) -> bool:
    """Can we connect to 127.0.0.1:<port> right now?

    Differs from ``port_is_available`` (which tests bindability) because
    macOS lets a second process bind ``127.0.0.1:<port>`` when another
    process has already bound ``*:<port>`` / ``0.0.0.0:<port>`` (e.g.
    Docker port publishing). For demo pre-flight we want "is there
    already something a client could reach", which is a connect test.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.5)
    try:
        sock.connect(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def find_free_local_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    try:
        return sock.getsockname()[1]
    finally:
        sock.close()


def choose_control_plane_port(default_port: int) -> int:
    """Resolve the control-plane port with the same rules as the old bash:

    * ``DBLOG_CONTROL_PLANE_PORT`` pins the value; fail if it is already held.
    * Otherwise use ``default_port`` if free, else pick a random free port
      and inform the user.
    """
    override = os.environ.get("DBLOG_CONTROL_PLANE_PORT")
    requested = int(override) if override else default_port

    if port_is_available(requested):
        return requested

    if override:
        print(
            f"DBLOG_CONTROL_PLANE_PORT={requested} is already in use on 127.0.0.1. "
            "Choose a free port and retry.",
            file=sys.stderr,
        )
        sys.exit(1)

    fallback = find_free_local_port()
    print(
        f"Control-plane port {default_port} is already in use on 127.0.0.1; "
        f"using {fallback} instead. Set DBLOG_CONTROL_PLANE_PORT to pin a port.",
        file=sys.stderr,
    )
    return fallback


def control_plane_url(port: int) -> str:
    return f"http://127.0.0.1:{port}"


# ---------------------------------------------------------------------------
# Demo pre-flight: refuse to continue if host ports are held by other processes
# ---------------------------------------------------------------------------

_DEMO_PORT_BY_SERVICE = {"mysql": 3306, "postgres": 5432}


def preflight_demo_ports_or_die(services: Sequence[str]) -> None:
    """Fail fast if the canonical demo host ports are held by a foreign process.

    Without this check, ``docker compose up -d --no-recreate`` silently
    creates containers with no published ports (Docker accepts the compose
    spec but the bind fails at runtime), which leaves later demo steps
    with opaque "Connection refused" errors.
    """
    requested = list(services) if services else ["mysql", "postgres"]
    _adopt_default_compose_project_if_reusable(requested)
    _refuse_running_runtime_service_if_needed(compose_project_name())
    conflicts: List[str] = []

    for svc in requested:
        normalized = compose_service_name(svc)
        expected_port = _DEMO_PORT_BY_SERVICE.get(normalized)
        if expected_port is None:
            continue
        if not port_has_listener(expected_port):
            continue
        # Port has a listener. Held by *our* running compose service? If so,
        # ``up --no-recreate`` will reuse it, not a conflict.
        running = docker_compose_capture(
            "ps", "--quiet", "--status", "running", normalized
        )
        if any(line.strip() for line in running.splitlines()):
            continue
        conflicts.append(f"port {expected_port} (service {normalized})")

    if not conflicts:
        return

    print(
        "Demo pre-flight failed: the following host ports are already in use by a\n"
        "non-demo process and would cause 'docker compose up' to create broken\n"
        "containers with no port bindings:",
        file=sys.stderr,
    )
    for entry in conflicts:
        print(f"  - {entry}", file=sys.stderr)
    print(
        "\nFree the ports or stop the other holder and retry. On macOS/Linux:\n"
        "  lsof -nP -iTCP:3306 -sTCP:LISTEN\n"
        "  lsof -nP -iTCP:5432 -sTCP:LISTEN\n\n"
        "On Windows (PowerShell):\n"
        "  Get-NetTCPConnection -LocalPort 3306 -State Listen\n"
        "  Get-NetTCPConnection -LocalPort 5432 -State Listen\n\n"
        "If the holder is another docker compose stack, bring it down first:\n"
        "  docker compose -f ops/docker/compose.yml down",
        file=sys.stderr,
    )
    sys.exit(1)


def _adopt_default_compose_project_if_reusable(services: Sequence[str]) -> None:
    """Reuse the README/manual compose stack when it already owns demo ports."""
    if os.environ.get("DBLOG_COMPOSE_PROJECT_NAME"):
        return

    normalized_services = [compose_service_name(svc) for svc in services]
    expected_services = [
        svc for svc in normalized_services if svc in _DEMO_PORT_BY_SERVICE
    ]
    if not expected_services:
        return
    if compose_project_name() == _DEFAULT_COMPOSE_PROJECT_NAME:
        return

    current_running = _running_services(compose_project_name(), expected_services)
    if expected_services and set(expected_services).issubset(current_running):
        return

    default_running = _running_services(_DEFAULT_COMPOSE_PROJECT_NAME, expected_services)
    if not set(expected_services).issubset(default_running):
        return

    occupied_ports = [
        _DEMO_PORT_BY_SERVICE[svc]
        for svc in expected_services
        if port_has_listener(_DEMO_PORT_BY_SERVICE[svc])
    ]
    if not occupied_ports:
        return

    if _runtime_service_is_running(_DEFAULT_COMPOSE_PROJECT_NAME):
        _fail_running_runtime_reuse(_DEFAULT_COMPOSE_PROJECT_NAME)

    _set_compose_project_name(_DEFAULT_COMPOSE_PROJECT_NAME)
    print(
        "Reusing existing ops/docker compose project "
        f"'{_DEFAULT_COMPOSE_PROJECT_NAME}' for demo databases. "
        "Set DBLOG_COMPOSE_PROJECT_NAME to force an isolated demo stack.",
        file=sys.stderr,
    )


def _refuse_running_runtime_service_if_needed(project_name: str) -> None:
    if not _runtime_service_is_running(project_name):
        return
    _fail_running_runtime_reuse(project_name)


def _runtime_service_is_running(project_name: str) -> bool:
    return "dblog" in _running_services(project_name, ["dblog"])


def _allow_running_runtime_reuse() -> bool:
    raw = os.environ.get(_ALLOW_RUNNING_RUNTIME_ENV, "")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _fail_running_runtime_reuse(project_name: str) -> None:
    if _allow_running_runtime_reuse():
        return
    print(
        "Demo pre-flight failed: compose project "
        f"'{project_name}' already has the packaged DBLog runtime service running.\n"
        "The Python demos reset fixture tables before starting their own local runtime; "
        "doing that while the packaged runtime is attached to the same database can make "
        "the packaged runtime fail closed on unsupported DDL such as TRUNCATE.\n\n"
        "Stop the packaged runtime first, or use an isolated project with "
        "DBLOG_COMPOSE_PROJECT_NAME. If you intentionally want to reuse the stack anyway, "
        f"set {_ALLOW_RUNNING_RUNTIME_ENV}=1 and rerun the demo.",
        file=sys.stderr,
    )
    sys.exit(1)


def _running_services(project_name: str, services: Sequence[str]) -> set[str]:
    running: set[str] = set()
    for service in services:
        raw = _docker_compose_capture_for_project(
            project_name, "ps", "--quiet", "--status", "running", service
        )
        if any(line.strip() for line in raw.splitlines()):
            running.add(service)
    return running


# ---------------------------------------------------------------------------
# Compose lifecycle
# ---------------------------------------------------------------------------


def start_demo_databases(*services: str) -> None:
    preflight_demo_ports_or_die(services)
    docker_compose_run("up", "-d", "--no-recreate", "--remove-orphans", *services)


def stop_demo_databases(*services: str) -> None:
    if not services:
        docker_compose_run("down", "-v", check=False, quiet=True)
    else:
        docker_compose_run("rm", "-fsv", *services, check=False, quiet=True)


def stop_demo_project(project_name: str) -> None:
    _docker_compose_run_for_project(
        project_name, "down", "-v", "--remove-orphans", check=False, quiet=True
    )


def _truthy_env(name: str) -> bool:
    raw = os.environ.get(name, "")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _should_keep_demo_containers(project_name: str) -> bool:
    return (
        _truthy_env(_KEEP_CONTAINERS_ENV)
        or project_name == _DEFAULT_COMPOSE_PROJECT_NAME
    )


@contextmanager
def demo_databases(*services: str) -> Iterator[None]:
    """Context manager: start demo fixtures and clean up script-owned stacks."""
    try:
        start_demo_databases(*services)
    except subprocess.CalledProcessError:
        project_name = compose_project_name()
        if not _should_keep_demo_containers(project_name):
            stop_demo_project(project_name)
        raise
    project_name = compose_project_name()
    try:
        yield
    finally:
        if _should_keep_demo_containers(project_name):
            if _truthy_env(_KEEP_CONTAINERS_ENV):
                print(
                    "Leaving demo database containers running in compose project "
                    f"'{project_name}' because {_KEEP_CONTAINERS_ENV} is set.",
                    file=sys.stderr,
                )
            elif project_name == _DEFAULT_COMPOSE_PROJECT_NAME:
                print(
                    "Leaving existing ops/docker compose database stack running. "
                    "Stop it with: docker compose -f ops/docker/compose.yml down -v",
                    file=sys.stderr,
                )
        else:
            stop_demo_project(project_name)


def reset_demo_databases(*services: str) -> None:
    stop_demo_databases(*services)
    start_demo_databases(*services)
    wait_for_demo_databases(*services)


def wait_for_demo_databases(*services: str, timeout: int = 120) -> None:
    deadline = time.monotonic() + timeout
    last_unhealthy: List[str] = []

    while time.monotonic() < deadline:
        raw = docker_compose_capture("ps", "--format", "json", *services).strip()
        rows: List[dict] = []
        if raw:
            try:
                parsed = json.loads(raw)
                rows = parsed if isinstance(parsed, list) else [parsed]
            except json.JSONDecodeError:
                # Newer Docker Compose emits one JSON object per line.
                for line in raw.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    rows.append(json.loads(line))

        unhealthy = [
            row.get("Name", "")
            for row in rows
            if row.get("Health") and row.get("Health") != "healthy"
        ]
        last_unhealthy = [name for name in unhealthy if name]
        if not last_unhealthy:
            return
        time.sleep(2)

    print("Timed out waiting for demo databases to become healthy", file=sys.stderr)
    ps_args = ["ps", *services]
    subprocess.run(
        _compose_cmd(*ps_args), cwd=str(DOCKER_DIR), check=False, stdout=sys.stderr
    )
    for service in last_unhealthy:
        print(f"--- recent logs for {service} ---", file=sys.stderr)
        subprocess.run(
            _compose_cmd("logs", "--tail=60", service),
            cwd=str(DOCKER_DIR),
            check=False,
            stdout=sys.stderr,
        )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Runtime subprocess management
# ---------------------------------------------------------------------------


def _gradlew_path() -> Path:
    return ROOT / ("gradlew.bat" if os.name == "nt" else "gradlew")


def start_runtime_background(
    log_path: Path, bootrun_args: Sequence[str]
) -> subprocess.Popen:
    """Start DBLog in the background, redirecting stdout+stderr to ``log_path``.

    If ``DBLOG_DEMO_JAR`` is set, run the jar directly. Otherwise invoke
    ``./gradlew bootRun`` (``gradlew.bat`` on Windows).
    """
    log_fp = log_path.open("wb")
    jar = os.environ.get("DBLOG_DEMO_JAR")
    if jar:
        cmd: List[str] = ["java", "-jar", jar, *bootrun_args]
    else:
        args_string = " ".join(bootrun_args)
        cmd = [str(_gradlew_path()), "bootRun", f"--args={args_string}"]

    # ``close_fds=True`` is fine on POSIX; on Windows Popen handles inheritance
    # correctly for the redirected stdio without needing the flag.
    return subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=log_fp,
        stderr=subprocess.STDOUT,
        close_fds=(os.name != "nt"),
    )


def stop_demo_process(proc: Optional[subprocess.Popen]) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@contextmanager
def background_runtime(
    log_path: Path, bootrun_args: Sequence[str]
) -> Iterator[subprocess.Popen]:
    """Context manager: start runtime, yield Popen, guarantee cleanup.

    Also registers an ``atexit`` hook so a KeyboardInterrupt or
    os._exit slip-through still stops the subprocess.
    """
    proc = start_runtime_background(log_path, bootrun_args)
    atexit.register(stop_demo_process, proc)
    try:
        yield proc
    finally:
        stop_demo_process(proc)


# ---------------------------------------------------------------------------
# Polling helpers
# ---------------------------------------------------------------------------


def wait_for_log_line(path: Path, pattern: str, timeout: int = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            try:
                content = path.read_text(errors="replace")
            except OSError:
                content = ""
            if pattern in content:
                return
        time.sleep(1)
    print(f"Timed out waiting for log pattern: {pattern}", file=sys.stderr)
    if path.exists():
        tail = path.read_text(errors="replace").splitlines()[-80:]
        print("\n".join(tail), file=sys.stderr)
    sys.exit(1)


def wait_for_http_endpoint(
    url: str,
    timeout: int = 60,
    runtime_proc: Optional[subprocess.Popen] = None,
    log_path: Optional[Path] = None,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                resp.read()
                return
        except (urllib.error.URLError, urllib.error.HTTPError, OSError):
            pass
        if runtime_proc is not None and runtime_proc.poll() is not None:
            print(
                f"Runtime process exited before HTTP endpoint became reachable: {url}",
                file=sys.stderr,
            )
            _dump_log_tail(log_path)
            sys.exit(1)
        time.sleep(1)
    print(f"Timed out waiting for HTTP endpoint: {url}", file=sys.stderr)
    _dump_log_tail(log_path)
    sys.exit(1)


def wait_for_file_lines(path: Path, expected_count: int, timeout: int = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            with path.open("rb") as f:
                actual = sum(1 for _ in f)
            if actual >= expected_count:
                return
        time.sleep(1)
    print(f"Timed out waiting for {expected_count} lines in {path}", file=sys.stderr)
    if path.exists():
        print(path.read_text(errors="replace"), file=sys.stderr)
    sys.exit(1)


def wait_for_sql_value(
    query: Callable[[], str], expected: str, timeout: int = 60
) -> None:
    """Poll a SQL callable until it returns the expected value (whitespace-normalized)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            raw = query()
        except subprocess.CalledProcessError:
            raw = ""
        if re.sub(r"\s+", "", raw) == expected:
            return
        time.sleep(1)
    print(f"Timed out waiting for SQL value '{expected}'", file=sys.stderr)
    try:
        print(query(), file=sys.stderr)
    except Exception:  # noqa: BLE001  -- best-effort last-known value for the user
        pass
    sys.exit(1)


def _dump_log_tail(path: Optional[Path]) -> None:
    if path is None or not path.exists():
        return
    tail = path.read_text(errors="replace").splitlines()[-80:]
    print("\n".join(tail), file=sys.stderr)


# ---------------------------------------------------------------------------
# SQL execution against the compose fixtures
# ---------------------------------------------------------------------------


def _docker_exec_capture(service: str, inner_cmd: Iterable[str]) -> str:
    cmd = _compose_cmd("exec", "-T", service, *inner_cmd)
    result = subprocess.run(
        cmd,
        cwd=str(DOCKER_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, cmd, output=result.stdout, stderr=result.stderr
        )
    return result.stdout


def mysql_exec(sql: str) -> str:
    """Run MySQL SQL as the ``dblog`` user. Returns tabular stdout."""
    return _docker_exec_capture(
        "mysql", ["mysql", "-udblog", "-pdblog", "-N", "-e", sql]
    )


def postgres_exec(sql: str) -> str:
    """Run PostgreSQL SQL as the ``postgres`` superuser in the ``app`` database."""
    return _docker_exec_capture(
        "postgres", ["psql", "-U", "postgres", "-d", "app", "-At", "-c", sql]
    )


def reset_postgres_replication_resources(publication_name: str, slot_name: str) -> None:
    """Drop demo-owned logical replication resources before a fresh demo run."""
    publication = _postgres_identifier(publication_name)
    slot = _postgres_literal(slot_name)
    active_pid = postgres_exec(
        "SELECT active_pid FROM pg_catalog.pg_replication_slots "
        f"WHERE slot_name = {slot} AND active_pid IS NOT NULL;"
    ).strip()
    if active_pid:
        print(
            "Cannot reset PostgreSQL demo replication slot "
            f"{slot_name!r}; it is active in backend pid {active_pid}. "
            "Stop the previous demo/runtime process and retry.",
            file=sys.stderr,
        )
        sys.exit(1)

    postgres_exec(
        f"DROP PUBLICATION IF EXISTS {publication}; "
        "SELECT pg_catalog.pg_drop_replication_slot(slot_name) "
        f"FROM pg_catalog.pg_replication_slots WHERE slot_name = {slot};"
    )


def ensure_postgres_dblog_runtime_privileges() -> None:
    """Ensure reused demo stacks let the dblog role manage runtime objects."""
    postgres_exec(
        "CREATE SCHEMA IF NOT EXISTS dblog_meta AUTHORIZATION dblog; "
        "ALTER SCHEMA dblog_meta OWNER TO dblog; "
        "ALTER TABLE IF EXISTS demo.sample_orders OWNER TO dblog; "
        "ALTER TABLE IF EXISTS app.sample_orders OWNER TO dblog; "
        "ALTER TABLE IF EXISTS dblog_meta.watermarks OWNER TO dblog; "
        "ALTER TABLE IF EXISTS dblog_meta.heartbeats OWNER TO dblog; "
        "GRANT USAGE, CREATE ON SCHEMA demo TO dblog; "
        "GRANT USAGE, CREATE ON SCHEMA app TO dblog; "
        "GRANT USAGE, CREATE ON SCHEMA dblog_meta TO dblog; "
        "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA demo TO dblog; "
        "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA app TO dblog; "
        "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dblog_meta TO dblog; "
        "ALTER DEFAULT PRIVILEGES IN SCHEMA demo "
        "GRANT ALL PRIVILEGES ON TABLES TO dblog; "
        "ALTER DEFAULT PRIVILEGES IN SCHEMA app "
        "GRANT ALL PRIVILEGES ON TABLES TO dblog; "
        "ALTER DEFAULT PRIVILEGES IN SCHEMA dblog_meta "
        "GRANT ALL PRIVILEGES ON TABLES TO dblog;"
    )


def _postgres_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"unsupported PostgreSQL identifier: {value!r}")
    return value


def _postgres_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Fixture prep (same semantics as the bash reset_* helpers)
# ---------------------------------------------------------------------------


def reset_mysql_sample_orders() -> None:
    mysql_exec("USE app; TRUNCATE TABLE sample_orders;")


def reset_postgres_demo_sample_orders() -> None:
    postgres_exec("TRUNCATE TABLE demo.sample_orders;")


def reset_postgres_app_sample_orders() -> None:
    postgres_exec(
        "CREATE SCHEMA IF NOT EXISTS app; "
        "CREATE TABLE IF NOT EXISTS app.sample_orders ("
        "  id BIGINT PRIMARY KEY, customer_name TEXT NOT NULL, "
        "  status TEXT NOT NULL, updated_at TIMESTAMPTZ NULL); "
        "TRUNCATE TABLE app.sample_orders;"
    )


def reset_mysql_demo_sample_orders() -> None:
    # The JDBC target sink routes MySQL targets by tableId.schemaName, so
    # the postgres->mysql demo needs a real ``demo`` database on the MySQL side.
    _docker_exec_capture(
        "mysql",
        [
            "mysql",
            "-uroot",
            "-proot",
            "-N",
            "-e",
            "CREATE DATABASE IF NOT EXISTS demo; "
            "GRANT ALL PRIVILEGES ON demo.* TO 'dblog'@'%'; FLUSH PRIVILEGES; "
            "CREATE TABLE IF NOT EXISTS demo.sample_orders ("
            "  id BIGINT PRIMARY KEY, customer_name VARCHAR(255) NOT NULL, "
            "  status VARCHAR(64) NOT NULL, updated_at TIMESTAMP NULL); "
            "TRUNCATE TABLE demo.sample_orders;",
        ],
    )


def ensure_postgres_demo_replica_identity_full() -> None:
    postgres_exec("ALTER TABLE demo.sample_orders REPLICA IDENTITY FULL;")


# ---------------------------------------------------------------------------
# Control plane client
# ---------------------------------------------------------------------------


def submit_all_tables_request(port: int, timeout: int = 30) -> str:
    """POST an ALL_TABLES request to the control plane, retrying until accepted."""
    url = f"http://127.0.0.1:{port}/api/v1/requests"
    payload = json.dumps({"scope": "ALL_TABLES"}).encode("utf-8")
    deadline = time.monotonic() + timeout
    last_err: Optional[BaseException] = None
    while time.monotonic() < deadline:
        req = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                text = resp.read().decode("utf-8", errors="replace")
                if "requestId" in text:
                    return text
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as err:
            last_err = err
        time.sleep(1)
    print(f"Timed out submitting ALL_TABLES request to {url}", file=sys.stderr)
    if last_err is not None:
        print(f"  last error: {last_err}", file=sys.stderr)
    sys.exit(1)


def extract_request_id(response: str) -> str:
    # The POST /api/v1/requests response shape is
    # ``{"accepted": true, "request": {"requestId": "...", ...}}``, so the id
    # is nested one level below the document root. Fall back to a regex scan
    # in case the server ever surfaces the id at the top level or inside a
    # different wrapper, matching what the previous ``sed``-based extractor did.
    try:
        data = json.loads(response)
    except json.JSONDecodeError:
        data = None
    if isinstance(data, dict):
        nested = data.get("request")
        if isinstance(nested, dict) and nested.get("requestId"):
            return str(nested["requestId"])
        if data.get("requestId"):
            return str(data["requestId"])
    match = re.search(r'"requestId"\s*:\s*"([^"]+)"', response)
    return match.group(1) if match else ""


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_demo_header(title: str) -> None:
    print()
    print(f"== {title} ==")


def print_structured_summary(
    demo_name: str,
    runtime_log: Path,
    control_plane_url_: Optional[str] = None,
    request_id: Optional[str] = None,
    artifact_name: Optional[str] = None,
    artifact_path: Optional[Path] = None,
    section_name: Optional[str] = None,
    section_content: Optional[str] = None,
) -> None:
    print()
    print("== Summary ==")
    print("summary_begin")
    print(f"demo={demo_name}")
    print("result=success")
    if control_plane_url_:
        print(f"control_plane_url={control_plane_url_}")
    if request_id:
        print(f"request_id={request_id}")
    print(f"runtime_log={runtime_log}")
    if artifact_name and artifact_path:
        print(f"{artifact_name}={artifact_path}")
    if section_name:
        print(f"{section_name}_begin")
        if section_content:
            print(section_content)
        print(f"{section_name}_end")
    print("summary_end")
