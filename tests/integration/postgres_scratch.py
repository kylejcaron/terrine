"""Start or stop a throwaway local PostgreSQL server for the live probes.

Usage:
    eval "$(uv run python -m tests.integration.postgres_scratch start)"
    make test-postgres
    uv run python -m tests.integration.postgres_scratch stop

The server uses trust authentication on 127.0.0.1 only, keeps its data in a
temporary directory and is removed by ``stop``.
"""

from __future__ import annotations

import argparse
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path

_STATE = Path(tempfile.gettempdir()) / "terrine-postgres-scratch"
_DATABASE = "terrine_test"


def _binary(name: str) -> str:
    found = shutil.which(name)
    if found is None:
        config = shutil.which("pg_config")
        if config is not None:
            bindir = subprocess.run(
                [config, "--bindir"], check=True, capture_output=True, text=True
            ).stdout.strip()
            candidate = Path(bindir) / name
            if candidate.exists():
                return str(candidate)
        raise SystemExit(f"{name} not found; install PostgreSQL 16 (brew install postgresql@16)")
    return found


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _exports(port: int) -> str:
    settings = {
        "TERRINE_TEST_POSTGRES_HOST": "127.0.0.1",
        "TERRINE_TEST_POSTGRES_PORT": str(port),
        "TERRINE_TEST_POSTGRES_USER": "postgres",
        "TERRINE_TEST_POSTGRES_PASSWORD": "",
        "TERRINE_TEST_POSTGRES_DB": _DATABASE,
    }
    return "\n".join(f"export {key}={value!r}" for key, value in settings.items())


def start() -> None:
    data = _STATE / "data"
    port_file = _STATE / "port"
    if data.exists():
        print(_exports(int(port_file.read_text())))
        return
    _STATE.mkdir(parents=True, exist_ok=True)
    port = _free_port()
    subprocess.run(
        [_binary("initdb"), "-D", str(data), "-U", "postgres", "--auth=trust", "-E", "UTF8"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            _binary("pg_ctl"),
            "-D",
            str(data),
            "-o",
            f"-p {port} -h 127.0.0.1 -k {_STATE}",
            "-l",
            str(_STATE / "server.log"),
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [_binary("createdb"), "-h", "127.0.0.1", "-p", str(port), "-U", "postgres", _DATABASE],
        check=True,
        capture_output=True,
    )
    port_file.write_text(str(port))
    print(_exports(port))


def stop() -> None:
    data = _STATE / "data"
    if data.exists():
        subprocess.run(
            [_binary("pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"],
            check=False,
            capture_output=True,
        )
    shutil.rmtree(_STATE, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop"))
    {"start": start, "stop": stop}[parser.parse_args().action]()


if __name__ == "__main__":
    main()
