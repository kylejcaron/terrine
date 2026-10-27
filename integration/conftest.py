import pytest

from tests.conftest import worker_duckdb_resources as worker_duckdb_resources  # noqa: F401


def pytest_addoption(parser):
    parser.addoption(
        "--generic-dialect",
        action="store_true",
        default=False,
        help="Compile every engine's study expressions with the portable base dialect.",
    )


def force_generic_dialect(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve every registered policy name to the portable base dialect.

    Only central expression selection changes: connections, providers, SQL
    dialects and identity admission stay native. The patch is undone with the
    monkeypatch, restoring the cached registered dialects.
    """
    from terrine.backends import dialect as policies

    monkeypatch.setattr(policies, "_registered_dialect", lambda name: policies.Dialect("generic"))
