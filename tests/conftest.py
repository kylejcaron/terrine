from functools import wraps

import pytest

from tests import _worlds
from tests.fixtures.readme import readme_tables


@pytest.fixture(scope="session", autouse=True)
def worker_duckdb_resources(request):
    if not hasattr(request.config, "workerinput"):
        yield
        return

    try:
        import duckdb
    except ModuleNotFoundError as error:
        if error.name != "duckdb":
            raise
        yield
        return

    original = duckdb.connect

    @wraps(original)
    def connect(*args, **kwargs):
        settings = dict(kwargs.pop("config", None) or {})
        settings.setdefault("threads", 2)
        return original(*args, config=settings, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(duckdb, "connect", connect)
        yield


@pytest.fixture(scope="session")
def events_world():
    return _worlds.events_world()


@pytest.fixture(scope="session")
def winsorization_world():
    return _worlds.winsorization_world()


@pytest.fixture(scope="session")
def interop_world():
    return _worlds.interop_world()


@pytest.fixture
def native_inputs(events_world):
    import ibis

    con = ibis.duckdb.connect()
    try:
        for name, table in events_world.tables.items():
            con.create_table(name, table)
        yield events_world.catalog, con
    finally:
        con.disconnect()


@pytest.fixture
def readme_con():
    import ibis

    con = ibis.duckdb.connect()
    try:
        for name, table in readme_tables().items():
            con.create_table(name, table)
        yield con
    finally:
        con.disconnect()
