import pytest

from terrine.data import Coverage
from terrine.model import Model
from terrine.spec import Design, EventAnchor, agg

pytest_plugins = ("tests.conftest",)


@pytest.fixture
def native_inputs_fixture(native_inputs):
    yield native_inputs


def _design(**kwargs):
    return Design("user", EventAnchor("session"), **kwargs)


def _model(native_inputs_fixture):
    catalog, con = native_inputs_fixture
    return Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-01-31")}
    )


def test_many_window_columns_compile_and_export_together(native_inputs_fixture):
    outcomes = {
        f"sessions_{days}d": agg("sessions", window=("0d", f"{days}d")) for days in range(1, 7)
    }
    result = _design(outcomes=outcomes).build(_model(native_inputs_fixture)).materialize()
    rows = result.table.sort_by("unit").to_pylist()
    assert [row["unit"] for row in rows] == ["a", "b"]
    assert all(row[name] == 1 for row in rows for name in outcomes)
    assert all(row[result.columns[name].complete_column] for row in rows for name in outcomes)
