from datetime import UTC, datetime

import ibis
import pyarrow as pa
import pytest

from terrine.data import Budget, Coverage, EligibilitySummary, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import (
    CalendarComparisonSpec,
    CohortSpec,
    Design,
    EventAnchor,
    FixedDate,
    Initiation,
    Schedule,
    agg,
    gt,
)
from terrine.spec.trajectory import Trajectory
from tests.compile.test_ltv import _trajectory_model
from tests.conftest import events_world as events_world  # noqa: F401
from tests.conftest import native_inputs as native_inputs  # noqa: F401


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


# --- the first refusal follows the check schedule ---------------------------------

_ORDERS_CATALOG = {
    "schema_version": 2,
    "day_boundary": "UTC",
    "dialect": "duckdb",
    "sources": {
        "orders": {
            "relation": {"kind": "table", "name": "orders"},
            "time": "ts",
            "units": {"user": "user_id"},
            "events": ["amount", "qty"],
            "properties": ["channel"],
            "dimensions": [],
        }
    },
    "events": {
        "amount": {"source": "orders", "column": "amount"},
        "qty": {"source": "orders", "column": "qty"},
    },
    "properties": {
        "channel": {
            "source": "orders",
            "column": "channel",
            "dtype": "string",
            "as_of": "event_time",
        }
    },
    "dimensions": {},
    "measures": {
        "amount_sum": {"event": "amount", "aggregation": "sum"},
        "web_quantities": {
            "event": "qty",
            "aggregation": "count_distinct",
            "filters": [{"property": "channel", "op": "equals", "values": ["web"]}],
        },
    },
    "metrics": {
        "spend": {"kind": "mean", "unit": "user", "measure": "amount_sum"},
        "web_distinct": {"kind": "mean", "unit": "user", "measure": "web_quantities"},
    },
}
_ORDER_SCHEMA = pa.schema(
    [
        ("user_id", pa.string()),
        ("ts", pa.timestamp("us", tz="UTC")),
        ("amount", pa.float64()),
        ("qty", pa.int64()),
        ("channel", pa.string()),
    ]
)


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


def _orders_model(con, orders, users=("a", "b")):
    con.create_table("users", pa.table({"user_id": list(users)}))
    con.create_table("orders", pa.Table.from_pylist(orders, schema=_ORDER_SCHEMA))
    return Model.from_catalog(
        Catalog.model_validate(_ORDERS_CATALOG),
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={"orders": Coverage("2024-12-01", "2025-02-01")},
    )


def _order(user, when, *, amount, qty=1, channel="web"):
    return {"user_id": user, "ts": when, "amount": amount, "qty": qty, "channel": channel}


def _refusal(design, model):
    with pytest.raises(TerrineError) as error:
        design.build(model).materialize()
    return error.value


@pytest.mark.parametrize("order", ["metric_first", "nonadditive_first"])
def test_first_refusal_follows_registration_order_across_check_kinds(con, order):
    """An additive sum over a NaN amount and a filtered distinct count whose filter
    is unknown for a matched event both fail; the output registered first refuses.
    Outputs register in name order, so the names fix the schedule."""
    model = _orders_model(
        con,
        [_order("a", datetime(2025, 1, 1, 12, tzinfo=UTC), amount=float("nan"), channel=None)],
    )
    spend = agg("spend", window=("-1d", "0s"))
    distinct = agg("web_distinct", window=("-1d", "0s"))
    features = (
        {"a_spend": spend, "b_distinct": distinct}
        if order == "metric_first"
        else {"a_distinct": distinct, "b_spend": spend}
    )
    error = _refusal(Design("user", FixedDate("2025-01-02"), features=features), model)
    assert error.code == (
        "terrine.temporal.float64_precision"
        if order == "metric_first"
        else "terrine.filter.unknown"
    )
    assert error.context["failed_count"] == 1


def test_metric_refusal_precedes_shape_refusal():
    """A feature window holding a NaN and a cumulative trajectory value that
    overflows its exact type both fail; the feature's check, registered before
    the shape's, refuses."""
    con, model = _trajectory_model(decimal_type="DECIMAL(38, 0)")
    huge = "60000000000000000000000000000000000000"
    try:
        con.raw_sql(
            f"UPDATE events SET decimal_amount = {huge} WHERE user_id = 'u1' AND kind = 'revenue'"
        )
        con.raw_sql(f"""
            INSERT INTO events (user_id, ts, kind, amount, decimal_amount, float_amount)
            VALUES ('u1', TIMESTAMPTZ '2025-01-17 00:00:00+00', 'revenue', 0, {huge}, 0)
        """)
        con.raw_sql("""
            UPDATE events SET float_amount = 'NaN'::DOUBLE
            WHERE user_id = 'u1' AND ts = TIMESTAMPTZ '2025-01-03 00:00:00+00'
        """)
        design = Design(
            "user",
            EventAnchor("purchase", which="first", offset="7d"),
            features={"recent": agg("float_revenue", window=("-7d", "0s"))},
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("decimal_revenue", "7d", periods=2, cumulative=True),
            ),
        )
        error = _refusal(design, model)
        assert error.code == "terrine.temporal.float64_precision"
        assert error.context["failure"] == "nonfinite_input"
        con.raw_sql("UPDATE events SET float_amount = 0 WHERE user_id = 'u1'")
        error = _refusal(design, model)
        assert error.code == "terrine.temporal.exact_overflow"
        assert error.context["output"] == "cumulative_value"
    finally:
        con.disconnect()


def test_row_checks_count_base_candidates_not_shape_rows(con):
    """A calendar comparison keeps only the stacked periods around each cohort;
    a NaN in the feature window of a candidate outside every stack still refuses."""
    model = _orders_model(
        con,
        [
            _order("a", datetime(2025, 1, 3, 6, tzinfo=UTC), amount=1.0),
            _order("b", datetime(2024, 12, 31, 12, tzinfo=UTC), amount=float("nan")),
        ],
    )
    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-05"),
        features={"spend": agg("spend", window=("-1d", "0s"))},
        shape=CalendarComparisonSpec(
            Initiation("orders", "amount", datetime(2025, 1, 1, tzinfo=UTC)),
            "never_treated",
            -1,
            (0,),
        ),
    )
    frame = design.build(model)
    stacked = con.to_pyarrow(frame._compiled.audit_table.select("unit", "t0")).to_pylist()
    assert {row["t0"] for row in stacked} == {
        datetime(2025, 1, 3, tzinfo=UTC),
        datetime(2025, 1, 4, tzinfo=UTC),
    }
    with pytest.raises(TerrineError) as error:
        frame.materialize()
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == "nonfinite_input"
    assert error.value.context["failed_count"] == 1


# --- the output statement carries the final run and the eligibility summary ------

_NOON = datetime(2025, 1, 1, 12, tzinfo=UTC)
_SPEND = {"spend": agg("spend", window=("-1d", "0s"))}


def _recording(monkeypatch):
    """Record the SQL of every statement a materialization executes or streams."""
    from terrine.execution.inputs import ExecutionContext

    executed, streamed = [], []
    execute, to_arrow_batches = ExecutionContext.execute, ExecutionContext.to_arrow_batches

    def recording_execute(self, query):
        executed.append(ibis.to_sql(query, dialect="duckdb"))
        return execute(self, query)

    def recording_batches(self, query, **kwargs):
        streamed.append(ibis.to_sql(query, dialect="duckdb"))
        return to_arrow_batches(self, query, **kwargs)

    monkeypatch.setattr(ExecutionContext, "execute", recording_execute)
    monkeypatch.setattr(ExecutionContext, "to_arrow_batches", recording_batches)
    return executed, streamed


def test_materialize_issues_one_output_statement(con, monkeypatch):
    """With no check that must run in place, the anchor checks are the only
    separate statements: every window check and the eligibility summary ride
    on the one output statement, whose gate columns never reach the result."""
    model = _orders_model(con, [_order("a", _NOON, amount=1.0), _order("b", _NOON, amount=2.0)])
    executed, streamed = _recording(monkeypatch)
    result = Design("user", FixedDate("2025-01-02"), features=_SPEND).build(model).materialize()
    assert result.table.num_rows == 2
    assert not any(name.startswith("__terrine_gate_") for name in result.table.column_names)
    assert len(streamed) == 1
    assert "__terrine_gate_check_" in streamed[0] and "__terrine_gate_retained" in streamed[0]
    assert executed and all("invalid_count" in sql for sql in executed)
    assert not any("__terrine_gate_" in sql for sql in executed)


def _excluded_everyone():
    return Design(
        "user",
        FixedDate("2025-01-02"),
        features=_SPEND,
        eligibility={"big": gt(agg("web_distinct", window=("-1d", "0s")), 5)},
    )


def test_materialize_gates_an_empty_output(con):
    """Eligibility excludes every candidate, so the output has no row to carry
    the gate; the NaN in an excluded candidate's window still refuses."""
    model = _orders_model(
        con,
        [_order("a", _NOON, amount=float("nan")), _order("b", _NOON, amount=2.0)],
        users=("a", "b", "c"),
    )
    error = _refusal(_excluded_everyone(), model)
    assert error.code == "terrine.temporal.float64_precision"
    assert error.context["failure"] == "nonfinite_input"
    assert error.context["failed_count"] == 1


def test_materialize_reports_the_summary_when_no_rows_are_retained(con):
    model = _orders_model(
        con, [_order("a", _NOON, amount=1.0), _order("b", _NOON, amount=2.0)], users=("a", "b", "c")
    )
    result = _excluded_everyone().build(model).materialize()
    assert result.table.num_rows == 0
    assert result.provenance.eligibility_summary == EligibilitySummary(
        candidate=3, excluded_false=3, unknown=0, retained=0
    )


def test_row_check_refusal_precedes_budget_refusal(con):
    model = _orders_model(
        con,
        [
            _order("a", _NOON, amount=float("nan")),
            _order("b", _NOON, amount=2.0),
            _order("c", _NOON, amount=3.0),
        ],
        users=("a", "b", "c"),
    )
    frame = Design("user", FixedDate("2025-01-02"), features=_SPEND).build(model)
    with pytest.raises(TerrineError) as error:
        frame.materialize(Budget(max_rows=1, max_output_bytes=10**6))
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failed_count"] == 1


def test_budgets_count_only_output_rows_and_bytes(con):
    """Row and byte budgets bound the retained output: excluded candidates and
    the gate columns the statement carries count for neither."""
    users = tuple("abcdefg")
    model = _orders_model(
        con, [_order(user, _NOON, amount=float(i)) for i, user in enumerate(users)], users=users
    )
    design = Design(
        "user",
        FixedDate("2025-01-02"),
        features=_SPEND,
        eligibility={"big": gt(agg("spend", window=("-1d", "0s")), 4.5)},
    )
    reference = design.build(model).materialize()
    assert reference.provenance.eligibility_summary == EligibilitySummary(
        candidate=7, excluded_false=5, unknown=0, retained=2
    )
    bounded = design.build(model).materialize(
        Budget(max_rows=2, max_output_bytes=reference.table.nbytes)
    )
    assert bounded.table.sort_by("unit").equals(reference.table.sort_by("unit"))
    with pytest.raises(TerrineError) as error:
        design.build(model).materialize(Budget(max_rows=1, max_output_bytes=10**6))
    assert error.value.code == "terrine.budget.rows_exceeded"
