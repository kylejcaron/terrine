from datetime import UTC, datetime

import ibis
import pyarrow as pa
import pytest

from terrine.data import Budget, Coverage, EligibilitySummary, Events, UnitSource
from terrine.errors import TerrineError
from terrine.metrics import count
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import (
    AnchorRange,
    CalendarComparisonSpec,
    CohortSpec,
    Design,
    EventAnchor,
    FixedDate,
    Initiation,
    Schedule,
    agg,
    asof,
    eq,
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
    "schema_version": 3,
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


@pytest.mark.parametrize(
    ("anchor", "unit", "spend"),
    [
        (EventAnchor("amount", "first"), "b", 5.0),
        (EventAnchor("amount", "nth", n=3), "a", 4.0),
    ],
)
def test_ranked_anchor_preserves_history_with_a_window_on_the_same_facts(con, anchor, unit, spend):
    february = datetime(2025, 2, 1, tzinfo=UTC)
    model = _orders_model(
        con,
        [
            _order("a", datetime(2025, 1, 10, tzinfo=UTC), amount=2.0),
            _order("a", datetime(2025, 1, 20, tzinfo=UTC), amount=3.0),
            _order("a", february, amount=4.0),
            _order("b", february, amount=5.0),
        ],
    )
    design = Design(
        "user",
        anchor,
        anchor_range=AnchorRange(start="2025-02-01"),
        outcomes={"spend": agg("spend", window=("0s", "1h"))},
    )
    rows = design.build(model).materialize().table.to_pylist()
    assert [(row["unit"], row["t0"], row["spend"]) for row in rows] == [(unit, february, spend)]


def test_materialize_checks_out_of_window_filter_membership_with_no_anchors(con):
    con.raw_sql("""
        CREATE TABLE users AS SELECT 'a' AS user_id;
        CREATE TABLE orders AS
        SELECT 'a' AS user_id, TIMESTAMPTZ '2025-01-01' AS ts, NULL::DOUBLE AS score;
    """)
    model = Model.from_tables(
        con,
        units={"user": UnitSource("users", "user_id")},
        events={
            "purchase": Events(
                "orders",
                time="ts",
                key="user_id",
                where={"score__gt": 0},
                complete=("2025-01-01", "2025-01-31"),
                metrics={"orders": count()},
            )
        },
    )
    design = Design(
        "user",
        FixedDate("2025-01-03"),
        anchor_range=AnchorRange(start="2025-01-05"),
        features={"orders": agg("orders", window=("-1h", "0s"))},
    )
    error = _refusal(design, model)
    assert error.code == "terrine.compile.anchor_integrity"
    assert error.context["check_key"] == "unknown_event_filter_value"


@pytest.mark.parametrize("order", ["metric_first", "nonadditive_first"])
def test_first_refusal_follows_registration_order_across_check_kinds(con, order):
    """An unrelated valid additive output must not reorder interleaved refusals."""
    model = _orders_model(
        con,
        [_order("a", datetime(2025, 1, 1, 12, tzinfo=UTC), amount=float("nan"), channel=None)],
    )
    spend = agg("spend", window=("-1d", "0s"))
    distinct = agg("web_distinct", window=("-1d", "0s"))
    features = (
        {"b_spend": spend, "c_spend_alias": spend, "d_distinct": distinct}
        if order == "metric_first"
        else {"b_distinct": distinct, "c_spend": spend, "d_spend_alias": spend}
    )
    features = {
        "a_unavailable": agg("spend", window=("-40d", "0s")),
        "a_empty": agg("spend", window=("-2d", "-1d")),
        **features,
    }
    error = _refusal(Design("user", FixedDate("2025-01-02"), features=features), model)
    assert error.code == (
        "terrine.temporal.float64_precision"
        if order == "metric_first"
        else "terrine.filter.unknown"
    )
    assert error.context["failed_count"] == 1
    assert error.context["source"] == "orders"
    assert error.context["measure"] == ("amount" if order == "metric_first" else "qty")


@pytest.mark.parametrize(
    ("history_first", "conversion", "as_of"),
    [
        pytest.param(False, False, False, id="earlier_numeric"),
        pytest.param(True, False, False, id="own_history_before_numeric"),
        pytest.param(True, True, False, id="conversion_without_row_checks"),
        pytest.param(False, False, True, id="later_asof_can_multiply_candidates"),
    ],
)
def test_source_history_refusal_follows_first_consuming_output(
    con, history_first, conversion, as_of
):
    model = _orders_model(
        con, [_order("a", datetime(2025, 1, 1, 12, tzinfo=UTC), amount=float("nan"))]
    )
    con.create_table("other_orders", con.table("orders"))
    con.create_table(
        "history",
        pa.table(
            {
                "user_id": ["a", "a"],
                "starts": pa.array(
                    [datetime(2024, 12, 1, tzinfo=UTC)] * 2,
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "ends": pa.array(
                    [datetime(2025, 2, 1, tzinfo=UTC)] * 2,
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "tier": ["gold", "silver"],
            }
        ),
    )
    definition = model.catalog.model_dump(mode="python")
    definition["sources"]["other"] = {
        **definition["sources"]["orders"],
        "relation": {"kind": "table", "name": "other_orders"},
        "events": ["other_amount"],
        "properties": ["tier"],
        "dimensions": ["history"],
    }
    definition["events"]["other_amount"] = {"source": "other", "column": "amount"}
    definition["properties"]["tier"] = {
        "source": "other",
        "column": "tier",
        "dtype": "string",
        "as_of": "event_time",
        "dimension": "history",
    }
    definition["dimensions"]["history"] = {
        "relation": {"kind": "table", "name": "history"},
        "unit": "user",
        "key": "user_id",
        "validity": {"valid_from": "starts", "valid_to": "ends"},
        "properties": ["tier"],
    }
    definition["measures"]["other_sum"] = {
        "event": "other_amount",
        "aggregation": "count" if conversion else "sum",
        "exact": conversion,
    }
    definition["metrics"]["other_spend"] = {
        "kind": "conversion" if conversion else "mean",
        "unit": "user",
        "measure": "other_sum",
    }
    model = Model.from_catalog(
        definition,
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={name: Coverage("2024-12-01", "2025-02-01") for name in ("orders", "other")},
    )
    invalid = agg("spend", window=("-1d", "0s"))
    history = asof("tier", before="0s") if as_of else agg("other_spend", window=("-1d", "0s"))
    features = (
        {"a_history": history, "b_invalid": invalid}
        if history_first
        else {"a_invalid": invalid, "b_history": history}
    )
    error = _refusal(Design("user", FixedDate("2025-01-02"), features=features), model)
    assert error.code == (
        "terrine.temporal.duplicate_history"
        if history_first
        else "terrine.temporal.float64_precision"
    )
    if not history_first:
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
            EventAnchor("purchase", occurrence="first", offset="7d"),
            features={"recent": agg("float_revenue", window=("-7d", "0s"))},
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("decimal_revenue", "7d", periods=2, cumulative=True),
            ),
        )
        error = _refusal(design, model)
        assert error.code == "terrine.temporal.float64_precision"
        assert error.context["reason"] == "nonfinite_input"
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
        Schedule("1d", "2025-01-01", "2025-01-04"),
        features={"spend": agg("spend", window=("-1d", "0s"))},
        shape=CalendarComparisonSpec(
            Initiation("amount", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
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
    assert error.value.context["reason"] == "nonfinite_input"
    assert error.value.context["failed_count"] == 1

    con.raw_sql("UPDATE orders SET amount = 0 WHERE user_id = 'b'")
    materialized = design.build(model).materialize()
    meta = materialized.columns["spend"]
    assert sorted(
        (
            row["t0"],
            row["spend"],
            row[meta.complete_column],
            row[meta.reason_column],
        )
        for row in materialized.table.to_pylist()
        if row["unit"] == "a"
    ) == [
        (datetime(2025, 1, 3, tzinfo=UTC), 0, True, None),
        (datetime(2025, 1, 4, tzinfo=UTC), 1, True, None),
    ]


# --- the output statement carries the final run and the eligibility summary ------

_NOON = datetime(2025, 1, 1, 12, tzinfo=UTC)
_SPEND = {"spend": agg("spend", window=("-1d", "0s"))}


def test_calendar_support_keeps_original_cohort_after_adopter_is_excluded(native_inputs):
    catalog, con = native_inputs
    con.raw_sql("DELETE FROM events WHERE user_id = 'b'")
    con.raw_sql("DELETE FROM labels WHERE user_id = 'a'")
    con.raw_sql("UPDATE labels SET ts = TIMESTAMPTZ '2025-01-01 00:00:00+00'")
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={name: Coverage("2024-12-29", "2025-01-31") for name in ("events", "labels")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    design = Design(
        "user",
        Schedule("1d", "2025-01-02", "2025-01-04"),
        outcomes={"y": agg("sessions", window=("0d", "1d"))},
        eligibility={"has_label": eq(agg("churned", window=("-3d", "0s")), True)},
        shape=CalendarComparisonSpec(
            Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
            "never_treated",
            -1,
            (0, 1),
        ),
    )
    result = design.build(model).materialize()
    cohort = datetime(2025, 1, 3, tzinfo=UTC)
    assert sorted(
        zip(
            result.table["unit"].to_pylist(),
            result.table["cohort"].to_pylist(),
            result.table["event_period"].to_pylist(),
            strict=True,
        )
    ) == [("b", cohort, -1), ("b", cohort, 0), ("b", cohort, 1)]
    assert result.provenance.eligibility_summary == EligibilitySummary(6, 3, 0, 3)
    comparison = result.provenance.calendar_comparison
    assert comparison is not None and comparison.support is not None
    assert [
        (cell.cohort, cell.period, cell.kind, cell.treated, cell.control)
        for cell in comparison.support
    ] == [
        (cohort, -1, "reference", 0, 1),
        (cohort, 0, "comparison", 0, 1),
        (cohort, 1, "comparison", 0, 1),
    ]

    from terrine.export import CalendarContrast, preflight_export, preflight_warehouse

    contrast = CalendarContrast(
        outcome="y", cohort=cohort, reference_period=-1, comparison_periods=(0, 1)
    )
    with pytest.raises(TerrineError) as arrow:
        preflight_export(result, calendar_contrast=contrast)
    relation = con.create_table("calendar_export", result.table, temp=True)
    with (
        pytest.raises(TerrineError) as warehouse,
        preflight_warehouse(
            relation,
            roles=result.roles,
            columns=result.columns,
            provenance=result.provenance,
            calendar_contrast=contrast,
        ),
    ):
        raise AssertionError("control-only contrast was accepted")
    assert (
        arrow.value.code == warehouse.value.code == "terrine.export.calendar_contrast_unsupported"
    )
    assert arrow.value.context["arm"] == warehouse.value.context["arm"] == "treated"


def _recording(monkeypatch):
    """Record the SQL submitted for each study read, excluding provider setup."""
    from terrine.execution.inputs import _DuckDBSession

    executed, streamed = [], []
    execute, batches = _DuckDBSession.execute, _DuckDBSession.batches

    def recording_execute(self, con, query, sql):
        executed.append(sql)
        return execute(self, con, query, sql)

    def recording_batches(self, con, query, sql, **kwargs):
        streamed.append(sql)
        return batches(self, con, query, sql, **kwargs)

    monkeypatch.setattr(_DuckDBSession, "execute", recording_execute)
    monkeypatch.setattr(_DuckDBSession, "batches", recording_batches)
    return executed, streamed


@pytest.mark.parametrize(("label_count", "statements"), [(0, 2), (1, 3), (4, 3)])
def test_materialize_batches_independent_checks(con, monkeypatch, label_count, statements):
    model = _orders_model(
        con,
        [
            _order("a", _NOON, amount=1.0),
            _order("b", _NOON, amount=2.0),
            _order("a", _NOON.replace(day=2), amount=None),
            _order("b", _NOON.replace(day=2), amount=3.0),
        ],
    )
    labels = tuple(f"label_{i}" for i in range(4))
    frame = Design(
        "user",
        FixedDate("2025-01-02"),
        features=_SPEND,
        outcomes={name: agg("spend", window=("0s", "1d")) for name in labels},
    ).build(model)
    if label_count:
        frame = frame.training_set(label=labels[:label_count])
    executed, streamed = _recording(monkeypatch)
    result = frame.materialize()
    if label_count:
        assert result.table["unit"].to_pylist() == ["b"]
        assert result.provenance.dataset_view is not None
        assert result.provenance.dataset_view.counts_by_reason == {"all_values_null": label_count}
    else:
        assert result.table.num_rows == 2
    assert len(executed) + len(streamed) == statements
    assert not any(name.startswith("__terrine_gate_") for name in result.table.column_names)
    assert len(streamed) == 1
    assert "__terrine_gate_check_" in streamed[0] and "__terrine_gate_retained" in streamed[0]
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
    assert error.context["reason"] == "nonfinite_input"
    assert error.context["failed_count"] == 1


def test_training_output_prunes_unread_payload_without_pruning_candidate_check(con, monkeypatch):
    model = _orders_model(
        con,
        [
            _order("a", _NOON, amount=float("nan"), channel="store"),
            _order("b", _NOON, amount=2.0),
        ],
    )
    distinct = agg("web_distinct", window=("-1d", "0s"))
    frame = Design(
        "user",
        FixedDate("2025-01-02"),
        features=_SPEND,
        outcomes={"label": agg("web_distinct", window=("0s", "1d"))},
        eligibility={"web": gt(distinct, 0)},
    ).build(model).training_set(label="label", features=(), incomplete_labels="keep")
    _, streamed = _recording(monkeypatch)
    with pytest.raises(TerrineError) as error:
        frame.materialize()
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failed_count"] == 1
    assert len(streamed) == 1
    carries_payload = 'AS "spend"' in streamed[0]
    assert not carries_payload


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
        frame.materialize(budget=Budget(max_rows=1, max_output_bytes=10**6))
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
        budget=Budget(max_rows=2, max_output_bytes=reference.table.nbytes)
    )
    assert bounded.table.sort_by("unit").equals(reference.table.sort_by("unit"))
    with pytest.raises(TerrineError) as bytes_error:
        design.build(model).materialize(budget=Budget(max_output_bytes=reference.table.nbytes - 1))
    assert bytes_error.value.code == "terrine.budget.output_bytes_exceeded"
    with pytest.raises(TerrineError) as error:
        design.build(model).materialize(budget=Budget(max_rows=1, max_output_bytes=10**6))
    assert error.value.code == "terrine.budget.rows_exceeded"
