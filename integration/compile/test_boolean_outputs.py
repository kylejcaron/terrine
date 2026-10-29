from collections.abc import Mapping
from dataclasses import dataclass

import ibis
import pyarrow as pa
import pytest

import terrine as tr
from terrine.data import ColumnMeta, ExpressionTiming
from terrine.dataset.splits import TimeSplit
from terrine.semantic.models import Catalog


@dataclass(frozen=True)
class BooleanResult:
    table: pa.Table
    columns: Mapping[str, ColumnMeta]
    timings: Mapping[str, ExpressionTiming]


def _inputs():
    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE users AS SELECT id, until_at::TIMESTAMPTZ AS until_at
        FROM (VALUES ('a', '2025-03-01'), ('b', '2025-03-01'),
                     ('c', '2025-01-03')) AS u(id, until_at)
    """)
    con.raw_sql("""
        CREATE TABLE events AS SELECT id, ts::TIMESTAMPTZ AS ts, item
        FROM (VALUES ('a', '2025-01-04', 1), ('a', '2025-01-05', 2),
                     ('a', '2025-01-06', 3), ('c', '2025-01-02', 1)) AS e(id, ts, item)
    """)
    catalog = Catalog.model_validate(
        {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "ts",
                    "units": {"user": "id"},
                    "events": ["order", "item"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "events": {
                "order": {"source": "events", "column": None},
                "item": {"source": "events", "column": "item"},
            },
            "properties": {},
            "dimensions": {},
            "measures": {
                "orders": {"event": "order", "aggregation": "count"},
                "items": {"event": "item", "aggregation": "count_distinct"},
            },
            "metrics": {
                name: {"kind": "mean", "unit": "user", "measure": name}
                for name in ("orders", "items")
            },
        }
    )
    model = tr.Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": tr.Coverage("2024-12-01", "2025-03-01")},
        units={"user": tr.UnitSource("users", "id", observed_until="until_at")},
    )
    week = tr.agg("orders", window=("0s", "7d"))
    short = tr.agg("orders", window=("0s", "1d"))
    distinct = tr.agg("items", window=("0s", "7d"))
    design = tr.Design(
        "user",
        tr.FixedDate("2025-01-01"),
        features={"no_prior_order": tr.is_null(tr.event_time("order", window=("-7d", "0s")))},
        outcomes={
            "converted_7d": tr.gt(week, 0),
            "orders_7d": week,
            "churned_30d": tr.eq(tr.agg("orders", window=("0s", "30d")), 0),
            "false_and_unknown": tr.all_of(tr.gt(short, 0), tr.gt(week, 0)),
            "true_and_unknown": tr.all_of(tr.eq(short, 0), tr.gt(week, 0)),
            "many_items": tr.gt(distinct, 2),
            "items_7d": distinct,
        },
    )
    return con, model, design


@pytest.fixture(scope="module")
def boolean_result():
    con, model, design = _inputs()
    try:
        frame = design.build(model)
        result = frame.materialize(tr.Budget(max_rows=10, strict_scan=False))
        return BooleanResult(
            result.table, frame._compiled.columns, frame._compiled.expression_timings
        )
    finally:
        con.disconnect()


def _states(result, name):
    meta = result.columns[name]
    return {
        row["unit"]: (row[name], row[meta.complete_column], row[meta.reason_column])
        for row in result.table.to_pylist()
    }


def test_converted_within_7d_true_false_and_unknown(boolean_result):
    assert _states(boolean_result, "converted_7d") == {
        "a": (True, True, None),
        "b": (False, True, None),
        "c": (None, False, "incomplete_observation"),
    }
    assert boolean_result.table.schema.field("converted_7d").type == pa.bool_()
    assert boolean_result.columns["converted_7d"].role == "outcome"


def test_conjunction_with_complete_false_term_is_false_and_complete(boolean_result):
    assert _states(boolean_result, "false_and_unknown")["c"] == (False, True, None)


def test_conjunction_with_true_and_incomplete_terms_is_null(boolean_result):
    assert _states(boolean_result, "true_and_unknown")["c"] == (
        None,
        False,
        "incomplete_observation",
    )
    assert boolean_result.timings["true_and_unknown"].support_seconds == (0, 7 * 86400)


def test_churned_30d_eq_zero(boolean_result):
    assert _states(boolean_result, "churned_30d") == {
        "a": (False, True, None),
        "b": (True, True, None),
        "c": (None, False, "incomplete_observation"),
    }


def test_is_null_event_time_feature_and_arrow_metadata_round_trip(boolean_result):
    assert _states(boolean_result, "no_prior_order") == {unit: (True, True, None) for unit in "abc"}
    assert boolean_result.columns["no_prior_order"].role == "feature"
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, boolean_result.table.schema) as writer:
        writer.write_table(boolean_result.table)
    restored = pa.ipc.open_stream(sink.getvalue()).read_all()
    assert restored.equals(boolean_result.table, check_metadata=True)


def test_predicate_over_nonadditive_operand_gets_range_plan(boolean_result):
    from terrine.compile.ranges import plan_ranges

    con, model, _ = _inputs()
    try:
        operand = tr.agg("items", window=("0s", "7d"))
        design = tr.Design(
            "user", tr.FixedDate("2025-01-01"), outcomes={"many_items": tr.gt(operand, 2)}
        )
        assert set(plan_ranges(design, model)) == {operand}
        frame = design.build(model)
        result = frame.materialize(tr.Budget(max_rows=10, strict_scan=False))
        predicate_only = BooleanResult(
            result.table, frame._compiled.columns, frame._compiled.expression_timings
        )
        assert _states(predicate_only, "many_items") == {
            "a": (True, True, None),
            "b": (False, True, None),
            "c": (None, False, "incomplete_observation"),
        }
        unwrapped = {row["unit"]: row["items_7d"] for row in boolean_result.table.to_pylist()}
        for row in predicate_only.table.to_pylist():
            expected = unwrapped[row["unit"]]
            assert row["many_items"] == (None if expected is None else expected > 2)
    finally:
        con.disconnect()


def test_time_split_assigns_boolean_label_by_its_window():
    con, model, design = _inputs()
    try:
        frame = design.build(model)
        splits = []
        for label in ("orders_7d", "converted_7d", "true_and_unknown"):
            training = frame.training_set(
                label=label,
                incomplete_labels="keep",
                split=TimeSplit(train_until="2025-01-05"),
            ).materialize(tr.Budget(max_rows=10, strict_scan=False))
            splits.append(
                {
                    row["unit"]: (row["__terrine_split"], row["__terrine_split_reason"])
                    for row in training.table.to_pylist()
                }
            )
        assert (
            splits[0]
            == splits[1]
            == splits[2]
            == {
                "a": ("unassigned", "label_overlap"),
                "b": ("unassigned", "label_overlap"),
                "c": ("unassigned", "incomplete_observation"),
            }
        )
    finally:
        con.disconnect()


def test_predicate_runtime_layers_match_direct_values():
    from terrine.runtime.materialize import compose_runtime_frame
    from tests.compile.test_persisted_layers import _persisted_prefixes

    con, model, design = _inputs()
    week = tr.agg("items", window=("0s", "7d"))
    design = tr.Design(
        "user",
        design.anchor,
        features=design.features,
        outcomes={
            **design.outcomes,
            "combined": tr.all_of(
                tr.gt(week, 2),
                tr.eq(tr.agg("items", window=("0s", "30d")), 0),
            ),
            "combined[0]": week,
        },
    )
    try:
        direct = design.build(model)._compiled
        runtime = compose_runtime_frame(design, model, persisted=_persisted_prefixes(design, model))
        expected = con.to_pyarrow(direct.table.order_by("unit"))
        actual = con.to_pyarrow(runtime.table.order_by("unit"))
        assert actual.equals(expected)
        assert runtime.expression_timings == direct.expression_timings
        assert runtime.range_plans == direct.range_plans
        assert len(direct.range_plans) == 5
    finally:
        con.disconnect()


def test_false_conjunction_retains_unresolved_operand_support_for_time_split():
    con, model, _ = _inputs()
    try:
        selector = tr.event_time("order", window=("0s", "7d"))
        design = tr.Design(
            "user",
            tr.FixedDate("2025-01-01"),
            outcomes={
                "label": tr.all_of(
                    tr.gt(tr.agg("orders", window=("0s", "1d")), 0),
                    tr.gt(tr.agg("orders", window=("0s", "7d"), relative_to=selector), 0),
                )
            },
        )
        frame = design.build(model).training_set(
            label="label",
            incomplete_labels="keep",
            split=TimeSplit(train_until="2025-01-10"),
        )
        materialized = frame.materialize(tr.Budget(max_rows=10, strict_scan=False))
        row = next(row for row in materialized.table.to_pylist() if row["unit"] == "b")
        meta = materialized.columns["label"]
        assert (row["label"], row[meta.complete_column], row[meta.reason_column]) == (
            False,
            True,
            None,
        )
        assert (row["__terrine_split"], row["__terrine_split_reason"]) == (
            "unassigned",
            "label_overlap",
        )
    finally:
        con.disconnect()
