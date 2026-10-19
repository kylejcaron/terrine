from datetime import UTC, datetime
from decimal import Decimal

import ibis
import pyarrow as pa
import pytest

from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg, quantile
from tests.reference.nonadditive import numpy_type7


@pytest.fixture
def nonadditive_model():
    con = ibis.duckdb.connect()
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["a", "a", "a", "a", "b"],
                "ts": [
                    datetime(2025, 1, 2, 10, tzinfo=UTC),
                    datetime(2025, 1, 2, 11, tzinfo=UTC),
                    datetime(2025, 1, 2, 11, 30, tzinfo=UTC),
                    datetime(2025, 1, 1, 23, 30, tzinfo=UTC),
                    datetime(2025, 1, 2, 11, tzinfo=UTC),
                ],
                "naive_ts": [
                    datetime(2025, 1, 2, 10),
                    datetime(2025, 1, 2, 11),
                    datetime(2025, 1, 2, 11, 30),
                    datetime(2025, 1, 1, 23, 30),
                    datetime(2025, 1, 2, 11),
                ],
                "value": [1, 2, 8, 2, None],
                "cents": pa.array(
                    [
                        Decimal("1.00"),
                        Decimal("1.01"),
                        Decimal("1.02"),
                        Decimal("1.01"),
                        None,
                    ],
                    type=pa.decimal128(8, 2),
                ),
                "wide": pa.array([0, 3, 5, 7, 0], type=pa.uint64()),
                "unit": [5, 6, 7, 6, 9],
            }
        ),
    )
    con.create_table("users", pa.table({"id": ["a", "b", "c"]}))
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["value", "cents", "wide", "unit"],
                    "properties": [],
                    "dimensions": [],
                },
                "naive_events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "naive_ts",
                    "entities": {"user": "user_id"},
                    "facts": ["naive_value"],
                    "properties": [],
                    "dimensions": [],
                },
            },
            "facts": {
                "value": {"source": "events", "column": "value"},
                "cents": {"source": "events", "column": "cents"},
                "wide": {"source": "events", "column": "wide"},
                "unit": {"source": "events", "column": "unit"},
                "naive_value": {"source": "naive_events", "column": "value"},
            },
            "properties": {},
            "dimensions": {},
            "measures": {
                "amount": {"fact": "value", "aggregation": "sum"},
                "amount_min": {"fact": "value", "aggregation": "min"},
                "amount_max": {"fact": "value", "aggregation": "max"},
                "amount_distinct": {"fact": "value", "aggregation": "count_distinct"},
                "decimal_amount": {"fact": "cents", "aggregation": "sum"},
                "wide_amount": {"fact": "wide", "aggregation": "sum"},
                "collision_distinct": {"fact": "unit", "aggregation": "count_distinct"},
                "naive_amount": {"fact": "naive_value", "aggregation": "sum"},
            },
            "metrics": {
                "amount": {"kind": "mean", "entity": "user", "measure": "amount"},
                "amount_min": {"kind": "mean", "entity": "user", "measure": "amount_min"},
                "amount_max": {"kind": "mean", "entity": "user", "measure": "amount_max"},
                "amount_distinct": {
                    "kind": "mean",
                    "entity": "user",
                    "measure": "amount_distinct",
                },
                "decimal_amount": {
                    "kind": "mean",
                    "entity": "user",
                    "measure": "decimal_amount",
                },
                "wide_amount": {"kind": "mean", "entity": "user", "measure": "wide_amount"},
                "collision_distinct": {
                    "kind": "mean",
                    "entity": "user",
                    "measure": "collision_distinct",
                },
                "naive_amount": {
                    "kind": "mean",
                    "entity": "user",
                    "measure": "naive_amount",
                },
            },
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")},
        readiness={
            "events": Coverage("2025-01-01", "2025-01-02"),
            "naive_events": Coverage("2025-01-01", "2025-01-02"),
        },
    )
    try:
        yield model
    finally:
        con.disconnect()


def test_quantile_frame_matches_independent_numpy_type7_and_null_reasons(nonadditive_model):
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={"p25": quantile("amount", "0.25", window=("-1d", "0d"))},
    )
    result = design.build(nonadditive_model).materialize()
    rows = result.to_arrow().sort_by("unit").to_pylist()
    oracle = numpy_type7([1, 2, 2, 8], 0.25)
    assert rows[0]["p25"] == Decimal("1.75")
    assert float(rows[0]["p25"]) == oracle
    meta = result.columns["p25"]
    assert [row[meta.reason_column] for row in rows] == [
        None,
        "all_values_null",
        "no_observations",
    ]


def test_nonadditive_window_is_half_open_at_intraday_anchor(nonadditive_model):
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 11, tzinfo=UTC)),
        features={"p50": quantile("amount", "0.5", window=("-1h", "0h"))},
    )
    result = design.build(nonadditive_model).materialize()
    rows = result.to_arrow().sort_by("unit").to_pylist()
    assert [row["p50"] for row in rows] == [Decimal("1.00"), None, None]
    reason_column = result.columns["p50"].reason_column
    assert [row[reason_column] for row in rows] == [None, "no_observations", "no_observations"]


def test_exact_probability_scale_is_not_narrowed_to_backend_default(nonadditive_model):
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={"q": quantile("amount", "0.0001", window=("-1d", "0d"))},
    )
    rows = design.build(nonadditive_model).materialize().to_arrow().sort_by("unit").to_pylist()
    assert rows[0]["q"] == Decimal("1.0003")


def test_decimal_quantile_preserves_interpolation_digits(nonadditive_model):
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 11, 30, tzinfo=UTC)),
        features={"q": quantile("decimal_amount", "0.5", window=("-2h", "0h"))},
    )
    rows = design.build(nonadditive_model).materialize().to_arrow().sort_by("unit").to_pylist()
    assert rows[0]["q"] == Decimal("1.005")


def test_uint64_quantile_reserves_twenty_integer_digits(nonadditive_model):
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={"q": quantile("wide_amount", "0.5", window=("-1d", "0d"))},
    )
    rows = design.build(nonadditive_model).materialize().to_arrow().sort_by("unit").to_pylist()
    assert rows[0]["q"] == Decimal("4.0")


def test_nonadditive_fact_alias_does_not_collide_with_analysis_unit(nonadditive_model):
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={"distinct": agg("collision_distinct", window=("-1d", "0d"))},
    )
    rows = design.build(nonadditive_model).materialize().to_arrow().sort_by("unit").to_pylist()
    assert [row["distinct"] for row in rows] == [3, 1, 0]


def test_naive_utc_source_timestamp_is_not_shifted_by_session_timezone(nonadditive_model):
    nonadditive_model.con.raw_sql("SET TimeZone='America/Los_Angeles'")
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={"q": quantile("naive_amount", "0.25", window=("-1d", "0d"))},
    )
    rows = design.build(nonadditive_model).materialize().to_arrow().sort_by("unit").to_pylist()
    assert rows[0]["q"] == Decimal("1.75")


def test_min_max_and_distinct_windows_preserve_empty_and_null_rows(nonadditive_model):
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={
            "minimum": agg("amount_min", window=("-1d", "0d")),
            "maximum": agg("amount_max", window=("-1d", "0d")),
            "distinct": agg("amount_distinct", window=("-1d", "0d")),
        },
    )
    rows = design.build(nonadditive_model).materialize().to_arrow().sort_by("unit").to_pylist()
    assert [(row["minimum"], row["maximum"], row["distinct"]) for row in rows] == [
        (1, 8, 3),
        (None, None, 0),
        (None, None, 0),
    ]


def test_quantile_probability_extends_over_nonadditive_measure_facts(nonadditive_model):
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={"q": quantile("amount_min", "0.25", window=("-1d", "0d"))},
    )
    rows = design.build(nonadditive_model).materialize().to_arrow().sort_by("unit").to_pylist()
    assert rows[0]["q"] == Decimal("1.75")


def test_reanchored_nonadditive_feature_cutoff_uses_absolute_selector_support(nonadditive_model):
    from terrine.spec import event_time

    selector = event_time("value", window=("-3d", "-2d"))
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={"minimum": agg("amount_min", window=("0d", "1d"), relative_to=selector)},
    )
    frame = design.build(nonadditive_model)
    assert frame._compiled.feature_cutoffs["minimum"].latest_seconds == -86_400


def test_quantile_refuses_probability_scale_exceeding_exact_rank_budget(nonadditive_model):
    from terrine.errors import TerrineError

    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={"q": quantile("amount", "0.000000001", window=("-1d", "0d"))},
    )
    with pytest.raises(TerrineError) as error:
        design.build(nonadditive_model)
    assert error.value.code == "terrine.metric.exact_precision_unavailable"


def test_persisted_quantile_uses_raw_events_without_prefix_layer(nonadditive_model):
    from terrine.compile.layers import plan_layers
    from terrine.compile.materialize import compose_materialized_frame

    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={"q": quantile("amount", "0.25", window=("-1d", "0d"))},
    )
    assert [key.layer for key in plan_layers(design, nonadditive_model).keys] == ["anchor"]
    compiled = compose_materialized_frame(design, nonadditive_model, persisted={})
    rows = nonadditive_model.con.to_pyarrow(compiled.table).to_pylist()
    assert rows[0]["q"] == Decimal("1.75")


def test_approximate_distinct_executes_in_duckdb_and_marks_column_metadata(nonadditive_model):
    from terrine import Frame
    from terrine.spec import approx_distinct

    design = Design(
        "user",
        FixedDate("2025-01-03"),
        features={"distinct": approx_distinct("amount_distinct", window=("-2d", "0d"))},
    )
    first = Frame(design, nonadditive_model).materialize()
    second = Frame(design, nonadditive_model).materialize()

    assert first.table.column("distinct").to_pylist() == [3, 0, 0]
    assert first.table.column("distinct").to_pylist() == second.table.column("distinct").to_pylist()
    meta = first.columns["distinct"].approximation
    assert meta.algorithm == "hll"
    assert meta.implementation == "DuckDB approx_count_distinct"
    assert meta.error_description == "HLL relative standard error ≈ 1.04/√m"
