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
                "value": [1, 2, 8, 2, None],
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
                    "facts": ["value"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"value": {"source": "events", "column": "value"}},
            "properties": {},
            "dimensions": {},
            "measures": {
                "amount": {"fact": "value", "aggregation": "sum"},
                "amount_min": {"fact": "value", "aggregation": "min"},
                "amount_max": {"fact": "value", "aggregation": "max"},
                "amount_distinct": {"fact": "value", "aggregation": "count_distinct"},
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
            },
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-02")},
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
