from dataclasses import replace

import ibis
import pytest

from terrine.compile.layers import plan_layers
from terrine.compile.materialize import compose_materialized_frame
from terrine.spec import Design, FixedDate, agg
from tests.compile.test_columns import _compile

pytest_plugins = ("tests.compile.test_columns",)


def test_persisted_daily_prefix_anchor_reuse_matches_ordinary_frame_compiler(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    ordinary = _compile(column_model, features={"total": agg("total", window=("-1d", "0s"))})
    reused = compose_materialized_frame(design, column_model, persisted={})
    assert column_model.con.to_pyarrow(ordinary.table.order_by("unit")).equals(
        column_model.con.to_pyarrow(reused.table.order_by("unit"))
    )
    assert ordinary.columns == reused.columns


def test_layer_substitution_checks_measure_unit_grain_and_resolution_identity(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    key = plan_layers(design, column_model).keys[0]
    bad = replace(key, grain="event")
    with pytest.raises(ValueError, match="identity"):
        compose_materialized_frame(design, column_model, persisted={bad: ibis.memtable({"x": []})})


def test_layer_substitution_rejects_cross_snapshot_relations(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    key = plan_layers(design, column_model).keys[0]
    a = replace(key, snapshot_id="snapshot-a")
    b = replace(key, snapshot_id="snapshot-b", source="other")
    empty = ibis.memtable({"_unit": [], "_timestamp": [], "_value": []})
    with pytest.raises(ValueError, match="one non-empty snapshot"):
        compose_materialized_frame(design, column_model, persisted={a: empty, b: empty})
