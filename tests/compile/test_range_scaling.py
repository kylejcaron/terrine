# ruff: noqa: F811
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal, cast

import pytest

from terrine.compile.dispatch import compile_design
from terrine.data import Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.bind import normalize_events
from terrine.spec import (
    Design,
    EventAnchor,
    EventPanel,
    Initiation,
    Schedule,
    Sequential,
    agg,
    event_time,
    quantile,
)
from terrine.temporal.daily import resolve_measure
from terrine.temporal.nonadditive import aggregate_nonadditive_window
from tests.compile.test_nonadditive import nonadditive_model  # noqa: F401
from tests.reference.ranges import window_values


def _features():
    return {
        "minimum": agg("amount_min", window=("-2d", "0d")),
        "maximum": agg("amount_max", window=("-2d", "0d")),
        "distinct": agg("amount_distinct", window=("-2d", "0d")),
        "q25": quantile("amount", "0.25", window=("-2d", "0d")),
    }


@pytest.mark.parametrize("shape", ["schedule", "event_panel", "sequential"])
def test_bounded_shape_matches_exact_path_and_independent_pandas(nonadditive_model, shape):
    original = nonadditive_model
    model = Model.from_catalog(
        original.catalog,
        con=original.con,
        units=original.units,
        readiness={name: Coverage("2025-01-01", "2025-01-31") for name in original.sources},
    )
    if shape == "event_panel":
        design = EventPanel(
            Design("user", EventAnchor("value", "first"), features=_features()),
            periods=(-1, 0, 1),
            period_seconds=86_400,
        )
    else:
        design = Design("user", Schedule("1d", "2025-01-02", "2025-01-04"), features=_features())
        if shape == "sequential":
            design = Sequential(
                design,
                treatment=Initiation("value", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
            )
    compiled = compile_design(design, model)
    assert set(compiled.range_plans) == set(_features())
    assert all(plan.max_candidates_per_event <= 3 for plan in compiled.range_plans.values())
    assert all("blocked_range_join" in compiled.strategy[name] for name in _features())
    rows = model.con.to_pyarrow(compiled.audit_table).to_pylist()
    events = (
        model.con.table("events").select(unit="user_id", timestamp="ts", value="value").to_pandas()
    )
    anchors = model.con.to_pyarrow(compiled._anchors.table).to_pandas()
    oracle = window_values(events, anchors, -2 * 86_400, 0)
    oracle_by_key = {(row["unit"], str(row["anchor_id"])): row for row in oracle}
    source = model.sources["events"]
    normalized = normalize_events(model.con.table("events"), source, source.units["user"])
    for name, expression in _features().items():
        unresolved = model.metrics[expression.metric].measures[0]
        measure = resolve_measure(normalized, unresolved, source=source, allow_nonadditive=True)
        exact = aggregate_nonadditive_window(
            compiled._anchors.table,
            normalized,
            measure,
            source,
            expression.window,
            model.readiness,
            output=name,
            role="feature",
            operation=cast(
                Literal["min", "max", "count_distinct", "quantile"],
                "quantile" if name == "q25" else measure.aggregation,
            ),
            probability=Decimal("0.25") if name == "q25" else None,
        )
        exact_rows = {
            (item["unit"], str(item["anchor_id"])): item
            for item in model.con.to_pyarrow(exact.table).to_pylist()
        }
        for row in rows:
            key = row["unit"], str(row["anchor_id"])
            expected = oracle_by_key[key]
            exact_row = exact_rows[key]
            assert row[name] == exact_row[name]
            meta = compiled.columns[name]
            exact_meta = exact.columns[name]
            assert row[meta.reason_column] == exact_row[exact_meta.reason_column]
            assert row[meta.complete_column] == exact_row[exact_meta.complete_column]
            if row[meta.complete_column]:
                assert row[name] == expected[name]


@pytest.mark.parametrize("kind", ["each", "dense", "relative"])
def test_range_bound_refuses_before_source_execution(nonadditive_model, monkeypatch, kind):
    if kind == "each":
        anchor = EventAnchor("value", "each")
        feature = agg("amount_min", window=("-2d", "0d"))
    elif kind == "dense":
        anchor = Schedule("1s", "2025-01-02", "2025-01-02")
        feature = agg("amount_min", window=("-1d", "0d"))
    else:
        anchor = Schedule("1h", "2025-01-01", "2025-01-30")
        feature = agg(
            "amount_min",
            window=("-1s", "0s"),
            relative_to=event_time("value", window=("-1d", "0d")),
        )

    def fail_execution(*args, **kwargs):
        raise AssertionError("range planning executed a source query")

    monkeypatch.setattr(nonadditive_model.con, "to_pyarrow", fail_execution)
    with pytest.raises(TerrineError) as error:
        compile_design(Design("user", anchor, features={"minimum": feature}), nonadditive_model)
    assert error.value.code == "terrine.temporal.range_bound_unavailable"
    assert error.value.context["route"]
    assert error.value.context["limit"] == 128


def test_additive_prefix_does_not_inherit_nonadditive_overlap_limit(nonadditive_model):
    design = Design(
        "user",
        Schedule("1s", "2025-01-02", "2025-01-02"),
        features={"total": agg("amount", window=("-1d", "0d"))},
    )
    compiled = compile_design(design, nonadditive_model)
    assert compiled.range_plans == {}
    assert "prefix" in compiled.strategy["total"]


def test_nonadditive_layer_plan_does_not_request_subtractable_prefixes(nonadditive_model):
    from terrine.compile.layers import plan_layers

    design = Design("user", Schedule("1d", "2025-01-02", "2025-01-03"), features=_features())
    assert {key.layer for key in plan_layers(design, nonadditive_model).keys} == {"anchor"}


@pytest.mark.parametrize("shape", ["calendar", "event_sequence", "binned_state", "observed_state"])
def test_shape_outputs_preserve_range_bound_metadata(nonadditive_model, shape):
    from terrine.semantic.models import Catalog
    from terrine.spec import (
        BinnedMetricState,
        CalendarComparisonSpec,
        EventSequence,
        StateObservations,
        StatePanel,
    )
    from terrine.windows import Window

    original = nonadditive_model
    catalog = original.catalog.model_dump(mode="json")
    catalog["sources"]["events"]["properties"] = ["state"]
    catalog["properties"]["state"] = {
        "source": "events",
        "column": "user_id",
        "dtype": "string",
        "as_of": "event_time",
    }
    model = Model.from_catalog(
        Catalog.model_validate(catalog),
        con=original.con,
        units=original.units,
        readiness={name: Coverage("2025-01-01", "2025-01-31") for name in original.sources},
    )
    q = quantile("amount", "0.25", window=("-1d", "0d"))
    if shape == "calendar":
        design = Design(
            "user",
            Schedule("1d", "2025-01-01", "2025-01-04"),
            features={"q": q},
            shape=CalendarComparisonSpec(
                Initiation("value", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
                "never_treated",
                -1,
                (0,),
            ),
        )
    elif shape == "event_sequence":
        design = Design(
            "user",
            EventAnchor("value", "first"),
            features={"q": q},
            shape=EventSequence("value", Window("-1d", "0d"), max_events=10),
        )
    else:
        state = (
            BinnedMetricState("amount", cutpoints=(0,), levels=("none", "some"))
            if shape == "binned_state"
            else StateObservations("state", levels=("a", "b", "c"))
        )
        design = Design(
            "user",
            Schedule("1d", "2025-01-02", "2025-01-03"),
            features={"q": q},
            shape=StatePanel(
                state,
                max_periods=2,
                baseline_features={
                    "baseline_min": agg("amount_min", window=("-1d", "0d")),
                },
            ),
        )
    compiled = compile_design(design, model)
    names = {"q", "baseline_min"} if shape.endswith("state") else {"q"}
    assert set(compiled.range_plans) == names
    assert all(compiled.range_plans[name].max_candidates_per_event <= 2 for name in names)
    rows = model.con.to_pyarrow(compiled.table).to_pylist()
    assert rows
    assert all(name in rows[0] for name in names)
