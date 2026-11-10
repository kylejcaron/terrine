"""State panel tests moved for fixed compiler overhead."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from terrine.data import Budget, Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import BinnedMetricState, Design, Schedule, StatePanel, agg
from tests.compile.test_state_panel import _observed_design as _observed_design
from tests.compile.test_state_panel import observed_model_factory as observed_model_factory  # noqa: F401
from tests.compile.test_state_panel import panel_model as panel_model  # noqa: F401
from tests.conftest import events_world as events_world  # noqa: F401
from tests.conftest import native_inputs as native_inputs  # noqa: F401


def _sum_model(
    native_inputs,
    timestamps,
    values,
    *,
    coverage=("2025-01-01", "2025-01-31"),
    day_boundary="UTC",
):
    import pyarrow as pa

    from terrine.semantic.models import Catalog

    catalog, con = native_inputs
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["a"] * len(values),
                "ts": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
                "amount": pa.array(
                    values,
                    type=pa.decimal128(38, 0) if isinstance(values[0], Decimal) else pa.float64(),
                ),
            }
        ),
        overwrite=True,
    )
    definition = catalog.model_dump(mode="python")
    definition["day_boundary"] = day_boundary
    definition["sources"]["events"]["events"] = ("session", "amount")
    definition["events"]["amount"] = {"source": "events", "column": "amount"}
    definition["measures"]["total_sum"] = {
        "event": "amount",
        "aggregation": "sum",
        "exact": isinstance(values[0], Decimal),
    }
    definition["metrics"]["total"] = {
        "kind": "mean",
        "unit": "user",
        "measure": "total_sum",
    }
    return Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness={"events": Coverage(*coverage)},
        units={"user": UnitSource("users", "user_id")},
    )


def test_baseline_is_computed_for_delayed_entry_at_panel_start(native_inputs):
    import pyarrow as pa

    catalog, con = native_inputs
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["a", "b"],
                "ts": [
                    datetime(2025, 1, 2, tzinfo=UTC),
                    datetime(2025, 1, 2, tzinfo=UTC),
                ],
                "event_id": ["a0", "b1"],
                "alternate_event_id": ["a0", "b1"],
            }
        ),
        overwrite=True,
    )
    con.create_table(
        "users",
        pa.table(
            {
                "user_id": ["a", "b"],
                "entered_at": [
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 4, tzinfo=UTC),
                ],
            }
        ),
        overwrite=True,
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id", entered_at="entered_at")},
    )
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        features={"origin": agg("sessions", window=("-1d", "0d"))},
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            baseline_features={"baseline": agg("sessions", window=("-1d", "0d"))},
            periods=2,
        ),
    )
    materialized = design.build(model).materialize(
        budget=Budget(max_rows=10, max_output_bytes=100_000)
    )
    restored = type(materialized).from_arrow(materialized.to_pyarrow())
    assert len(restored.to_pandas()) == materialized.table.num_rows
    rows = materialized.table.to_pylist()
    entrant = next(row for row in rows if row["unit"] == "b" and row["period_index"] == 1)
    assert entrant["baseline"] == 1
    assert entrant["origin"] == 0
    assert restored.provenance.feature_cutoffs["baseline"].reference == "panel_start"
    assert restored.provenance.feature_cutoffs["origin"].reference == "row_t0"
    assert entrant[materialized.columns["baseline"].complete_column] is True


def test_float_metric_bin_compares_the_rounded_value_at_a_near_boundary_cut(native_inputs):
    import pyarrow as pa

    from terrine.semantic.models import Catalog

    cut = float.fromhex("0x1.0000000000001p+0")
    catalog, con = native_inputs
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["a", "a"],
                "ts": [
                    datetime(2025, 1, 3, tzinfo=UTC),
                    datetime(2025, 1, 3, 0, 0, 1, tzinfo=UTC),
                ],
                "event_id": ["a-f1", "a-f2"],
                "alternate_event_id": ["a-g1", "a-g2"],
                "score": pa.array([1.0, 1.0], type=pa.float64()),
            }
        ),
        overwrite=True,
    )
    definition = catalog.model_dump(mode="python")
    definition["sources"]["events"]["events"] = ("session", "score")
    definition["events"]["score"] = {"source": "events", "column": "score"}
    definition["measures"]["score_mean"] = {"event": "score", "aggregation": "avg_event"}
    definition["metrics"]["score_avg"] = {
        "kind": "mean",
        "unit": "user",
        "measure": "score_mean",
    }
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
    )
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-03"),
        shape=StatePanel(
            BinnedMetricState("score_avg", cutpoints=(cut,), levels=("low", "high")),
            periods=1,
        ),
    )
    rows = (
        design.build(model)
        .materialize(budget=Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    row = next(row for row in rows if row["unit"] == "a")
    assert row["to_state"] == "low"
    assert row["to_state__reason"] is None


def test_binned_panel_retains_named_eligibility_column(panel_model):
    from terrine.compile.dispatch import compile_design
    from terrine.spec import gt

    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        eligibility={"has_history": gt(agg("sessions", window=("-1d", "0d")), 0)},
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            periods=2,
        ),
    )
    compiled = compile_design(design, panel_model)
    assert "has_history" in compiled.audit_table.columns
    materialized = design.build(panel_model).materialize(
        budget=Budget(max_rows=10, max_output_bytes=100_000)
    )
    assert materialized.table["has_history"].to_pylist() == [True, True]


def test_observed_panel_retains_named_eligibility_column(observed_model_factory):
    from terrine.compile.dispatch import compile_design
    from terrine.spec import StateObservations, gt

    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 4, tzinfo=UTC), "active"),
        ]
    )
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        eligibility={"has_history": gt(agg("sessions", window=("-1d", "0d")), 0)},
        shape=StatePanel(
            StateObservations("state", ("active", "closed")),
            periods=2,
        ),
    )
    compiled = compile_design(design, model)
    assert "has_history" in compiled.audit_table.columns
    materialized = design.build(model).materialize(
        budget=Budget(max_rows=10, max_output_bytes=100_000)
    )
    assert materialized.table["has_history"].to_pylist() == [True, True]


@pytest.mark.parametrize(
    ("budget", "code"),
    [(Budget(max_rows=1, max_output_bytes=100_000), "terrine.budget.rows_exceeded")],
)
def test_state_panel_enforces_output_row_and_byte_budgets(panel_model, budget, code):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            periods=2,
        ),
    )
    with pytest.raises(TerrineError) as error:
        design.build(panel_model).materialize(budget=budget)
    assert error.value.code == code


@pytest.mark.parametrize(
    ("every", "boundary"), [("1d", "UTC"), ("12h", "UTC"), ("1d", "UTC+05:30")]
)
def test_adjacent_binned_state_relation_pairs_response_with_successor_prior(
    native_inputs, every, boundary
):
    import pyarrow as pa

    timestamps = [datetime(2025, 1, 3, tzinfo=UTC), datetime(2025, 1, 4, tzinfo=UTC)]
    values = [Decimal(1), None]
    if boundary != "UTC":
        timestamps = [
            datetime(2025, 1, 2, 18, 29, 59, 999999, tzinfo=UTC),
            datetime(2025, 1, 2, 18, 30, tzinfo=UTC),
            datetime(2025, 1, 3, 18, 30, tzinfo=UTC),
            datetime(2025, 1, 4, 18, 29, 59, 999999, tzinfo=UTC),
            datetime(2025, 1, 4, 18, 30, tzinfo=UTC),
        ]
        values = [Decimal(0), Decimal(1), None, None, Decimal(9)]
    model = _sum_model(native_inputs, timestamps, values, day_boundary=boundary)
    schedule = Schedule(every, "2025-01-03", "2025-01-04")
    periods = schedule.count(model.day_boundary)
    design = Design(
        "user",
        schedule,
        features={"past_sessions": agg("total", window=(f"-{every}", "0d"))},
        shape=StatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("none", "some")),
            periods=periods,
        ),
    )
    table = (
        design.build(model).materialize(budget=Budget(max_rows=10, max_output_bytes=100_000)).table
    )
    public = table.select(
        [
            "unit",
            "anchor_id",
            "period_index",
            "from_state",
            "to_state",
            "from_state__complete",
            "from_state__reason",
            "to_state__complete",
            "to_state__reason",
        ]
    )
    for name in ("to_time", "scheduled_to"):
        public = public.append_column(name, table[name].cast(pa.timestamp("us", tz="UTC")))
    rows = public.to_pylist()
    a_positions = sorted(
        (index for index, row in enumerate(rows) if row["unit"] == "a"),
        key=lambda index: rows[index]["period_index"],
    )
    a_rows = [rows[index] for index in a_positions]
    assert len(a_rows) == periods
    assert [row["anchor_id"] for row in a_rows] == [f"schedule:{index}" for index in range(periods)]
    assert a_rows[0]["period_index"] == 0
    assert a_rows[1]["period_index"] == 1
    assert a_rows[0]["to_state"] == "some"
    assert a_rows[0]["to_state"] == a_rows[1]["from_state"]
    assert a_rows[0]["to_time"] == schedule.instant(1, model.day_boundary)
    assert a_rows[0]["scheduled_to"] == a_rows[0]["to_time"]
    assert a_rows[0]["to_state__complete"] is True
    assert a_rows[0]["to_state__reason"] is None
    null_pair = a_rows[-1] if every == "1d" else a_rows[-2]
    assert null_pair["to_state"] is None
    assert null_pair["to_state__complete"] is False
    assert null_pair["to_state__reason"] == "all_values_null"
    if boundary != "UTC":
        assert [
            (
                row["anchor_id"],
                row["from_state"],
                row["to_state"],
                row["from_state__complete"],
                row["to_state__complete"],
                row["from_state__reason"],
                row["to_state__reason"],
            )
            for row in a_rows
        ] == [
            ("schedule:0", "none", "some", True, True, None, None),
            ("schedule:1", "some", None, True, False, None, "all_values_null"),
        ]
        expected_ticks = {
            "t0": [1735842600000000, 1735929000000000],
            "from_time": [1735842600000000, 1735929000000000],
            "to_time": [1735929000000000, 1736015400000000],
            "scheduled_to": [1735929000000000, 1736015400000000],
            "from_state_observed_from": [1735669800000000, 1735669800000000],
            "to_state_observed_from": [1735669800000000, 1735669800000000],
            "from_state_observed_until": [1738348200000000, 1738348200000000],
            "to_state_observed_until": [1738348200000000, 1738348200000000],
        }
        for name, expected in expected_ticks.items():
            ticks = table[name].cast(pa.int64()).to_pylist()
            assert [ticks[index] for index in a_positions] == expected


@pytest.mark.parametrize("every", ["1d", "12h"])
def test_binned_response_remains_complete_at_membership_exit(native_inputs, every):
    import pyarrow as pa

    base = _sum_model(
        native_inputs,
        [datetime(2025, 1, 3, hour, tzinfo=UTC) for hour in (0, 13)],
        [Decimal(1), Decimal(1)],
    )
    catalog, con = base.catalog, base.con
    con.create_table(
        "users",
        pa.table(
            {
                "user_id": ["a", "b"],
                "exited_at": [
                    datetime(2025, 1, 4, tzinfo=UTC),
                    datetime(2025, 1, 10, tzinfo=UTC),
                ],
            }
        ),
        overwrite=True,
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id", exited_at="exited_at")},
    )
    design = Design(
        "user",
        Schedule(every, "2025-01-03", "2025-01-03"),
        shape=StatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("none", "some")),
            periods=2 if every == "12h" else 1,
        ),
    )
    rows = (
        design.build(model)
        .materialize(budget=Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    row = max((row for row in rows if row["unit"] == "a"), key=lambda row: row["period_index"])
    assert row["to_state"] == "some"
    assert row["to_state__complete"] is True


@pytest.mark.parametrize("every", ["1d", "12h"])
def test_binned_missing_period_sides_and_unit_end_exclusivity(native_inputs, every):
    import pyarrow as pa

    base = _sum_model(native_inputs, [datetime(2025, 1, 3, tzinfo=UTC)], [Decimal(1)])
    catalog, con = base.catalog, base.con
    con.create_table(
        "users",
        pa.table(
            {
                "user_id": ["a", "b"],
                "observed_from": [
                    datetime(2025, 1, 3, tzinfo=UTC),
                    datetime(2025, 1, 1, tzinfo=UTC),
                ],
                "observed_until": [
                    datetime(2025, 1, 10, tzinfo=UTC),
                    datetime(2025, 1, 3, tzinfo=UTC),
                ],
            }
        ),
        overwrite=True,
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={
            "user": UnitSource(
                "users", "user_id", observed_from="observed_from", observed_until="observed_until"
            )
        },
    )
    design = Design(
        "user",
        Schedule(every, "2025-01-03", "2025-01-03"),
        shape=StatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("none", "some")),
            periods=2 if every == "12h" else 1,
        ),
    )
    rows = (
        design.build(model)
        .materialize(budget=Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    by_unit = {row["unit"]: row for row in rows if row["period_index"] == 0}
    assert by_unit["a"]["from_state__complete"] is False
    assert by_unit["a"]["to_state__complete"] is True
    assert by_unit["b"]["from_state__complete"] is True
    assert by_unit["b"]["to_state__complete"] is False


@pytest.mark.parametrize(("overflow", "failed_count"), [("all", 2), ("endpoint", 1)])
def test_binned_exact_overflow_retains_scheduled_and_endpoint_check_domains(
    native_inputs, overflow, failed_count
):
    maximum = Decimal("9" * 38)
    values = [maximum if overflow == "all" else Decimal(0)] * 4 + [maximum] * 2
    times = [datetime(2025, 1, day, hour, tzinfo=UTC) for day in (2, 3, 4) for hour in (1, 2)]
    model = _sum_model(native_inputs, times, values)
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        shape=StatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("low", "high")),
            periods=2,
        ),
    )
    with pytest.raises(TerrineError) as error:
        design.build(model).materialize(budget=Budget(max_rows=10, max_output_bytes=100_000))
    assert error.value.code == "terrine.temporal.exact_overflow"
    assert error.value.context["failed_count"] == failed_count
    assert error.value.context["source"] == "events"
    assert error.value.context["measure"] == "amount"


def test_binned_native_float_multiday_endpoint_keeps_direct_cancellation(native_inputs):
    import sys

    positive = 0.75 * sys.float_info.max
    model = _sum_model(
        native_inputs,
        [
            datetime(2025, 1, 5, 1, tzinfo=UTC),
            datetime(2025, 1, 6, 1, tzinfo=UTC),
            datetime(2025, 1, 5, 2, tzinfo=UTC),
            datetime(2025, 1, 6, 2, tzinfo=UTC),
        ],
        [positive, -positive, positive, -positive],
    )
    design = Design(
        "user",
        Schedule("2d", "2025-01-03", "2025-01-06"),
        shape=StatePanel(
            BinnedMetricState("total", cutpoints=(0.0,), levels=("low", "high")),
            periods=2,
        ),
    )
    rows = (
        design.build(model)
        .materialize(budget=Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    final = next(row for row in rows if row["unit"] == "a" and row["period_index"] == 1)
    assert final["to_state"] == "low"
    assert final["to_state__complete"] is True
    assert final["to_state__reason"] is None


def test_binned_endpoint_at_last_calendar_day_keeps_original_relation(native_inputs):
    model = _sum_model(
        native_inputs,
        [datetime(9999, 12, 29, 12, tzinfo=UTC)],
        [Decimal(1)],
        coverage=("9999-12-27", "9999-12-30"),
    )
    design = Design(
        "user",
        Schedule("1d", "9999-12-29", "9999-12-30"),
        shape=StatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("none", "some")),
            periods=2,
        ),
    )
    rows = (
        design.build(model)
        .materialize(budget=Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    actual = sorted(
        (row["anchor_id"], row["from_state"], row["to_state"], row["to_time"])
        for row in rows
        if row["unit"] == "a"
    )
    assert actual == [
        ("schedule:0", "none", "some", datetime(9999, 12, 30, tzinfo=UTC)),
        ("schedule:1", "some", "none", datetime(9999, 12, 31, tzinfo=UTC)),
    ]


@pytest.mark.parametrize(
    ("budget", "code"),
    [(Budget(max_rows=10, max_output_bytes=1), "terrine.budget.output_bytes_exceeded")],
)
def test_state_panel_enforces_output_byte_budget(panel_model, budget, code):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            periods=2,
        ),
    )
    with pytest.raises(TerrineError) as error:
        design.build(panel_model).materialize(budget=budget)
    assert error.value.code == code


def test_terminal_detection_precedes_output_eligibility_filter(observed_model_factory):
    from terrine.spec import StateObservations, gt

    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 4, tzinfo=UTC), "closed"),
        ]
    )
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        eligibility={"history": gt(agg("sessions", window=("-1d", "0d")), 0)},
        shape=StatePanel(
            StateObservations("state", ("active", "closed"), absorbing=("closed",)),
            periods=2,
        ),
    )
    rows = (
        design.build(model)
        .materialize(budget=Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    assert not [row for row in rows if row["unit"] == "a"]


def test_terminal_at_origin_suppresses_and_source_correction_rebuilds_suffix(
    observed_model_factory,
):
    model = observed_model_factory([("a", datetime(2025, 1, 3, tzinfo=UTC), "closed")])
    rows = (
        _observed_design()
        .build(model)
        .materialize(budget=Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    assert not any(row["unit"] == "a" for row in rows)

    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 4, tzinfo=UTC), "closed"),
        ]
    )
    closed_rows = (
        _observed_design()
        .build(model)
        .materialize(budget=Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    assert len([row for row in closed_rows if row["unit"] == "a"]) == 1

    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 5, tzinfo=UTC), "active"),
        ]
    )
    corrected_rows = (
        _observed_design(absorbing=())
        .build(model)
        .materialize(budget=Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    assert len([row for row in corrected_rows if row["unit"] == "a"]) == 2
