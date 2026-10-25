"""State panel tests moved for fixed compiler overhead."""

from datetime import UTC, datetime

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
        Schedule("1d", "2025-01-03", "2025-01-05"),
        features={"origin": agg("sessions", window=("-1d", "0d"))},
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            baseline_features={"baseline": agg("sessions", window=("-1d", "0d"))},
            max_periods=2,
        ),
    )
    materialized = design.build(model).materialize(Budget(max_rows=10, max_output_bytes=100_000))
    restored = type(materialized).from_arrow(materialized.to_arrow())
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
    definition["sources"]["events"]["facts"] = ("session", "score")
    definition["facts"]["score"] = {"source": "events", "column": "score"}
    definition["measures"]["score_mean"] = {"fact": "score", "aggregation": "avg_event"}
    definition["metrics"]["score_avg"] = {
        "kind": "mean",
        "entity": "user",
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
        Schedule("1d", "2025-01-03", "2025-01-04"),
        shape=StatePanel(
            BinnedMetricState("score_avg", cutpoints=(cut,), levels=("low", "high")),
            max_periods=1,
        ),
    )
    rows = (
        design.build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    row = next(row for row in rows if row["unit"] == "a")
    assert row["to_state"] == "low"
    assert row["to_state_reason"] is None


def test_binned_panel_retains_named_eligibility_column(panel_model):
    from terrine.compile.dispatch import compile_design
    from terrine.spec import gt

    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-05"),
        eligibility={"has_history": gt(agg("sessions", window=("-1d", "0d")), 0)},
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            max_periods=2,
        ),
    )
    compiled = compile_design(design, panel_model)
    assert "has_history" in compiled.audit_table.columns
    materialized = design.build(panel_model).materialize(
        Budget(max_rows=10, max_output_bytes=100_000)
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
        Schedule("1d", "2025-01-03", "2025-01-05"),
        eligibility={"has_history": gt(agg("sessions", window=("-1d", "0d")), 0)},
        shape=StatePanel(
            StateObservations("state", ("active", "closed")),
            max_periods=2,
        ),
    )
    compiled = compile_design(design, model)
    assert "has_history" in compiled.audit_table.columns
    materialized = design.build(model).materialize(Budget(max_rows=10, max_output_bytes=100_000))
    assert materialized.table["has_history"].to_pylist() == [True, True]


@pytest.mark.parametrize(
    ("budget", "code"),
    [(Budget(max_rows=1, max_output_bytes=100_000), "terrine.budget.rows_exceeded")],
)
def test_state_panel_enforces_output_row_and_byte_budgets(panel_model, budget, code):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-05"),
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            max_periods=2,
        ),
    )
    with pytest.raises(TerrineError) as error:
        design.build(panel_model).materialize(budget)
    assert error.value.code == code


def test_adjacent_binned_state_relation_pairs_response_with_successor_prior(panel_model):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-05"),
        features={"past_sessions": agg("sessions", window=("-1d", "0d"))},
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            max_periods=2,
        ),
    )
    rows = (
        design.build(panel_model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    a_rows = sorted(
        (row for row in rows if row["unit"] == "a"), key=lambda row: row["period_index"]
    )
    assert len(a_rows) == 2
    assert a_rows[0]["period_index"] == 0
    assert a_rows[1]["period_index"] == 1
    assert a_rows[0]["to_state"] == "some"
    assert a_rows[0]["to_state"] == a_rows[1]["from_state"]
    assert a_rows[0]["to_time"] == datetime(2025, 1, 4, tzinfo=UTC)
    assert a_rows[0]["scheduled_to"] == a_rows[0]["to_time"]


def test_binned_response_remains_complete_at_membership_exit(native_inputs):
    import pyarrow as pa

    catalog, con = native_inputs
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
        Schedule("1d", "2025-01-03", "2025-01-04"),
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            max_periods=1,
        ),
    )
    rows = (
        design.build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    row = next(row for row in rows if row["unit"] == "a")
    assert row["to_state"] == "some"
    assert row["to_state_complete"] is True


def test_binned_missing_period_sides_and_unit_end_exclusivity(native_inputs):
    import pyarrow as pa

    catalog, con = native_inputs
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
        Schedule("1d", "2025-01-03", "2025-01-04"),
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            max_periods=1,
        ),
    )
    rows = (
        design.build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    by_unit = {row["unit"]: row for row in rows}
    assert by_unit["a"]["from_state_complete"] is False
    assert by_unit["a"]["to_state_complete"] is True
    assert by_unit["b"]["from_state_complete"] is True
    assert by_unit["b"]["to_state_complete"] is False


@pytest.mark.parametrize(
    ("budget", "code"),
    [(Budget(max_rows=10, max_output_bytes=1), "terrine.budget.output_bytes_exceeded")],
)
def test_state_panel_enforces_output_byte_budget(panel_model, budget, code):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-05"),
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            max_periods=2,
        ),
    )
    with pytest.raises(TerrineError) as error:
        design.build(panel_model).materialize(budget)
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
        Schedule("1d", "2025-01-03", "2025-01-05"),
        eligibility={"history": gt(agg("sessions", window=("-1d", "0d")), 0)},
        shape=StatePanel(
            StateObservations("state", ("active", "closed"), absorbing=("closed",)),
            max_periods=2,
        ),
    )
    rows = (
        design.build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
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
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
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
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
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
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    assert len([row for row in corrected_rows if row["unit"] == "a"]) == 2
