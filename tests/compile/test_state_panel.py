from datetime import UTC, datetime
from decimal import Decimal

import pytest

from terrine.data import Budget, Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import BinnedMetricState, Design, Schedule, StateObservations, StatePanel, agg


@pytest.fixture
def panel_model(native_inputs):
    catalog, con = native_inputs
    return Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
    )


@pytest.fixture
def observed_panel_model(native_inputs):
    from datetime import UTC, datetime

    import pyarrow as pa

    from terrine.semantic.models import Catalog

    catalog, con = native_inputs
    con.create_table(
        "states",
        pa.table(
            {
                "user_id": ["a", "a", "b"],
                "ts": [
                    datetime(2025, 1, 3, tzinfo=UTC),
                    datetime(2025, 1, 4, tzinfo=UTC),
                    datetime(2025, 1, 3, tzinfo=UTC),
                ],
                "state": ["active", "closed", "active"],
            }
        ),
    )
    definition = catalog.model_dump(mode="python")
    definition["sources"]["states"] = {
        "relation": {"kind": "table", "name": "states"},
        "timestamp": "ts",
        "entities": {"user": "user_id"},
        "facts": [],
        "properties": ["state"],
        "dimensions": [],
    }
    definition["properties"]["state"] = {
        "source": "states",
        "column": "state",
        "dtype": "string",
        "as_of": "event_time",
    }
    return Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness={
            "events": Coverage("2025-01-01", "2025-01-31"),
            "states": Coverage("2025-01-01", "2025-01-31"),
        },
        units={"user": UnitSource("users", "user_id")},
    )


def test_observed_state_panel_compiles_point_lookup_contract(observed_panel_model):
    from terrine.spec import StateObservations

    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-05"),
        shape=StatePanel(
            StateObservations("state", ("active", "closed"), absorbing=("closed",)),
            max_periods=2,
        ),
    )
    from terrine.compile.dispatch import compile_design

    compiled = compile_design(design, observed_panel_model)
    assert compiled.roles.prior_state == "from_state"
    assert compiled.roles.outcomes == ("to_state",)
    assert compiled.strategy["from_state"] == "point_observation"
    assert compiled.table.columns[:8] == (
        "unit",
        "anchor_id",
        "t0",
        "from_time",
        "to_time",
        "scheduled_to",
        "period_index",
        "gap",
    )


@pytest.fixture
def observed_model_factory(native_inputs):
    import pyarrow as pa

    from terrine.semantic.models import Catalog

    catalog, con = native_inputs
    definition = catalog.model_dump(mode="python")
    definition["sources"]["states"] = {
        "relation": {"kind": "table", "name": "states"},
        "timestamp": "ts",
        "entities": {"user": "user_id"},
        "facts": [],
        "properties": ["state"],
        "dimensions": [],
    }
    definition["properties"]["state"] = {
        "source": "states",
        "column": "state",
        "dtype": "string",
        "as_of": "event_time",
    }

    def build(
        observations,
        *,
        entered_at=None,
        observed_from=None,
        observed_until=None,
        coverage_end="2025-01-31",
    ):
        for table_name in ("states", "users"):
            if table_name in con.list_tables():
                con.drop_table(table_name)
        con.create_table(
            "states",
            pa.table(
                {
                    "user_id": [row[0] for row in observations],
                    "ts": [row[1] for row in observations],
                    "state": [row[2] for row in observations],
                }
            ),
        )
        users = {"user_id": ["a", "b"]}
        unit_options = {}
        if entered_at is not None:
            users["entered_at"] = entered_at
            unit_options["entered_at"] = "entered_at"
        if observed_from is not None:
            users["observed_from"] = observed_from
            unit_options["observed_from"] = "observed_from"
        if observed_until is not None:
            users["observed_until"] = observed_until
            unit_options["observed_until"] = "observed_until"
        con.create_table("users", pa.table(users))
        return Model.from_catalog(
            Catalog.model_validate(definition),
            con=con,
            readiness={
                "events": Coverage("2025-01-01", "2025-01-31"),
                "states": Coverage("2025-01-01", coverage_end),
            },
            units={"user": UnitSource("users", "user_id", **unit_options)},
        )

    return build


def test_binned_state_panel_compiles_bounded_period_pairing(panel_model):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-05"),
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            max_periods=2,
        ),
    )
    from terrine.compile.dispatch import compile_design

    compiled = compile_design(design, panel_model)
    assert compiled.roles.prior_state == "from_state"
    assert compiled.roles.outcomes == ("to_state",)
    assert compiled.roles.cluster_unit == "unit"
    assert compiled.table.columns[:8] == (
        "unit",
        "anchor_id",
        "t0",
        "from_time",
        "to_time",
        "scheduled_to",
        "period_index",
        "gap",
    )


def test_panel_baseline_and_per_origin_feature_cutoffs_are_distinct(panel_model):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-05"),
        features={"per_origin": agg("sessions", window=("-1d", "0d"))},
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            max_periods=2,
            baseline_features={"baseline": agg("sessions", window=("-1d", "0d"))},
        ),
    )
    from terrine.compile.dispatch import compile_design

    compiled = compile_design(design, panel_model)
    assert compiled.feature_cutoffs["per_origin"].reference == "row_t0"
    assert compiled.feature_cutoffs["baseline"].reference == "panel_start"


@pytest.mark.slow
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


@pytest.mark.slow
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


@pytest.mark.slow
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


@pytest.mark.slow
@pytest.mark.parametrize(
    ("terminal_time", "range_start"),
    [
        (datetime(2025, 1, 2, tzinfo=UTC), None),
        (datetime(2025, 1, 4, tzinfo=UTC), "2025-01-04"),
    ],
)
def test_terminal_is_found_before_panel_or_anchor_range_filter(
    observed_model_factory, terminal_time, range_start
):
    from terrine.spec import AnchorRange

    model = observed_model_factory([("a", terminal_time, "closed")])
    design = _observed_design()
    if range_start is not None:
        design = Design(
            design.unit,
            design.anchor,
            shape=design.shape,
            anchor_range=AnchorRange(start=range_start),
        )
    rows = (
        design.build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    assert not [row for row in rows if row["unit"] == "a"]


@pytest.mark.slow
@pytest.mark.parametrize(
    "observations",
    [
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
        ],
        [
            (None, datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("b", datetime(2025, 1, 4, tzinfo=UTC), "active"),
        ],
        [
            ("a", None, "active"),
            ("b", datetime(2025, 1, 4, tzinfo=UTC), "active"),
        ],
        [("a", datetime(2025, 1, 3, tzinfo=UTC), "unknown")],
    ],
)
def test_observation_key_and_level_integrity_refuses(observed_model_factory, observations):
    model = observed_model_factory(observations)
    with pytest.raises(TerrineError) as error:
        _observed_design().build(model).materialize(Budget(max_rows=10, max_output_bytes=100_000))
    assert error.value.code == "terrine.state_panel.invalid_observations"


def test_binned_state_periods_ignore_covariate_gap(panel_model):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-05"),
        gap="1d",
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
    assert rows


@pytest.mark.parametrize(
    "name",
    [
        "from_state_complete",
        "from_state_reason",
        "to_state_complete",
        "to_state_reason",
        "from_state_observed_from",
        "from_state_observed_until",
        "to_state_observed_from",
        "to_state_observed_until",
    ],
)
def test_state_panel_rejects_generated_status_name_collisions(panel_model, name):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        features={name: agg("sessions", window=("-1d", "0d"))},
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            max_periods=1,
        ),
    )
    with pytest.raises(TerrineError) as error:
        design.build(panel_model)
    assert error.value.code == "terrine.spec.reserved_output_name"


@pytest.mark.slow
def test_known_null_after_absorption_does_not_count_as_recovery(observed_model_factory):
    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 4, tzinfo=UTC), "closed"),
            ("a", datetime(2025, 1, 5, tzinfo=UTC), None),
        ]
    )
    rows = (
        _observed_design()
        .build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    assert len([row for row in rows if row["unit"] == "a"]) == 1


def test_float_bin_certificate_rounds_upper_endpoint_outward():
    import math

    import ibis
    import pyarrow as pa

    from terrine.compile.shapes.state_panel import _label
    from terrine.data import ColumnMeta

    cut = float.fromhex("0x1.0000000000001p+0")
    state = BinnedMetricState("sessions", (cut,), ("low", "high"))
    meta = ColumnMeta(
        None,
        "state",
        None,
        None,
        None,
        None,
        numeric_policy="checked_float64_v1",
        absolute_error_column="error",
    )
    relation = ibis.memtable(
        pa.table(
            {
                "_state_period": pa.array([1.0]),
                "error": pa.array([2.2204460492503136e-16]),
            }
        )
    )
    con = ibis.duckdb.connect()
    try:
        value = con.execute(relation.select(label=_label(relation, state, meta)))["label"].iloc[0]
        assert math.isnan(value)
    finally:
        con.disconnect()


@pytest.mark.parametrize("kind", ["partial", "outcome", "treatment"])
def test_state_panel_refuses_partial_schedule_and_unsupported_designs(panel_model, kind):
    from terrine.spec import Treatment

    panel = StatePanel(
        BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
        max_periods=2,
    )
    schedule = (
        Schedule("36h", "2025-01-03", "2025-01-05")
        if kind == "partial"
        else Schedule("1d", "2025-01-03", "2025-01-05")
    )
    if kind == "outcome":
        design = Design(
            "user", schedule, outcomes={"future": agg("churned", window=("0d", "1d"))}, shape=panel
        )
    elif kind == "treatment":
        design = Design("user", schedule, treatment=Treatment("enabled"), shape=panel)
    else:
        design = Design("user", schedule, shape=panel)
    with pytest.raises(TerrineError) as error:
        design.validate(panel_model)
    expected = (
        "terrine.state_panel.invalid_schedule"
        if kind == "partial"
        else "terrine.state_panel.unsupported_combination"
    )
    assert error.value.code == expected


@pytest.mark.slow
@pytest.mark.parametrize(
    ("cut", "ambiguous"),
    [
        (float.fromhex("0x1.0000000000001p+0"), True),
        (Decimal("0.1"), False),
    ],
)
def test_float_metric_cut_precision_and_near_boundary(native_inputs, cut, ambiguous):
    import pyarrow as pa

    from terrine.semantic.models import Catalog

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
    if not ambiguous:
        design = Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-04"),
            shape=StatePanel(
                BinnedMetricState("score_avg", cutpoints=(cut,), levels=("low", "high")),
                max_periods=1,
            ),
        )
        with pytest.raises(TerrineError) as error:
            design.build(model)
        assert error.value.code == "terrine.state_panel.unsupported_combination"
        return
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
    assert row["to_state"] is None
    assert row["to_state_reason"] == "ambiguous_state_bin"


@pytest.mark.slow
def test_binned_state_preserves_metric_null_reasons():
    import ibis
    import pyarrow as pa

    from terrine.compile.shapes.state_panel import _state_value
    from terrine.data import ColumnMeta

    state = BinnedMetricState("sessions", (0,), ("none", "some"))
    meta = ColumnMeta(None, "state", None, None, None, None)
    relation = ibis.memtable(
        pa.table(
            {
                "_state_period": pa.array([None] * 4, type=pa.float64()),
                "__terrine_state_complete": pa.array([True] * 4),
                "__terrine_state_reason": pa.array(
                    ["all_values_null", "no_observations", "zero_denominator", None],
                    type=pa.string(),
                ),
            }
        )
    )
    value, complete, reason = _state_value(relation, state, meta)
    con = ibis.duckdb.connect()
    try:
        result = con.execute(relation.select(value=value, complete=complete, reason=reason))
    finally:
        con.disconnect()
    assert result["value"].isna().all()
    assert not result["complete"].any()
    assert result["reason"].tolist() == [
        "all_values_null",
        "no_observations",
        "zero_denominator",
        "state_value_null",
    ]


@pytest.mark.slow
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


@pytest.mark.slow
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


def test_float_metric_rejects_integer_cutpoint_outside_float64_range():
    import ibis.expr.datatypes as dt

    from terrine.compile.shapes.state_panel import _cut_literal

    with pytest.raises(TerrineError) as error:
        _cut_literal(10**400, dt.float64)
    assert error.value.code == "terrine.state_panel.unsupported_combination"


def test_observed_state_property_must_be_source_owned(panel_model):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-05"),
        shape=StatePanel(
            StateObservations("not_a_property", ("active", "closed")),
            max_periods=2,
        ),
    )
    with pytest.raises(TerrineError) as error:
        design.validate(panel_model)
    assert error.value.code == "terrine.state_panel.invalid_state_source"


def test_state_panel_rejects_schedule_over_cap_with_owned_refusal(panel_model):
    design = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-06"),
        shape=StatePanel(
            BinnedMetricState("sessions", cutpoints=(0,), levels=("none", "some")),
            max_periods=2,
        ),
    )
    with pytest.raises(TerrineError) as error:
        design.validate(panel_model)
    assert error.value.code == "terrine.state_panel.invalid_schedule"


@pytest.mark.parametrize(
    "budget",
    [
        Budget(max_rows=10),
        Budget(max_rows=0, max_output_bytes=100_000),
        Budget(max_rows=10, max_output_bytes=0),
        Budget(max_output_bytes=100_000),
    ],
)
def test_state_panel_materialization_requires_positive_row_and_byte_budgets(panel_model, budget):
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
    assert error.value.code == "terrine.state_panel.budget_required"


@pytest.mark.slow
@pytest.mark.parametrize(
    ("budget", "code"),
    [
        (Budget(max_rows=1, max_output_bytes=100_000), "terrine.budget.rows_exceeded"),
        (Budget(max_rows=10, max_output_bytes=1), "terrine.budget.output_bytes_exceeded"),
    ],
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


@pytest.mark.slow
def test_observed_state_panel_is_invariant_to_source_row_order(observed_model_factory):
    observations = [
        ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
        ("a", datetime(2025, 1, 4, tzinfo=UTC), "closed"),
        ("b", datetime(2025, 1, 3, tzinfo=UTC), "active"),
    ]
    results = []
    for rows in (observations, list(reversed(observations))):
        model = observed_model_factory(rows)
        panel = (
            _observed_design()
            .build(model)
            .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        )
        results.append(
            sorted(
                (
                    row["unit"],
                    row["period_index"],
                    row["from_state"],
                    row["to_state"],
                    row["to_time"],
                    row["terminal"],
                )
                for row in panel.table.to_pylist()
            )
        )
    assert results[0] == results[1]


@pytest.mark.slow
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


def _observed_design(*, absorbing=("closed",)):
    from terrine.spec import StateObservations

    return Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-05"),
        shape=StatePanel(
            StateObservations("state", ("active", "closed"), absorbing=absorbing),
            max_periods=2,
        ),
    )


@pytest.mark.slow
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


@pytest.mark.slow
def test_missing_observations_are_not_bridged_across_adjacent_rows(observed_model_factory):
    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 5, tzinfo=UTC), "active"),
        ]
    )
    rows = (
        _observed_design(absorbing=())
        .build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    a_rows = sorted(
        (row for row in rows if row["unit"] == "a"), key=lambda row: row["period_index"]
    )
    assert len(a_rows) == 2
    assert a_rows[0]["from_state"] == "active"
    assert a_rows[0]["to_state"] is None
    assert a_rows[0]["to_state_complete"] is False
    assert a_rows[0]["to_state_reason"] == "missing_state_observation"
    assert a_rows[1]["from_state"] is None
    assert a_rows[1]["from_state_complete"] is False
    assert a_rows[1]["from_state_reason"] == "missing_state_observation"
    assert a_rows[1]["to_state"] == "active"


@pytest.mark.slow
def test_known_null_observation_is_complete_but_not_a_state_label(observed_model_factory):
    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 4, tzinfo=UTC), None),
            ("a", datetime(2025, 1, 5, tzinfo=UTC), "active"),
        ]
    )
    rows = (
        _observed_design(absorbing=())
        .build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    first = next(row for row in rows if row["unit"] == "a" and row["period_index"] == 0)
    second = next(row for row in rows if row["unit"] == "a" and row["period_index"] == 1)
    assert first["to_state"] is None
    assert first["to_state_complete"] is True
    assert first["to_state_reason"] == "state_value_null"
    assert second["from_state"] is None
    assert second["from_state_complete"] is True
    assert second["from_state_reason"] == "state_value_null"


@pytest.mark.slow
def test_terminal_boundary_emits_incoming_transition_and_suppresses_suffix(observed_model_factory):
    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 5, tzinfo=UTC), "closed"),
        ]
    )
    rows = (
        _observed_design()
        .build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    a_rows = [row for row in rows if row["unit"] == "a"]
    assert len(a_rows) == 2
    terminal_row = next(row for row in a_rows if row["period_index"] == 1)
    assert terminal_row["terminal"] is True
    assert terminal_row["to_time"] == datetime(2025, 1, 5, tzinfo=UTC)


@pytest.mark.slow
def test_usable_state_recovery_after_absorption_refuses(observed_model_factory):
    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "closed"),
            ("a", datetime(2025, 1, 4, tzinfo=UTC), "active"),
        ]
    )
    with pytest.raises(TerrineError) as error:
        _observed_design().build(model).materialize(Budget(max_rows=10, max_output_bytes=100_000))
    assert error.value.code == "terrine.state_panel.invalid_observations"


@pytest.mark.slow
def test_off_grid_terminal_is_allowed_but_nonterminal_refuses(observed_model_factory):
    from datetime import timedelta

    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 4, 12, tzinfo=UTC), "closed"),
        ]
    )
    rows = (
        _observed_design()
        .build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    terminal = next(row for row in rows if row["unit"] == "a" and row["terminal"])
    assert terminal["to_time"] == datetime(2025, 1, 4, 12, tzinfo=UTC)
    assert terminal["gap"] == timedelta(hours=12)

    model = observed_model_factory([("a", datetime(2025, 1, 3, 12, tzinfo=UTC), "active")])
    with pytest.raises(TerrineError) as error:
        _observed_design(absorbing=()).build(model).materialize(
            Budget(max_rows=10, max_output_bytes=100_000)
        )
    assert error.value.code == "terrine.state_panel.invalid_observations"


@pytest.mark.slow
def test_delayed_entry_and_observation_bounds_do_not_shift_or_override_history(
    observed_model_factory,
):
    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 4, tzinfo=UTC), "active"),
        ],
        entered_at=[
            datetime(2025, 1, 4, tzinfo=UTC),
            datetime(2025, 1, 3, tzinfo=UTC),
        ],
    )
    rows = (
        _observed_design()
        .build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    a_rows = [row for row in rows if row["unit"] == "a"]
    assert [row["period_index"] for row in a_rows] == [1]
    assert a_rows[0]["from_state"] == "active"
    assert a_rows[0]["terminal"] is False

    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 4, tzinfo=UTC), "closed"),
        ],
        observed_from=[
            datetime(2025, 1, 5, tzinfo=UTC),
            datetime(2025, 1, 3, tzinfo=UTC),
        ],
    )
    rows = (
        _observed_design()
        .build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    a_rows = [row for row in rows if row["unit"] == "a"]
    assert len(a_rows) == 2
    assert all(not row["terminal"] for row in a_rows)
    assert a_rows[0]["to_state_reason"] == "incomplete_observation"


@pytest.mark.slow
def test_unit_observed_until_is_exclusive_for_point_states(observed_model_factory):
    model = observed_model_factory(
        [
            ("b", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("b", datetime(2025, 1, 4, tzinfo=UTC), "closed"),
        ],
        observed_until=[
            datetime(2025, 1, 31, tzinfo=UTC),
            datetime(2025, 1, 4, tzinfo=UTC),
        ],
    )
    rows = (
        _observed_design()
        .build(model)
        .materialize(Budget(max_rows=10, max_output_bytes=100_000))
        .table.to_pylist()
    )
    first = next(row for row in rows if row["unit"] == "b" and row["period_index"] == 0)
    assert first["from_state"] == "active"
    assert first["to_state"] is None
    assert first["to_state_reason"] == "incomplete_observation"


@pytest.mark.slow
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


@pytest.mark.slow
def test_exclusive_source_coverage_end_is_unavailable_not_absorbing(observed_model_factory):
    model = observed_model_factory(
        [
            ("a", datetime(2025, 1, 3, tzinfo=UTC), "active"),
            ("a", datetime(2025, 1, 4, tzinfo=UTC), "closed"),
        ],
        coverage_end="2025-01-03",
    )
    materialized = (
        _observed_design().build(model).materialize(Budget(max_rows=10, max_output_bytes=100_000))
    )
    a_rows = [row for row in materialized.table.to_pylist() if row["unit"] == "a"]
    assert len(a_rows) == 2
    assert not any(row["terminal"] for row in a_rows)
    assert a_rows[0]["to_state_reason"] == "incomplete_coverage"
    assert a_rows[1]["from_state_reason"] == "incomplete_coverage"
    metadata = materialized.provenance.state_panel
    assert metadata is not None
    assert metadata.levels == ("active", "closed")
    assert metadata.state_source["point_time_columns"] == {
        "from_state": "from_time",
        "to_state": "to_time",
    }


def test_binned_classifier_preserves_huge_decimal_cuts_and_lower_equality():
    from decimal import Decimal

    import ibis
    import pyarrow as pa

    from terrine.compile.shapes.state_panel import _label
    from terrine.data import ColumnMeta

    lower = Decimal(2**80)
    upper = lower + Decimal(1)
    state = BinnedMetricState("sessions", (lower, upper), ("low", "mid", "high"))
    meta = ColumnMeta(None, "state", None, None, None, None, numeric_policy="exact")
    values = pa.array([lower, upper, upper + 1], type=pa.decimal128(30, 0))
    relation = ibis.memtable(pa.table({"_state_period": values}))
    labels = relation.select(label=_label(relation, state, meta))
    con = ibis.duckdb.connect()
    try:
        assert con.execute(labels)["label"].tolist() == ["low", "mid", "high"]
    finally:
        con.disconnect()


def test_binned_classifier_refuses_a_float_certificate_spanning_a_cut():
    import math

    import ibis
    import pyarrow as pa

    from terrine.compile.shapes.state_panel import _state_value
    from terrine.data import ColumnMeta

    state = BinnedMetricState("sessions", (0.0,), ("low", "high"))
    meta = ColumnMeta(
        None,
        "state",
        None,
        None,
        "__terrine_state_complete",
        "__terrine_state_reason",
        numeric_policy="checked_float64_v1",
        absolute_error_column="error",
    )
    relation = ibis.memtable(
        pa.table(
            {
                "_state_period": pa.array([0.0]),
                "error": pa.array([0.01]),
                "__terrine_state_complete": pa.array([True]),
                "__terrine_state_reason": pa.array([None], type=pa.string()),
            }
        )
    )
    value, complete, reason = _state_value(relation, state, meta)
    classified = relation.select(value=value, complete=complete, reason=reason)
    con = ibis.duckdb.connect()
    try:
        result = con.execute(classified).iloc[0]
        assert math.isnan(result["value"])
        assert not result["complete"]
        assert result["reason"] == "ambiguous_state_bin"
    finally:
        con.disconnect()
