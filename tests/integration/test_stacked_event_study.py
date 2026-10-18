from datetime import UTC, datetime

import pyarrow as pa

from terrine.data import Budget, Coverage, MaterializedFrame, UnitSource
from terrine.model import Model
from terrine.spec import (
    CalendarComparisonPanel,
    Design,
    Initiation,
    Schedule,
    agg,
)


def test_calendar_comparison_materializes_roster_controls_for_both_modes(native_inputs):
    catalog, con = native_inputs
    con.insert("users", pa.table({"user_id": ["c", "d", "e"], "alt_id": ["cc", "dd", "ee"]}))
    con.insert(
        "events",
        pa.table(
            {
                "user_id": ["d", "e"],
                "ts": [
                    datetime(2025, 1, 8, tzinfo=UTC),
                    datetime(2025, 1, 4, tzinfo=UTC),
                ],
                "event_id": ["d1", "e1"],
                "alternate_event_id": ["d2", "e2"],
            }
        ),
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    base = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-12"),
        outcomes={"y": agg("sessions", window=("0d", "1d"))},
    )
    cohort = Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC))
    frames = {
        mode: CalendarComparisonPanel(
            base,
            model,
            cohort=cohort,
            control=mode,
            reference_period=-1,
            comparison_periods=(0, 1),
        ).materialize(Budget(max_rows=200, max_output_bytes=1_000_000))
        for mode in ("never_treated", "not_yet_treated")
    }

    never = frames["never_treated"]
    not_yet = frames["not_yet_treated"]
    assert never.provenance.shape == not_yet.provenance.shape == "calendar_comparison"
    assert never.provenance.calendar_comparison is not None
    assert not_yet.provenance.calendar_comparison is not None
    assert never.provenance.calendar_comparison.control == "never_treated"
    assert not_yet.provenance.calendar_comparison.control == "not_yet_treated"
    assert never.provenance.design_hash != not_yet.provenance.design_hash
    never_rows = never.table.to_pylist()
    not_yet_rows = not_yet.table.to_pylist()
    early_never = [row for row in never_rows if row["cohort"] == datetime(2025, 1, 3, tzinfo=UTC)]
    early_not_yet = [
        row for row in not_yet_rows if row["cohort"] == datetime(2025, 1, 3, tzinfo=UTC)
    ]
    assert {row["unit"] for row in early_never if not row["treatment"]} == {"c"}
    assert {
        row["unit"] for row in early_not_yet if row["event_period"] == 0 and not row["treatment"]
    } == {"c", "d", "e"}
    assert all(row["event_period"] in (-1, 0, 1) for row in not_yet_rows)
    restored = MaterializedFrame.from_arrow(not_yet.to_arrow())
    assert restored.provenance.calendar_comparison == not_yet.provenance.calendar_comparison
    assert restored.table.equals(not_yet.table)


def test_never_treated_history_accepts_observation_through_schedule_end(native_inputs):
    catalog, con = native_inputs
    con.insert("users", pa.table({"user_id": ["c"], "alt_id": ["cc"]}))
    con.raw_sql("ALTER TABLE users ADD COLUMN observed_until TIMESTAMPTZ")
    con.raw_sql(
        "UPDATE users SET observed_until = TIMESTAMPTZ '2025-01-12 00:00:00+00' WHERE user_id = 'c'"
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id", observed_until="observed_until")},
        event_ids={"events": "event_id"},
    )
    frame = CalendarComparisonPanel(
        Design("user", Schedule("1d", "2025-01-01", "2025-01-12")),
        model,
        cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
        control="never_treated",
        reference_period=-1,
        comparison_periods=(0,),
    ).materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
    assert frame.table.num_rows > 0


def test_calendar_comparison_excludes_ineligible_units(native_inputs):
    from terrine.spec import gt

    catalog, con = native_inputs
    con.insert("users", pa.table({"user_id": ["c"], "alt_id": ["cc"]}))
    con.raw_sql("ALTER TABLE users ADD COLUMN observed_from TIMESTAMPTZ")
    con.raw_sql(
        "UPDATE users SET observed_from = TIMESTAMPTZ '2024-12-31 00:00:00+00' WHERE user_id = 'c'"
    )
    con.insert(
        "events",
        pa.table(
            {
                "user_id": ["a", "a", "a"],
                "ts": [
                    datetime(2024, 12, 31, tzinfo=UTC),
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 3, tzinfo=UTC),
                ],
                "event_id": ["a0", "a1", "a2"],
                "alternate_event_id": ["a0-alt", "a1-alt", "a2-alt"],
            }
        ),
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={
            "events": Coverage("2024-12-29", "2025-01-31"),
            "labels": Coverage("2024-12-29", "2025-01-31"),
        },
        units={"user": UnitSource("users", "user_id", observed_from="observed_from")},
        event_ids={"events": "event_id"},
    )
    result = CalendarComparisonPanel(
        Design(
            "user",
            Schedule("1d", "2025-01-02", "2025-01-04"),
            gap="1d",
            eligibility={"eligible": gt(agg("sessions", window=("-2d", "-1d")), 0)},
        ),
        model,
        cohort=Initiation("labels", "churn", datetime(2024, 12, 30, tzinfo=UTC)),
        control="never_treated",
        reference_period=-1,
        comparison_periods=(0,),
    ).materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
    assert {row["unit"] for row in result.table.to_pylist()} == {"a"}


def test_calendar_comparison_controls_have_explicit_boolean_labels(native_inputs):
    catalog, con = native_inputs
    con.insert("users", pa.table({"user_id": ["c", "d"], "alt_id": ["cc", "dd"]}))
    con.insert(
        "events",
        pa.table(
            {
                "user_id": ["d"],
                "ts": [datetime(2025, 1, 8, tzinfo=UTC)],
                "event_id": ["d1"],
                "alternate_event_id": ["d2"],
            }
        ),
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    rows = (
        CalendarComparisonPanel(
            Design("user", Schedule("1d", "2025-01-01", "2025-01-12")),
            model,
            cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
            control="never_treated",
            reference_period=-1,
            comparison_periods=(0,),
        )
        .materialize(Budget(max_rows=200, max_output_bytes=1_000_000))
        .table.to_pylist()
    )
    assert all(row["treatment"] is False for row in rows if row["unit"] == "c")


def test_late_initiations_are_not_classified_as_never_treated(native_inputs):
    catalog, con = native_inputs
    con.insert("users", pa.table({"user_id": ["c", "d"], "alt_id": ["cc", "dd"]}))
    con.insert(
        "events",
        pa.table(
            {
                "user_id": ["d"],
                "ts": [datetime(2025, 1, 8, tzinfo=UTC)],
                "event_id": ["d1"],
                "alternate_event_id": ["d2"],
            }
        ),
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    rows = (
        CalendarComparisonPanel(
            Design("user", Schedule("1d", "2025-01-01", "2025-01-06")),
            model,
            cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
            control="never_treated",
            reference_period=-1,
            comparison_periods=(0,),
        )
        .materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
        .table.to_pylist()
    )
    controls = {row["unit"] for row in rows if not row["treatment"]}
    assert controls == {"c"}


def test_calendar_comparison_refuses_requested_period_outside_schedule(native_inputs):
    import pytest

    from terrine.errors import TerrineError

    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    frame = CalendarComparisonPanel(
        Design("user", Schedule("1d", "2025-01-01", "2025-01-04")),
        model,
        cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
        control="never_treated",
        reference_period=-1,
        comparison_periods=(0, 2),
    )
    with pytest.raises(TerrineError) as error:
        frame.materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
    assert error.value.code == "terrine.compile.unsupported_calendar_comparison"


def test_calendar_comparison_requires_complete_per_unit_history(native_inputs):
    import pytest

    from terrine.errors import TerrineError

    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE users ADD COLUMN observed_from TIMESTAMPTZ")
    con.raw_sql(
        "UPDATE users SET observed_from = TIMESTAMPTZ '2025-01-02 00:00:00+00' WHERE user_id = 'a'"
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id", observed_from="observed_from")},
        event_ids={"events": "event_id"},
    )
    frame = CalendarComparisonPanel(
        Design("user", Schedule("1d", "2025-01-03", "2025-01-04")),
        model,
        cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
        control="never_treated",
        reference_period=-1,
        comparison_periods=(0,),
    )
    with pytest.raises(TerrineError) as error:
        frame.materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
    assert error.value.code == "terrine.design.incomplete_unit_treatment_history"


def test_calendar_comparison_preserves_feature_named_initiation_time(native_inputs):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    frame = CalendarComparisonPanel(
        Design(
            "user",
            Schedule("1d", "2025-01-02", "2025-01-06"),
            outcomes={"initiation_time": agg("sessions", window=("0d", "1d"))},
        ),
        model,
        cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
        control="never_treated",
        reference_period=-1,
        comparison_periods=(0,),
    ).materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
    assert sorted(row["initiation_time"] for row in frame.table.to_pylist()) == [0, 0, 1, 1]


def test_not_yet_treated_history_must_extend_past_anticipation_cutoff(native_inputs):
    import pytest

    from terrine.errors import TerrineError

    catalog, con = native_inputs
    con.insert("users", pa.table({"user_id": ["c"], "alt_id": ["cc"]}))
    con.raw_sql("ALTER TABLE users ADD COLUMN observed_until TIMESTAMPTZ")
    con.raw_sql(
        "UPDATE users SET observed_until = TIMESTAMPTZ '2025-01-04 00:00:00+00' WHERE user_id = 'c'"
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id", observed_until="observed_until")},
        event_ids={"events": "event_id"},
    )
    frame = CalendarComparisonPanel(
        Design("user", Schedule("1d", "2025-01-01", "2025-01-05")),
        model,
        cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
        control="not_yet_treated",
        reference_period=-2,
        comparison_periods=(0,),
        anticipation_periods=(-1,),
    )
    with pytest.raises(TerrineError) as error:
        frame.materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
    assert error.value.code == "terrine.design.incomplete_unit_treatment_history"


def test_never_treated_history_covers_full_schedule(native_inputs):
    import pytest

    from terrine.errors import TerrineError

    catalog, con = native_inputs
    con.insert("users", pa.table({"user_id": ["c"], "alt_id": ["cc"]}))
    con.raw_sql("ALTER TABLE users ADD COLUMN observed_until TIMESTAMPTZ")
    con.raw_sql(
        "UPDATE users SET observed_until = TIMESTAMPTZ '2025-01-04 00:00:00+00' WHERE user_id = 'c'"
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id", observed_until="observed_until")},
        event_ids={"events": "event_id"},
    )
    frame = CalendarComparisonPanel(
        Design("user", Schedule("1d", "2025-01-01", "2025-01-12")),
        model,
        cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
        control="never_treated",
        reference_period=-1,
        comparison_periods=(0,),
    )
    with pytest.raises(TerrineError) as error:
        frame.materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
    assert error.value.code == "terrine.design.incomplete_unit_treatment_history"


def test_reference_outcome_window_precedes_actual_intraday_initiation(native_inputs):
    import pytest

    from terrine.errors import TerrineError

    catalog, con = native_inputs
    con.insert(
        "users",
        pa.table({"user_id": ["midday", "never"], "alt_id": ["midday-alt", "never-alt"]}),
    )
    con.insert(
        "events",
        pa.table(
            {
                "user_id": ["midday"],
                "ts": [datetime(2025, 1, 3, 12, tzinfo=UTC)],
                "event_id": ["midday-event"],
                "alternate_event_id": ["midday-alt-event"],
            }
        ),
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    materialized = CalendarComparisonPanel(
        Design(
            "user",
            Schedule("1d", "2025-01-01", "2025-01-08"),
            outcomes={"y": agg("sessions", window=("0d", "1d"))},
        ),
        model,
        cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
        control="never_treated",
        reference_period=-1,
        comparison_periods=(0,),
    ).materialize(Budget(max_rows=200, max_output_bytes=1_000_000))
    assert materialized.columns["y"].window is not None
    assert any(
        row["treatment"]
        and row["__terrine_treatment_initiation_time"] == datetime(2025, 1, 3, 12, tzinfo=UTC)
        for row in materialized.table.to_pylist()
    )
    with pytest.raises(TerrineError) as error:
        from terrine.estimate import stacked_event_study

        stacked_event_study(
            materialized, outcome="y", treatment="treatment", cohort="cohort", period="period"
        )
    assert error.value.code == "terrine.estimate.stacked_input"
    assert error.value.context["reason"] == "outcome_window_contaminates_reference"


def test_comparison_treatment_coverage_includes_outcome_window(native_inputs):
    import pytest

    from terrine.errors import TerrineError

    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-12")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    with pytest.raises(TerrineError) as error:
        CalendarComparisonPanel(
            Design(
                "user",
                Schedule("1d", "2025-01-01", "2025-01-12"),
                outcomes={"y": agg("sessions", window=("0d", "7d"))},
            ),
            model,
            cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
            control="not_yet_treated",
            reference_period=-8,
            comparison_periods=(0,),
        )
    assert error.value.code == "terrine.design.incomplete_treatment_history"


def test_not_yet_treated_observation_covers_exclusive_window_endpoint(native_inputs):
    catalog, con = native_inputs
    con.insert("users", pa.table({"user_id": ["c"], "alt_id": ["cc"]}))
    con.raw_sql("ALTER TABLE users ADD COLUMN observed_until TIMESTAMPTZ")
    con.raw_sql(
        "UPDATE users SET observed_until = TIMESTAMPTZ '2025-01-04 00:00:00+00' WHERE user_id = 'c'"
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id", observed_until="observed_until")},
        event_ids={"events": "event_id"},
    )
    frame = CalendarComparisonPanel(
        Design(
            "user",
            Schedule("1d", "2025-01-01", "2025-01-05"),
            outcomes={"y": agg("sessions", window=("0d", "1d"))},
        ),
        model,
        cohort=Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
        control="not_yet_treated",
        reference_period=-2,
        comparison_periods=(0,),
    ).materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
    assert frame.table.num_rows > 0
