from datetime import UTC, date, datetime
from typing import Any, cast

import pytest

from terrine.errors import TerrineError
from terrine.spec.anchors import Schedule
from terrine.spec.design import Design
from terrine.spec.io import from_json, to_json
from terrine.spec.panels import (
    CalendarComparisonPanel,
    CalendarComparisonSpec,
    Initiation,
    MatchedT0Sample,
)
from tests.spec.test_validate import model as static_model


def base_design():
    return Design("unit_id", Schedule("30d", date(2025, 1, 1), date(2025, 2, 28)))


def test_comparison_control_modes_round_trip_with_distinct_hashes():
    cohort = Initiation(
        "started", history_start=datetime(2024, 1, 1, tzinfo=UTC), knowledge="recorded_at"
    )
    never = Design(
        "unit_id",
        base_design().anchor,
        shape=CalendarComparisonSpec(cohort, "never_treated", -2, (0, 1), (-1,)),
    )
    not_yet = Design(
        "unit_id",
        base_design().anchor,
        shape=CalendarComparisonSpec(cohort, "not_yet_treated", -2, (0, 1), (-1,)),
    )
    assert never.spec_hash != not_yet.spec_hash
    assert from_json(to_json(never)) == never
    assert from_json(to_json(not_yet)) == not_yet
    assert isinstance(never.shape, CalendarComparisonSpec)


def test_comparison_reference_conflict_refuses_without_shifting():
    cohort = Initiation(
        "started", history_start=datetime(2024, 1, 1, tzinfo=UTC), knowledge="recorded_at"
    )
    with pytest.raises(TerrineError) as error:
        CalendarComparisonSpec(cohort, "never_treated", -1, (0,), (-1,))
    assert error.value.code == "terrine.design.invalid_calendar_comparison"


def test_pre_treatment_comparisons_must_precede_anticipation():
    cohort = Initiation("started", history_start=datetime(2024, 1, 1, tzinfo=UTC))
    spec = CalendarComparisonSpec(cohort, "never_treated", -2, (-3, 0), (-1,))
    assert spec.comparison_periods == (-3, 0)
    with pytest.raises(TerrineError):
        CalendarComparisonSpec(cohort, "never_treated", -2, (-1, 0), (-1,))


def test_calendar_comparison_producer_refuses_sampled_population():
    cohort = Initiation(
        "started", history_start=datetime(2024, 1, 1, tzinfo=UTC), knowledge="recorded_at"
    )
    base = Design(
        "unit_id",
        base_design().anchor,
        sample=MatchedT0Sample(("stratum",), controls_per_treated=1, seed=4),
    )
    with pytest.raises(TerrineError):
        CalendarComparisonPanel(
            base,
            cast(Any, None),
            cohort=cohort,
            control="never_treated",
            reference_period=-2,
            comparison_periods=(0,),
        )


def test_calendar_comparison_refuses_month_schedules_before_reading_sources():
    cohort = Initiation("session", history_start=datetime(2024, 1, 1, tzinfo=UTC))
    design = Design(
        "user",
        Schedule("1mo", date(2025, 1, 1), date(2025, 3, 1)),
        shape=CalendarComparisonSpec(cohort, "never_treated", -2, (0,)),
    )
    model = static_model()
    with pytest.raises(TerrineError) as error:
        design.validate(model)
    assert error.value.code == "terrine.design.invalid_calendar_comparison"
    assert error.value.context["reason"] == "calendar_months_unsupported"
    assert (error.value.context["every"], error.value.context["route"]) == ("1mo", "4w")
    assert model.con.calls == []
