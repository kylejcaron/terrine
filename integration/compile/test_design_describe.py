import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

import terrine as tr
from terrine.spec import CalendarComparisonSpec, Initiation, StateObservations, TimeToEvent
from terrine.spec.expressions import Expr


@pytest.fixture(scope="module")
def description_model():
    complete = ("2024-12-01", "2025-01-31")
    users = ["a", "b", "quiet", "short"]
    model = tr.Model.from_tables(
        units={
            "user": tr.UnitSource(
                pd.DataFrame(
                    {
                        "id": users,
                        "until_at": pd.to_datetime(["2025-02-01"] * 3 + ["2025-01-08"], utc=True),
                    }
                ),
                "id",
            )
        },
        events={
            "purchase": tr.Events(
                pd.DataFrame(
                    {
                        "id": ["a", "a"],
                        "at": pd.to_datetime(["2025-01-03", "2025-01-04"], utc=True),
                        "event_id": ["p1", "p2"],
                        "amount": [5, 7],
                    }
                ),
                time="at",
                key="id",
                metrics={
                    "count": tr.count(),
                    "total": tr.sum("amount"),
                    "event_mean": tr.mean("amount"),
                    "daily_mean": tr.mean("amount", per="day"),
                    "minimum": tr.min("amount"),
                    "maximum": tr.max("amount"),
                    "distinct": tr.count_distinct("amount"),
                    "ratio": tr.ratio("total", "count"),
                },
                complete=complete,
            ),
            "signup": tr.Events(
                pd.DataFrame(
                    {
                        "id": [*users, "a"],
                        "at": pd.to_datetime(["2025-01-02"] * 4 + ["2025-01-04"], utc=True),
                        "event_id": ["s1", "s2", "s3", "s4", "s5"],
                    }
                ),
                time="at",
                key="id",
                complete=complete,
            ),
            "churn": tr.Events(
                pd.DataFrame({"id": ["a"], "at": pd.to_datetime(["2025-01-10"], utc=True)}),
                time="at",
                key="id",
                complete=complete,
            ),
            "upgrade": tr.Events(
                pd.DataFrame(
                    {"id": ["a", "b"], "at": pd.to_datetime(["2025-01-06", "2025-01-13"], utc=True)}
                ),
                time="at",
                key="id",
                complete=complete,
            ),
            "checkup": tr.Events(
                pd.DataFrame(
                    {
                        "id": ["a", "a"],
                        "at": pd.to_datetime(["2025-01-06", "2025-01-13"], utc=True),
                        "health": ["well", "ill"],
                    }
                ),
                time="at",
                key="id",
                labels=("health",),
                complete=complete,
            ),
        },
        event_ids={"purchase": "event_id", "signup": "event_id"},
    )
    try:
        yield model
    finally:
        model.con.disconnect()


def _materialize(design, model):
    return design.build(model).materialize(
        tr.Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)
    )


def _tokens(summary):
    return set(re.findall(r"\b[a-z][a-z0-9_]*\b", summary))


def test_description_column_set_matches_materialized_eligibility(description_model):
    design = tr.Design(
        None,
        tr.FixedDate("2025-01-06"),
        features={"prior": tr.agg("count", window=("-5d", "0s"))},
        outcomes={"future": tr.agg("total", window=("0s", "2d"))},
        eligibility={"eligible": tr.eq(tr.agg("count", window=("-1d", "0s")), 0)},
    )
    result = _materialize(design, description_model)
    expected = {"unit", "anchor_id", "t0", *result.columns}
    for meta in result.columns.values():
        expected.update(name for name in (meta.complete_column, meta.reason_column) if name)
    assert expected <= set(result.table.column_names)
    listed = design.describe(description_model).rsplit("Columns:", 1)[1]
    assert set(re.findall(r"\b[a-z][a-z0-9_]*\b", listed)) == expected


def test_description_empty_results_match_bound_aggregations(description_model):
    expressions: dict[str, Expr] = {
        name: tr.agg(name, window=("-2d", "0s"))
        for name in (
            "count",
            "total",
            "event_mean",
            "daily_mean",
            "minimum",
            "maximum",
            "distinct",
            "ratio",
        )
    }
    expressions["median"] = tr.quantile("total", "0.5", window=("-2d", "0s"))
    design = tr.Design(None, tr.FixedDate("2025-01-06"), features=expressions)
    result = _materialize(design, description_model)
    row = next(row for row in result.table.to_pylist() if row["unit"] == "quiet")
    declared = dict(
        re.findall(
            r"(?m)^\s+([a-z_]+): (null|false|0(?:\.0)?)\b", design.describe(description_model)
        )
    )
    assert set(declared) == set(expressions)
    for name, literal in declared.items():
        expected = None if literal == "null" else False if literal == "false" else float(literal)
        assert row[name] == expected
    assert row["daily_mean"] == 0.0
    assert row["daily_mean__reason"] is None
    assert row["event_mean"] is None and row["event_mean__reason"] == "no_observations"


@pytest.mark.parametrize("representation", ["positions", "arrays"])
def test_description_sequence_grain_and_reasons_match_materialization(
    description_model, representation
):
    model = tr.Model.from_catalog(
        description_model.catalog,
        con=description_model.con,
        readiness=description_model.readiness,
        units={"user": tr.UnitSource("user", "id", observed_until="until_at")},
        event_ids=description_model.event_ids,
    )
    design = tr.Design(
        None,
        tr.EventAnchor("signup", offset="1d"),
        shape=tr.EventSequence(
            "purchase", tr.Window("0s", "2d"), max_events=5, representation=representation
        ),
    )
    result = _materialize(design, model)
    rows = [row for row in result.table.to_pylist() if row["unit"] == "a"]
    summary = design.describe(model)
    if representation == "positions":
        assert [row["is_event"] for row in rows].count(False) == 1
        assert [row["is_event"] for row in rows].count(True) == 2
        assert set(re.findall(r"is_event=(true|false)", summary)) == {"true", "false"}
    else:
        assert len(rows) == 1 and len(rows[0]["event_values"]) == 2
        assert "event_values" in _tokens(summary)
    for offset, unit, reason in (
        ("29d", "a", "source_coverage"),
        ("5d", "short", "unit_observation"),
    ):
        incomplete = replace(design, anchor=tr.EventAnchor("signup", offset=offset))
        row = next(
            row for row in _materialize(incomplete, model).table.to_pylist() if row["unit"] == unit
        )
        assert row["event_count"] is None
        assert row["event_count__reason"] == reason
        assert reason in _tokens(incomplete.describe(model))


def test_description_trajectory_uses_the_selected_origin_and_period_reasons(description_model):
    design = tr.recipes.ltv(
        start_event="signup",
        metric="total",
        period="2d",
        periods=3,
        administrative_cutoff=datetime(2025, 1, 7, tzinfo=UTC),
    )
    design = replace(design, anchor=tr.EventAnchor("signup", offset="2d"))
    result = _materialize(design, description_model)
    rows = sorted(
        (row for row in result.table.to_pylist() if row["unit"] == "a"), key=lambda row: row["age"]
    )
    assert {row["cohort_time"] for row in rows} == {datetime(2025, 1, 4, tzinfo=UTC)}
    assert [row["period_value__reason"] for row in rows] == [
        None,
        "administrative_cutoff",
        "administrative_cutoff",
    ]
    tokens = _tokens(design.describe(description_model))
    assert {"period_value", "administrative_cutoff", "cohort_time"} <= tokens
    assert "+2d" in design.describe(description_model)


@pytest.mark.parametrize("life_table", [False, True])
def test_description_survival_coverage_is_censoring_not_a_missing_window(
    description_model, life_table
):
    model = tr.Model.from_catalog(
        description_model.catalog,
        con=description_model.con,
        readiness=description_model.readiness,
        units={"user": tr.UnitSource("user", "id", observed_until="until_at")},
        event_ids=description_model.event_ids,
    )
    design = (
        tr.recipes.life_table(start_event="signup", end_event="churn", bin="7d")
        if life_table
        else tr.Design(None, tr.EventAnchor("signup"), shape=TimeToEvent("churn"))
    )
    result = _materialize(design, model)
    rows = result.table.to_pylist()
    if life_table:
        reasons = {reason for row in rows for reason, count in row["censored_by_reason"] if count}
    else:
        censored = [row for row in rows if not row["event"]]
        assert censored and all(row["duration"] is not None for row in censored)
        assert all(row["duration__complete"] is True for row in censored)
        reasons = {row["censor_reason"] for row in censored}
        short = next(row for row in censored if row["unit"] == "short")
        assert short["duration"] == timedelta(days=6)
    assert reasons == {"source_coverage"}
    assert reasons | {"until_at"} <= _tokens(design.describe(model))


def test_description_state_panel_matches_missing_and_absorbing_states(description_model):
    design = tr.recipes.state_panel(
        state_source=StateObservations("health", ("well", "ill"), absorbing=("ill",)),
        every="1w",
        start="2025-01-06",
        end="2025-01-26",
    )
    rows = _materialize(design, description_model).table.to_pylist()
    a_rows = [row for row in rows if row["unit"] == "a"]
    assert len(a_rows) == 1 and a_rows[0]["terminal"] is True
    reasons = {row["to_state__reason"] for row in rows if row["to_state__reason"]}
    assert reasons == {"missing_state_observation"}
    assert reasons <= _tokens(design.describe(description_model))


def test_description_calendar_comparison_names_the_full_stack_grain(description_model):
    design = tr.Design(
        None,
        tr.Schedule("1w", "2024-12-30", "2025-01-20"),
        outcomes={"future": tr.agg("total", window=("0s", "1d"))},
        shape=CalendarComparisonSpec(
            Initiation("upgrade", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
            control="not_yet_treated",
            reference_period=-1,
            comparison_periods=(0, 1),
        ),
    )
    rows = _materialize(design, description_model).table.to_pylist()
    assert {row["event_period"] for row in rows} == {-1, 0, 1}
    assert len({(row["unit"], row["cohort"], row["event_period"]) for row in rows}) == len(rows)
    assert len({(row["unit"], row["t0"]) for row in rows}) < len(rows)
    assert {"cohort", "event_period"} <= _tokens(design.describe(description_model))


def test_description_event_panel_uses_selected_anchor_times(description_model):
    design = tr.recipes.event_panel(
        event="signup", periods=(-1, 0, 1), period="2d", occurrence="nth", n=2
    )
    rows = _materialize(design, description_model).table.to_pylist()
    assert {row["parent_t0"] for row in rows} == {datetime(2025, 1, 4, tzinfo=UTC)}
    assert all(
        row["t0"] == row["parent_t0"] + timedelta(days=2 * row["event_period"]) for row in rows
    )
    assert {"parent_t0", "event_period"} <= _tokens(design.describe(description_model))
