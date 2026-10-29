from datetime import UTC, datetime
from decimal import Decimal

import pandas as pd
import pytest

import terrine as tr
from terrine import recipes
from terrine.errors import TerrineError
from terrine.spec import (
    AnchorRange,
    Design,
    EventAnchor,
    FixedDate,
    Initiation,
    StateObservations,
    Treatment,
)
from terrine.spec.anchors import Schedule
from terrine.spec.panels import CalendarComparisonSpec
from tests.fixtures.readme import readme_design, readme_model

_COMPLETE = ("2025-01-01", "2025-03-31")


@pytest.fixture(scope="module")
def shapes_model():
    """Signups, churn, purchases, upgrades, checkups and a plan history for one user unit."""
    stamps = pd.to_datetime(["2025-01-02", "2025-01-10"])
    model = tr.Model.from_tables(
        units={"user": tr.UnitSource(pd.DataFrame({"user_id": ["u1", "u2"]}), "user_id")},
        events={
            "signup": tr.Events(
                pd.DataFrame({"user_id": ["u1", "u2"], "signed_up_at": stamps}),
                time="signed_up_at",
                key="user_id",
                complete=_COMPLETE,
            ),
            "churn": tr.Events(
                pd.DataFrame({"user_id": ["u1"], "churned_at": stamps[:1]}),
                time="churned_at",
                key="user_id",
                complete=_COMPLETE,
            ),
            "purchase": tr.Events(
                pd.DataFrame(
                    {
                        "user_id": ["u1", "u2"],
                        "bought_at": stamps,
                        "amount": [10.0, 5.0],
                        "status": ["paid", "refunded"],
                    }
                ),
                time="bought_at",
                key="user_id",
                metrics={
                    "spend": tr.sum("amount"),
                    "purchases": tr.count(),
                    "paid_spend": tr.sum("amount", where={"status": "paid"}),
                },
                labels=("status",),
                complete=_COMPLETE,
            ),
            "upgrade": tr.Events(
                pd.DataFrame({"user_id": ["u1"], "upgraded_at": stamps[1:]}),
                time="upgraded_at",
                key="user_id",
                complete=_COMPLETE,
            ),
            "checkups": tr.Events(
                pd.DataFrame(
                    {"user_id": ["u1", "u2"], "seen_at": stamps, "health": ["well", "ill"]}
                ),
                time="seen_at",
                key="user_id",
                labels=("health",),
                complete=_COMPLETE,
            ),
        },
        history={
            "plan": tr.History(
                pd.DataFrame(
                    {"user_id": ["u1", "u2"], "changed_at": stamps, "plan": ["basic", "pro"]}
                ),
                key="user_id",
                column="plan",
                changed_at="changed_at",
                complete=_COMPLETE,
            )
        },
        event_ids={"checkups": "user_id"},
    )
    try:
        yield model
    finally:
        model.con.disconnect()


_DESIGNS = {
    "survival": lambda: recipes.survival(start_event="signup", end_event="churn"),
    "ltv": lambda: recipes.ltv(start_event="signup", metric="spend", period="7d", periods=3),
    "state_panel": lambda: recipes.state_panel(
        state_source=StateObservations("health", ("well", "ill")),
        every="1w",
        start="2025-01-06",
        end="2025-02-02",
    ),
    "trial_panel": lambda: recipes.trial_panel(
        every="1w",
        start="2025-01-06",
        end="2025-01-27",
        treatment=Initiation("upgrade", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
        outcomes={"spend_7d": tr.agg("spend", window=("0s", "7d"))},
    ),
    "life_table": lambda: recipes.life_table(
        start_event="signup",
        end_event="churn",
        bin="5d",
        group_by={"baseline_plan": tr.asof("plan", before="-1d")},
    ),
    "event_panel": lambda: recipes.event_panel(
        event="upgrade",
        periods=(-2, -1, 0, 1),
        period="2d",
        outcomes={"spend_2d": tr.agg("spend", window=("0s", "2d"))},
    ),
    "calendar_comparison": lambda: Design(
        None,
        Schedule("1w", "2025-01-06", "2025-02-03"),
        outcomes={"spend_7d": tr.agg("spend", window=("0s", "7d"))},
        shape=CalendarComparisonSpec(
            cohort=Initiation("upgrade", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
            control="not_yet_treated",
            reference_period=-1,
            comparison_periods=(0, 1),
        ),
    ),
    "event_sequence": lambda: recipes.event_sequence(
        event="purchase", horizon="11d", max_events=17, session_gap="2h"
    ),
}


def test_describe_names_every_part_of_the_readme_design(readme_con):
    model, design = readme_model(readme_con), readme_design()
    text = design.describe(model)
    for needle in (
        "customer",
        "customers",
        "customer_id",
        "2025-02-01",
        "2025-03-01",
        "orders_30d",
        "spend_30d",
        "spend_next_30d",
        "30d",
        "2025-01-01",
        "2025-03-20",
    ):
        assert needle in text
    for column in design.build(model).columns:
        assert column in text
        assert f"{column}__complete" in text
        assert f"{column}__reason" in text


@pytest.mark.parametrize("name", ["readme", *_DESIGNS])
def test_describe_runs_no_query(name, shapes_model, readme_con, monkeypatch):
    model = readme_model(readme_con) if name == "readme" else shapes_model

    def refuse(name):
        def method(*args, **kwargs):
            raise AssertionError(name)

        return method

    for method in ("execute", "to_pyarrow", "to_pyarrow_batches", "raw_sql"):
        monkeypatch.setattr(type(model.con), method, refuse(method))
    monkeypatch.setattr("terrine.compile.dispatch.compile_design", refuse("compile_design"))
    design = readme_design() if name == "readme" else _DESIGNS[name]()
    assert design.describe(model)


@pytest.mark.parametrize(
    ("name", "needles"),
    [
        ("survival", ["signup", "churn"]),
        ("ltv", ["spend", "3", "7d"]),
        ("state_panel", ["well", "ill", "1w", "health"]),
        ("trial_panel", ["upgrade", "2025-01-06", "2025-01-27", "spend_7d"]),
        ("life_table", ["signup", "churn", "5d", "baseline_plan"]),
        ("event_panel", ["upgrade", "-2, -1, 0, 1", "2d", "spend_2d"]),
        ("calendar_comparison", ["upgrade", "spend_7d", "2025-02-03"]),
        ("event_sequence", ["purchase", "11d", "17", "2h"]),
    ],
)
def test_describe_carries_each_shape_s_own_identifiers(name, needles, shapes_model):
    text = _DESIGNS[name]().describe(shapes_model)
    assert all(needle in text for needle in needles)


def test_describe_mentions_the_treatment_attribute(shapes_model):
    design = recipes.time_series(
        every="1mo",
        start="2025-02-01",
        end="2025-03-01",
        features={"purchases_30d": tr.agg("purchases", window=("-30d", "0s"))},
        treatment=Treatment("plan"),
    )
    text = design.describe(shapes_model)
    assert "plan" in text and "purchases_30d" in text and "treatment" in text


def test_describe_refuses_like_validate(readme_con):
    design = recipes.time_series(
        every="1mo",
        start="2025-02-01",
        end="2025-03-01",
        features={"refunds": tr.agg("refunds", window=("-30d", "0s"))},
    )
    with pytest.raises(TerrineError) as error:
        design.describe(readme_model(readme_con))
    assert error.value.code == "terrine.design.missing_reference"
    assert error.value.context["name"] == "refunds"


def test_describe_reads_expression_metadata_and_only_required_sources(shapes_model):
    selector = tr.event_time("signup", window=("-20d", "-5d"))
    design = Design(
        None,
        FixedDate("2025-02-01"),
        features={
            "paid": tr.agg("paid_spend", window=("-30d", "-1d")),
            "median": tr.quantile("spend", "0.5", window=("-30d", "-1d")),
            "distinct": tr.approx_distinct("spend", window=("-30d", "-1d")),
            "plan_before": tr.asof("plan", before="-2d"),
            "relative": tr.agg("spend", window=("-3d", "0s"), relative_to=selector),
            "checked": tr.event_time(
                "checkups",
                window=("-8d", "-1d"),
                label_property="health",
                equals="well",
                occurrence="nth",
                n=2,
            ),
        },
        eligibility={
            "eligible": tr.all_of(
                tr.gt(tr.agg("purchases", window=("-9d", "-1d")), 4),
                tr.eq(tr.asof("plan", before="-1d"), "pro"),
                tr.is_null(tr.event_time("upgrade", window=("-10d", "-1d")), negate=True),
            )
        },
        gap="1d",
    )
    text = design.describe(shapes_model)
    for identifier in (
        *design.features,
        *design.eligibility,
        "purchase.amount",
        "status",
        "'paid'",
        "0.5",
        "linear_type7",
        "plan",
        "signup",
        "health",
        "'well'",
        "2nd",
        "upgrade",
        "'pro'",
        "- 3d",
        "- 5d",
        "- 9d",
        "> 4",
        "==",
    ):
        assert identifier in text
    assert "churn" not in text


def test_describe_retains_calendar_clamping_and_bounds_long_schedules(shapes_model):
    design = recipes.time_series(every="1mo", start="2025-01-31", end="2026-12-31")
    text = design.describe(shapes_model)
    for value in ("2025-01-31", "2025-02-28", "2025-03-31", "2025-12-31", "2026-12-31"):
        assert value in text
    assert "2026-01-31" not in text


def test_describe_carries_anchor_offsets_ranges_and_signed_asof(shapes_model):
    design = Design(
        None,
        EventAnchor("signup", which="nth", n=3, offset="2d"),
        features={"previous_plan": tr.asof("plan", before="-4d")},
        treatment=Treatment("plan", at="-3d"),
        anchor_range=AnchorRange("2025-01-05", "2025-01-31"),
    )
    text = design.describe(shapes_model)
    for value in ("3rd", "signup", "+2d", "2025-01-05", "2025-01-31", "t0 - 4d", "t0 - 3d"):
        assert value in text


def test_describe_uses_local_schedule_dates_and_completeness_days(shapes_model):
    model = tr.Model.from_catalog(
        shapes_model.catalog.model_copy(update={"day_boundary": "UTC+05:30"}),
        con=shapes_model.con,
        units=shapes_model.units,
        readiness={name: tr.Coverage(*_COMPLETE) for name in shapes_model.sources},
    )
    design = recipes.time_series(
        every="1mo",
        start="2025-02-01",
        end="2025-03-01",
        features={"spend_before": tr.agg("spend", window=("-30d", "0s"))},
    )
    text = design.describe(model)
    for value in ("2025-02-01", "2025-03-01", "00:00 UTC+05:30", "2025-01-01", "2025-03-31"):
        assert value in text
    assert "18:30" not in text
    assert "2025-02-01T00:00:00+05:30" in Design(None, FixedDate("2025-02-01")).describe(model)


def test_describe_retention_names_identity_bands_and_horizon(shapes_model):
    design = Design(
        None,
        FixedDate("2025-02-01"),
        outcomes={
            "followup": tr.retention(
                "checkups",
                label_property="health",
                target_event="well",
                thresholds=(1, 3),
                horizon="12d",
            )
        },
    )
    text = " ".join(design.describe(shapes_model).split())
    for value in ("followup", "checkups", "health", "well", "1, 3", "t0 + 12d"):
        assert value in text


def test_describe_approximate_quantile_names_the_declared_sketch(shapes_model):
    from importlib.metadata import version

    from terrine.spec import SketchRequest

    pytest.importorskip("datasketches")
    sketch = SketchRequest("tdigest", version("datasketches"), Decimal("0.01"))
    design = Design(
        None,
        FixedDate("2025-02-01"),
        features={
            "upper": tr.approx_quantile("spend", "0.9", window=("-30d", "0s"), sketch=sketch)
        },
    )
    text = design.describe(shapes_model)
    for value in ("upper", "purchase.amount", "tdigest", sketch.version, "0.01", "0.9"):
        assert value in text
