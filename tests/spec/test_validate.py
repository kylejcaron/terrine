import subprocess
import sys
from datetime import UTC, date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from terrine.data import BoundCoverage
from terrine.errors import TerrineError
from terrine.semantic.models import BoundMeasure, BoundMetric
from terrine.spec.anchors import AnchorRange, EventAnchor, FixedDate, Schedule
from terrine.spec.design import Design, Treatment
from terrine.spec.expressions import EventTime, agg, asof, gt, quantile
from terrine.spec.sequence import EventSequence
from terrine.temporal.readiness import Readiness


class NoQuery:
    def __init__(self):
        self.calls = []

    def table(self, *args, **kwargs):
        self.calls.append(("table", args))
        raise AssertionError("static validation attempted a query")

    def sql(self, *args, **kwargs):
        self.calls.append(("sql", args))
        raise AssertionError("static validation attempted a query")


def model(
    *,
    kind="mean",
    window_days=None,
    transforms=(),
    dialect="duckdb",
    ready=True,
    aggregation="count",
    properties=None,
    dimensions=None,
    entities=None,
    catalog_properties=None,
    boundary="UTC",
):
    coverage = BoundCoverage(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 4, tzinfo=UTC))
    zone = timezone(timedelta(hours=-5)) if boundary == "UTC-05:00" else UTC
    source = SimpleNamespace()
    source.name, source.dialect = "events", dialect
    source.facts = {"session": None}
    source.properties, source.dimensions = properties or {}, dimensions or []
    source.entities = entities or {"user": "user_id", "workspace": "workspace_id"}
    metric = BoundMetric(
        "sessions",
        "user",
        {"user": "user_id"},
        kind,
        (BoundMeasure("session", "events", aggregation, (), window_days, None),),
        ("events",),
        transforms,
    )
    if catalog_properties is None:
        catalog_properties = {
            name: SimpleNamespace(source="events", dimension=prop.dimension_name)
            for name, prop in source.properties.items()
        }
        for dimension in source.dimensions:
            for name in dimension.properties:
                catalog_properties.setdefault(
                    name,
                    SimpleNamespace(source="events", dimension=dimension.name),
                )
    return type(
        "BoundModel",
        (),
        {
            "catalog": type(
                "CatalogMetadata",
                (),
                {
                    "day_boundary": boundary,
                    "facts": {"session": type("Fact", (), {"source": "events"})()},
                    "properties": catalog_properties,
                },
            )(),
            "sources": {"events": source},
            "metrics": {"sessions": metric},
            "readiness": Readiness({"events": coverage} if ready else {}),
            "units": {"user": object(), "workspace": object()},
            "con": NoQuery(),
            "day_boundary": staticmethod(
                lambda value: datetime(value.year, value.month, value.day, tzinfo=zone)
            ),
        },
    )()


@pytest.mark.parametrize(
    ("role", "window"),
    [("features", ("0d", "7d")), ("outcomes", ("-7d", "0d"))],
)
def test_predicate_outputs_validate_each_leaf_for_its_role(role, window):
    from terrine.spec import all_of, eq

    values = {
        "label": all_of(
            gt(agg("sessions", window=window), 0), eq(agg("sessions", window=window), 1)
        )
    }
    design = Design(
        "user",
        FixedDate("2025-01-02"),
        features=values if role == "features" else None,
        outcomes=values if role == "outcomes" else None,
    )
    with pytest.raises(TerrineError) as error:
        design.validate(model())
    assert error.value.code == "terrine.design.gap_violation"


def test_binned_state_metric_uses_ordinary_metric_admission():
    from terrine.errors import TerrineError
    from terrine.spec.anchors import Schedule
    from terrine.spec.state_panel import BinnedMetricState, StatePanel

    cases = (
        (
            {"entities": {"user": "user_id", "workspace": "workspace_id"}},
            "terrine.design.incompatible_unit",
        ),
        ({"aggregation": "median"}, "terrine.metric.unsupported_aggregation"),
        ({"transforms": (object(),)}, "terrine.metric.unsupported_transform"),
        ({"window_days": 2}, "terrine.design.metric_window_conflict"),
    )
    for options, code in cases:
        bound = model(**options)

        unit = "workspace" if "entities" in options else "user"
        design = Design(
            unit,
            Schedule("1d", date(2025, 1, 1), date(2025, 1, 3)),
            shape=StatePanel(
                BinnedMetricState("sessions", (0,), ("none", "some")),
                max_periods=2,
            ),
        )
        with pytest.raises(TerrineError) as error:
            design.validate(bound)
        assert error.value.code == code
        assert bound.con.calls == []


@pytest.mark.parametrize(
    "shape_name",
    ["sequential_treatment", "calendar_cohort"],
)
def test_panel_sources_refuse_missing_analysis_unit_mapping(shape_name):
    from terrine.spec.panels import CalendarComparisonSpec, Initiation, SequentialSpec
    from terrine.spec.validate import validate_design

    source_model = model(entities={"workspace": "workspace_id"})
    selector = Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC))
    shape = (
        SequentialSpec(selector)
        if shape_name == "sequential_treatment"
        else CalendarComparisonSpec(selector, "never_treated", -1, (0,))
    )
    design = Design("user", Schedule("1d", "2025-01-01", "2025-01-04"), shape=shape)

    with pytest.raises(TerrineError) as error:
        validate_design(design, source_model)

    assert error.value.code == "terrine.design.incompatible_unit"
    assert error.value.context == {"unit": "user", "name": "session"}


def test_calendar_comparison_requires_coverage_through_comparison_horizon():
    from terrine.spec.panels import CalendarComparisonSpec, Initiation
    from terrine.spec.validate import validate_design

    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-04"),
        shape=CalendarComparisonSpec(
            Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC), "recorded_at"),
            "never_treated",
            -1,
            (0, 2),
        ),
    )
    with pytest.raises(TerrineError) as error:
        validate_design(design, model())
    assert error.value.code == "terrine.design.incomplete_treatment_history"
    assert error.value.context["boundary"] == "2025-01-05T00:00:00+00:00"


def test_calendar_comparison_rejects_exclusive_coverage_equal_to_horizon():
    from terrine.spec.panels import CalendarComparisonSpec, Initiation
    from terrine.spec.validate import validate_design

    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-04"),
        shape=CalendarComparisonSpec(
            Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC), "recorded_at"),
            "not_yet_treated",
            -1,
            (0, 2),
        ),
    )
    bound = model()
    bound.readiness = Readiness(
        {
            "events": BoundCoverage(
                datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 5, tzinfo=UTC)
            )
        }
    )
    with pytest.raises(TerrineError) as error:
        validate_design(design, bound)
    assert error.value.context["boundary"] == "2025-01-05T00:00:00+00:00"


def test_never_treated_negative_comparisons_still_require_full_schedule_history():
    from terrine.spec.panels import CalendarComparisonSpec, Initiation
    from terrine.spec.validate import validate_design

    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-04"),
        outcomes={"y": agg("sessions", window=("0d", "1d"))},
        shape=CalendarComparisonSpec(
            Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
            "never_treated",
            -1,
            (-2,),
        ),
    )
    bound = model()
    bound.readiness = Readiness(
        {
            "events": BoundCoverage(
                datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 3, tzinfo=UTC)
            )
        }
    )
    with pytest.raises(TerrineError) as error:
        validate_design(design, bound)
    assert error.value.code == "terrine.design.incomplete_treatment_history"


def test_never_treated_coverage_rejects_equality_at_anticipation_cutoff():
    from terrine.spec.panels import CalendarComparisonSpec, Initiation
    from terrine.spec.validate import validate_design

    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-10"),
        shape=CalendarComparisonSpec(
            Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
            "never_treated",
            -2,
            (0,),
            (-1,),
        ),
    )
    bound = model()
    bound.readiness = Readiness(
        {
            "events": BoundCoverage(
                datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 10, tzinfo=UTC)
            )
        }
    )
    with pytest.raises(TerrineError) as error:
        validate_design(design, bound)
    assert error.value.code == "terrine.design.incomplete_treatment_history"


def test_never_treated_schedule_boundary_coverage_is_admissible():
    from terrine.spec.panels import CalendarComparisonSpec, Initiation
    from terrine.spec.validate import validate_design

    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-10"),
        shape=CalendarComparisonSpec(
            Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
            "never_treated",
            -1,
            (0,),
        ),
    )
    bound = model()
    bound.readiness = Readiness(
        {
            "events": BoundCoverage(
                datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 10, tzinfo=UTC)
            )
        }
    )
    assert validate_design(design, bound).required_sources == ("events",)


def test_never_treated_schedule_history_refusal_reports_schedule_boundary():
    from terrine.spec.panels import CalendarComparisonSpec, Initiation
    from terrine.spec.validate import validate_design

    design = Design(
        "user",
        Schedule("7d", "2025-01-01", "2025-01-10"),
        shape=CalendarComparisonSpec(
            Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
            "never_treated",
            -1,
            (0,),
        ),
    )
    bound = model()
    bound.readiness = Readiness(
        {
            "events": BoundCoverage(
                datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 9, tzinfo=UTC)
            )
        }
    )
    with pytest.raises(TerrineError) as error:
        validate_design(design, bound)
    assert error.value.context["boundary"] == "2025-01-10T00:00:00+00:00"


def test_not_yet_treated_refusal_reports_comparison_horizon_only():
    from terrine.spec.panels import CalendarComparisonSpec, Initiation
    from terrine.spec.validate import validate_design

    design = Design(
        "user",
        Schedule("7d", "2025-01-01", "2025-01-10"),
        shape=CalendarComparisonSpec(
            Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC)),
            "not_yet_treated",
            -1,
            (0,),
        ),
    )
    bound = model()
    bound.readiness = Readiness(
        {
            "events": BoundCoverage(
                datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 7, tzinfo=UTC)
            )
        }
    )
    with pytest.raises(TerrineError) as error:
        validate_design(design, bound)
    assert error.value.context["boundary"] == "2025-01-08T00:00:00+00:00"


def test_window_extending_beyond_declared_coverage_is_runtime_incomplete_not_refused():
    design = Design(
        "user", EventAnchor("session"), features={"x": agg("sessions", window=("-7d", "0d"))}
    )
    from terrine.spec.validate import validate_design

    bound = model()
    report = validate_design(design, bound)
    assert "events" in report.required_sources
    assert "coverage_completeness" in report.runtime_obligations
    assert design.validate(bound) is None
    assert bound.con.calls == []


def test_event_anchor_fact_must_be_member_of_bound_source_facts():
    bound = model()
    bound.catalog.facts["ghost"] = SimpleNamespace(source="events")
    with pytest.raises(TerrineError) as error:
        Design("user", EventAnchor("ghost")).validate(bound)
    assert error.value.code == "terrine.design.missing_reference"


def test_unknown_metric_refuses_with_stable_code():
    with pytest.raises(TerrineError) as error:
        Design(
            "user", EventAnchor("session"), features={"x": agg("missing", window=("-1d", "0d"))}
        ).validate(model())
    assert error.value.code == "terrine.design.missing_reference"


def test_exact_quantile_is_admitted_without_query():
    design = Design(
        "user",
        EventAnchor("session"),
        features={"p50": quantile("sessions", "0.5", window=("-7d", "0d"))},
    )
    assert design.validate(model()) is None


def test_exact_quantile_refuses_unverified_backend_before_query():
    with pytest.raises(TerrineError) as error:
        Design(
            "user",
            FixedDate(date(2025, 1, 1)),
            features={"p50": quantile("sessions", "0.5", window=("-7d", "0d"))},
        ).validate(model(dialect="sqlite"))
    assert error.value.code == "terrine.metric.exact_quantile_backend_unsupported"


def test_exact_quantile_accepts_case_insensitive_duckdb_dialect():
    design = Design(
        "user",
        FixedDate(date(2025, 1, 1)),
        features={"p50": quantile("sessions", "0.5", window=("-7d", "0d"))},
    )
    assert design.validate(model(dialect="DuckDB")) is None


@pytest.mark.parametrize("kind", ["conversion", "ratio"])
def test_nonadditive_metric_aggregations_refuse_unsupported_metric_kinds(kind):
    with pytest.raises(TerrineError) as error:
        Design(
            "user",
            FixedDate(date(2025, 1, 1)),
            features={"x": agg("sessions", window=("-1d", "0d"))},
        ).validate(model(kind=kind, aggregation="min"))
    assert error.value.code == "terrine.metric.nonadditive_kind_unsupported"


def test_missing_referenced_source_readiness_refuses():
    with pytest.raises(TerrineError) as error:
        Design(
            "user", EventAnchor("session"), features={"x": agg("sessions", window=("-1d", "0d"))}
        ).validate(model(ready=False))
    assert error.value.code == "terrine.readiness.missing_source"


def test_event_sequence_validation_rejects_unknown_event_property_without_query():
    from terrine.errors import TerrineError
    from terrine.spec.anchors import EventAnchor
    from terrine.spec.design import Design
    from terrine.spec.validate import validate_design
    from terrine.windows import Window

    bound = model()
    design = Design(
        "user",
        EventAnchor("session"),
        shape=EventSequence("session", Window("-1d", "0d"), max_events=3, properties=("missing",)),
    )
    with pytest.raises(TerrineError) as error:
        validate_design(design, bound)
    assert error.value.code == "terrine.design.missing_reference"
    assert bound.con.calls == []


def test_future_selector_dependency_refuses_safe_looking_preanchor_metric():
    from terrine.spec.validate import validate_design
    from terrine.windows import Window

    origin = EventTime("session", Window("-2d", "-1d"))
    selector = EventTime("session", Window("1d", "2d"), relative_to=origin)
    design = Design(
        "user",
        EventAnchor("session"),
        features={"x": agg("sessions", window=("-2d", "-1d"), relative_to=selector)},
    )
    bound = model()
    with pytest.raises(TerrineError) as error:
        validate_design(design, bound)
    assert error.value.code == "terrine.sequence.reanchor_leak"
    assert bound.con.calls == []


def test_asof_before_offsets_relative_support_for_feature_cutoff():
    from terrine.spec.validate import validate_design
    from terrine.windows import Window

    origin = EventTime("session", Window("-2d", "-1d"))
    attribute = SimpleNamespace(dtype="string", dimension_name=None, as_of="effective_time")
    design = Design(
        "user",
        EventAnchor("session"),
        features={"status": asof("state", before="2d", relative_to=origin)},
    )
    bound = model(properties={"state": attribute})
    with pytest.raises(TerrineError) as error:
        validate_design(design, bound)
    assert error.value.code == "terrine.sequence.reanchor_leak"
    assert bound.con.calls == []


def test_event_sequence_validation_accepts_dimension_backed_event_property():
    from terrine.spec.anchors import EventAnchor
    from terrine.spec.design import Design
    from terrine.spec.validate import validate_design
    from terrine.windows import Window

    dimension = SimpleNamespace(
        name="regions",
        properties={"region": SimpleNamespace(dtype="string", dimension_name="regions")},
    )
    bound = model(
        dimensions=[dimension],
        catalog_properties={"region": SimpleNamespace(source="events", dimension="regions")},
    )
    design = Design(
        "user",
        EventAnchor("session"),
        shape=EventSequence("session", Window("-1d", "0d"), max_events=3, properties=("region",)),
    )
    validate_design(design, bound)
    assert bound.con.calls == []


def test_feature_and_eligibility_windows_must_end_by_minus_gap():
    with pytest.raises(TerrineError) as error:
        Design(
            "user",
            EventAnchor("session"),
            gap="1d",
            features={"x": agg("sessions", window=("-2d", "0d"))},
        ).validate(model())
    assert error.value.code == "terrine.design.gap_violation"
    with pytest.raises(TerrineError):
        Design(
            "user",
            EventAnchor("session"),
            gap="1d",
            eligibility={"eligible": gt(agg("sessions", window=("-2d", "0d")), 0)},
        ).validate(model())
    Design(
        "user",
        EventAnchor("session"),
        gap="1d",
        features={"x": agg("sessions", window=("-2d", "-1d"))},
    ).validate(model())


def test_outcomes_start_at_or_after_zero():
    with pytest.raises(TerrineError) as error:
        Design(
            "user", EventAnchor("session"), outcomes={"y": agg("sessions", window=("-1s", "1d"))}
        ).validate(model())
    assert error.value.code == "terrine.design.gap_violation"
    Design(
        "user", EventAnchor("session"), outcomes={"y": agg("sessions", window=("0d", "1d"))}
    ).validate(model())


def test_asof_cutoff_obeys_strict_gap_but_baseline_treatment_at_zero_is_descriptive():
    prop = SimpleNamespace(dimension_name=None, as_of="static")
    properties = {"baseline": prop}
    with pytest.raises(TerrineError):
        Design(
            "user", EventAnchor("session"), gap="1d", features={"x": asof("baseline", before="0d")}
        ).validate(model(properties=properties))
    Design(
        "user",
        EventAnchor("session"),
        gap="1d",
        treatment=Treatment("baseline", at="0d", known_at_t0=True),
    ).validate(model(properties=properties))


def test_unknown_event_anchor_cutoff_alignment_is_runtime_obligation():
    prop = SimpleNamespace(dimension_name=None, as_of="event_time")
    from terrine.spec.validate import validate_design

    report = validate_design(
        Design(
            "user",
            EventAnchor("session"),
            features={"status": asof("status", before="-1d")},
        ),
        model(properties={"status": prop}),
    )
    assert "asof_cutoff_alignment" in report.runtime_obligations


def test_metric_window_days_conflict_refuses():
    with pytest.raises(TerrineError) as error:
        Design(
            "user", EventAnchor("session"), features={"x": agg("sessions", window=("-3d", "0d"))}
        ).validate(model(window_days=7))
    assert error.value.code == "terrine.design.metric_window_conflict"


def test_date_only_fixed_anchor_uses_model_day_boundary_for_calendar_average():
    Design(
        "user", FixedDate(date(2025, 3, 1)), features={"x": agg("sessions", window=("-1d", "0d"))}
    ).validate(model(aggregation="avg_calendar_day", boundary="UTC-05:00"))
    with pytest.raises(TerrineError) as error:
        Design(
            "user",
            FixedDate(datetime(2025, 3, 1, tzinfo=UTC)),
            features={"x": agg("sessions", window=("-1d", "0d"))},
        ).validate(model(aggregation="avg_calendar_day", boundary="UTC-05:00"))
    assert error.value.code == "terrine.metric.unaligned_calendar_average"


def test_extreme_instant_calendar_alignment_is_a_coded_refusal():
    bound = model(aggregation="avg_calendar_day", boundary="UTC-05:00")
    for instant in (datetime.min.replace(tzinfo=UTC), datetime.max.replace(tzinfo=UTC)):
        with pytest.raises(TerrineError) as error:
            Design(
                "user",
                FixedDate(instant),
                outcomes={"y": agg("sessions", window=("0d", "1d"))},
            ).validate(bound)
        assert error.value.code == "terrine.metric.unaligned_calendar_average"


def test_subday_schedule_calendar_average_refuses_known_intraday_anchor():
    with pytest.raises(TerrineError) as error:
        Design(
            "user",
            Schedule("12h", date(2025, 3, 1), date(2025, 3, 3)),
            features={"x": agg("sessions", window=("-1d", "0d"))},
        ).validate(model(aggregation="avg_calendar_day"))
    assert error.value.code == "terrine.metric.unaligned_calendar_average"


@pytest.mark.parametrize("aggregation", ["min", "max", "count_distinct"])
def test_nonadditive_aggregations_are_admitted(aggregation):
    design = Design(
        "user",
        EventAnchor("session"),
        features={"x": agg("sessions", window=("-1d", "0d"))},
    )
    assert design.validate(model(aggregation=aggregation)) is None


def test_named_dimension_ambiguity_refuses_and_does_not_conflate_entity_keys():
    prop = SimpleNamespace(dimension_name="unused", as_of="event_time")
    dimensions = [
        type(
            "Dimension",
            (),
            {
                "name": name,
                "source_entity": "workspace",
                "validity": None,
                "properties": {"region": prop},
            },
        )()
        for name in ("account_dim", "billing_dim")
    ]
    with pytest.raises(TerrineError) as error:
        Design(
            "user", EventAnchor("session"), features={"region": asof("region", before="0d")}
        ).validate(model(dimensions=dimensions, catalog_properties={}))
    assert error.value.code == "terrine.design.ambiguous_dimension"
    one_dimension = [dimensions[0]]
    with pytest.raises(TerrineError) as error:
        Design(
            "user", EventAnchor("session"), features={"region": asof("region", before="0d")}
        ).validate(
            model(
                dimensions=one_dimension, entities={"user": "user_id", "workspace": "workspace_id"}
            )
        )
    assert error.value.code == "terrine.design.incompatible_unit"


def test_dimension_property_inline_and_dimension_bindings_are_one_logical_property():
    prop = SimpleNamespace(dimension_name="account_dim", as_of="event_time")
    dimension = SimpleNamespace(
        name="account_dim",
        source_entity="workspace",
        validity=None,
        properties={"region": prop},
    )
    assert (
        Design(
            "workspace",
            FixedDate(date(2025, 3, 1)),
            features={"region": asof("region", before="0d")},
        ).validate(
            model(
                properties={"region": prop},
                dimensions=[dimension],
            )
        )
        is None
    )


def test_shared_dimension_on_multiple_sources_uses_catalog_property_owner():
    prop = SimpleNamespace(dimension_name="account_dim", as_of="event_time")
    dimension = SimpleNamespace(
        name="account_dim",
        source_entity="workspace",
        validity=None,
        properties={"region": prop},
    )
    bound = model(properties={"region": prop}, dimensions=[dimension])
    other = SimpleNamespace()
    other.facts = {}
    other.name, other.dialect = "other_events", "duckdb"
    other.properties, other.dimensions = {}, [dimension]
    other.entities = {"workspace": "workspace_id"}
    bound.sources["other_events"] = other
    from terrine.spec.validate import validate_design

    report = validate_design(
        Design(
            "workspace",
            FixedDate(date(2025, 3, 1)),
            features={"region": asof("region", before="0d")},
        ),
        bound,
    )
    assert report.required_sources == ("events",)


def test_static_dimension_and_static_inline_properties_do_not_need_event_resolution():
    date_anchor = FixedDate(date(2025, 3, 1))
    dimension_property = SimpleNamespace(dimension_name="static_dim", as_of="event_time")
    static_dimension = SimpleNamespace(
        name="static_dim",
        source_entity="workspace",
        validity=None,
        properties={"region": dimension_property},
    )
    assert (
        Design(
            "workspace",
            date_anchor,
            features={"region": asof("region", before="-1d")},
        ).validate(model(dimensions=[static_dimension], dialect="sqlite"))
        is None
    )
    inline_static = SimpleNamespace(dimension_name=None, as_of="static")
    assert (
        Design(
            "user",
            date_anchor,
            features={"enabled": asof("enabled", before="-1d")},
        ).validate(model(properties={"enabled": inline_static}, dialect="sqlite"))
        is None
    )


@pytest.mark.parametrize("as_of", ("event_time", "static"))
def test_versioned_cutoff_capability_uses_exact_anchor_clock_not_offset_sign(as_of):
    prop = SimpleNamespace(dimension_name="history_dim", as_of=as_of)
    dimension = SimpleNamespace(
        name="history_dim",
        source_entity="workspace",
        validity=object(),
        properties={"region": prop},
    )
    Design(
        "workspace",
        FixedDate(date(2025, 3, 1)),
        features={"region": asof("region", before="-1d")},
    ).validate(model(dimensions=[dimension], dialect="sqlite", boundary="UTC-05:00"))
    with pytest.raises(TerrineError) as error:
        Design(
            "workspace",
            FixedDate(datetime(2025, 3, 1, 12, tzinfo=UTC)),
            features={"region": asof("region", before="0d")},
        ).validate(model(dimensions=[dimension], dialect="sqlite"))
    assert error.value.code == "terrine.temporal.unsupported_event_resolution"


def test_extreme_asof_clock_classification_avoids_datetime_endpoint_arithmetic():
    prop = SimpleNamespace(dimension_name="history_dim", as_of="event_time")
    dimension = SimpleNamespace(
        name="history_dim",
        source_entity="workspace",
        validity=object(),
        properties={"region": prop},
    )
    with pytest.raises(TerrineError) as error:
        Design(
            "workspace",
            FixedDate(datetime.min.replace(tzinfo=UTC)),
            features={"region": asof("region", before="0d")},
        ).validate(model(dimensions=[dimension], dialect="sqlite", boundary="UTC-05:00"))
    assert error.value.code == "terrine.temporal.unsupported_event_resolution"


def test_inline_event_time_cutoff_requires_capability_only_when_unaligned():
    prop = SimpleNamespace(dimension_name=None, as_of="event_time")
    Design(
        "user",
        FixedDate(date(2025, 3, 1)),
        features={"status": asof("status", before="-1d")},
    ).validate(model(properties={"status": prop}, dialect="sqlite"))
    with pytest.raises(TerrineError) as error:
        Design(
            "user",
            FixedDate(datetime(2025, 3, 1, 12, tzinfo=UTC)),
            features={"status": asof("status", before="0d")},
        ).validate(model(properties={"status": prop}, dialect="sqlite"))
    assert error.value.code == "terrine.temporal.unsupported_event_resolution"


def test_unsupported_kind_transform_and_event_profile_refuse():
    design = Design(
        "user", EventAnchor("session"), features={"x": agg("sessions", window=("-1d", "0d"))}
    )
    with pytest.raises(TerrineError) as error:
        design.validate(model(kind="other"))
    assert error.value.code == "terrine.metric.unsupported_temporal_kind"
    with pytest.raises(TerrineError) as error:
        design.validate(model(transforms=(1,)))
    assert error.value.code == "terrine.metric.unsupported_transform"
    with pytest.raises(TerrineError) as error:
        design.validate(model(dialect="other"))
    assert error.value.code == "terrine.temporal.unsupported_event_resolution"
    with pytest.raises(TerrineError) as error:
        Design(
            "user",
            FixedDate(datetime(2025, 3, 1, 12, tzinfo=UTC)),
            features={"x": agg("sessions", window=("-1d", "0d"))},
        ).validate(model(dialect="other"))
    assert error.value.code == "terrine.temporal.unsupported_event_resolution"


def test_fixed_anchor_and_mixed_date_datetime_range_use_model_day_boundary():
    Design(
        "user",
        FixedDate(date(2025, 3, 1)),
        anchor_range=AnchorRange(start=datetime(2025, 3, 1, 1, tzinfo=UTC), end=date(2025, 3, 1)),
    ).validate(model(boundary="UTC-05:00"))
    with pytest.raises(TerrineError):
        Design(
            "user",
            FixedDate(date(2025, 3, 1)),
            anchor_range=AnchorRange(
                start=datetime(2025, 3, 1, 1, tzinfo=UTC), end=date(2025, 3, 1)
            ),
        ).validate(model())
    code = r"""
import builtins
from datetime import UTC, date, datetime
from types import SimpleNamespace

from terrine.data import BoundCoverage
from terrine.errors import TerrineError
from terrine.semantic.models import BoundMeasure, BoundMetric
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design
from terrine.spec.expressions import agg
from terrine.temporal.readiness import Readiness
from terrine.spec.validate import validate_design

coverage = BoundCoverage(
    datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 4, tzinfo=UTC)
)
measure = BoundMeasure("f", "events", "avg_calendar_day", (), None, None)
metric = BoundMetric("m", "user", {"user": "id"}, "mean", (measure,), ("events",), ())
source = SimpleNamespace(
    name="events",
    dialect="duckdb",
    properties={},
    facts={},
    entities={"user": "id"},
)
model = SimpleNamespace(
    catalog=SimpleNamespace(day_boundary="UTC", facts={}),
    sources={"events": source},
    metrics={"m": metric},
    readiness=Readiness({"events": coverage}),
    units={"user": object()},
    day_boundary=lambda value: datetime(value.year, value.month, value.day, tzinfo=UTC),
)
original = builtins.__import__


def blocked(name, *args, **kwargs):
    denied = ("ibis.", "terrine.temporal", "terrine.compile")
    if name == "ibis" or name.startswith(denied):
        raise AssertionError(f"blocked import attempted: {name}")
    return original(name, *args, **kwargs)


builtins.__import__ = blocked
try:
    design = Design(
        "user",
        FixedDate(date(2025, 3, 1)),
        features={"x": agg("m", window=("-1h", "0d"))},
    )
    validate_design(design, model)
except TerrineError as exc:
    assert exc.code == "terrine.metric.unaligned_calendar_average"
else:
    raise AssertionError("expected unaligned calendar-average refusal")
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


def test_outcome_selector_support_is_not_checked_as_outcome_measurement():
    from terrine.spec.expressions import agg
    from terrine.windows import Window

    selector = EventTime("session", Window("-2d", "-1d"))
    design = Design(
        "user",
        EventAnchor("session"),
        outcomes={"later": agg("sessions", window=("3d", "4d"), relative_to=selector)},
    )
    assert design.validate(model()) is None


@pytest.mark.parametrize("shape_name", ["sequential", "calendar"])
def test_panel_history_validation_covers_reanchored_outcome_support(shape_name):
    from terrine.spec.expressions import event_time
    from terrine.spec.panels import CalendarComparisonSpec, Initiation, SequentialSpec

    selector = event_time("session", window=("2d", "3d"))
    treatment = Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC))
    shape = (
        SequentialSpec(treatment)
        if shape_name == "sequential"
        else CalendarComparisonSpec(treatment, "not_yet_treated", -1, (0,))
    )
    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-02"),
        outcomes={"y": agg("sessions", window=("0d", "1d"), relative_to=selector)},
        shape=shape,
    )

    with pytest.raises(TerrineError) as error:
        design.validate(model())

    assert error.value.code == "terrine.design.incomplete_treatment_history"


@pytest.mark.parametrize("shape_name", ["sequential", "calendar"])
def test_panel_history_validation_covers_exact_quantile_outcome(shape_name):
    from terrine.spec.panels import CalendarComparisonSpec, Initiation, SequentialSpec

    initiation = Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC))
    shape = (
        SequentialSpec(initiation)
        if shape_name == "sequential"
        else CalendarComparisonSpec(initiation, "not_yet_treated", -1, (0,))
    )
    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-02"),
        outcomes={"y": quantile("sessions", "0.5", window=("0d", "7d"))},
        shape=shape,
    )
    with pytest.raises(TerrineError) as error:
        design.validate(model())
    assert error.value.code == "terrine.design.incomplete_treatment_history"


@pytest.mark.parametrize("shape_name", ["sequential", "calendar"])
def test_approximate_distinct_outcome_contributes_to_panel_followup_horizon(shape_name):
    from terrine.spec.expressions import approx_distinct
    from terrine.spec.panels import CalendarComparisonSpec, Initiation, SequentialSpec

    initiation = Initiation("events", "session", datetime(2025, 1, 1, tzinfo=UTC))
    shape = (
        SequentialSpec(initiation)
        if shape_name == "sequential"
        else CalendarComparisonSpec(initiation, "not_yet_treated", -1, (0,))
    )
    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-02"),
        outcomes={"distinct": approx_distinct("sessions", window=("0d", "7d"))},
        shape=shape,
    )
    with pytest.raises(TerrineError) as error:
        design.validate(model(aggregation="count_distinct"))
    assert error.value.code == "terrine.design.incomplete_treatment_history"


def test_unknown_string_shape_is_rejected():
    design = Design("user", FixedDate(date(2025, 1, 1)), shape="ltv")
    with pytest.raises(TerrineError) as error:
        design.validate(model())
    assert error.value.code == "terrine.design.invalid"
