"""Every public constructor refuses user input with a stable code and portable context."""

import copy
import pickle
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pyarrow as pa
import pytest

from terrine.data import BoundCoverage, Budget, Coverage, Events, History, UnitSource
from terrine.dataset import GroupSplit, TimeSplit
from terrine.errors import TerrineError
from terrine.spec import (
    AnchorRange,
    BinnedMetricState,
    CalendarComparisonSpec,
    CohortGrouping,
    DatasetDefinition,
    Design,
    EventAnchor,
    EventPanelSpec,
    EventSequence,
    FixedDate,
    Initiation,
    LifeTable,
    MatchedT0Sample,
    Schedule,
    Sequential,
    SketchRequest,
    StateObservations,
    StatePanel,
    TimeToEvent,
    Trajectory,
    Treatment,
    TrialWeight,
    agg,
    all_of,
    asof,
    event_time,
    from_json,
    gt,
    quantile,
    to_json,
)
from terrine.windows import Window
from tests.fixtures.readme import readme_model

_HISTORY = datetime(2024, 1, 1)
_METRIC = agg("orders", window=("-1d", "0s"))

CASES = [
    # anchors
    (
        "terrine.design.invalid",
        lambda: EventAnchor("purchase", which="last"),
        {"reason": "unsupported_value", "field": "which", "choices": ("first", "each", "nth")},
    ),
    (
        "terrine.design.invalid",
        lambda: FixedDate(cast(Any, None)),
        {"reason": "wrong_type", "field": "date"},
    ),
    (
        "terrine.design.invalid",
        lambda: Schedule("1d", "2025-01-01T00:00:00Z", "2025-02-01"),
        {"reason": "invalid_date", "field": "start"},
    ),
    (
        "terrine.design.invalid",
        lambda: EventAnchor("purchase", offset="-1s"),
        {"reason": "negative", "field": "offset"},
    ),
    ("terrine.design.invalid", lambda: AnchorRange(), {"reason": "no_bound", "field": "start"}),
    (
        "terrine.design.invalid",
        lambda: AnchorRange(start=datetime(2025, 1, 1)),
        {"reason": "naive_datetime", "field": "start"},
    ),
    # design and treatment
    (
        "terrine.design.invalid",
        lambda: Treatment(""),
        {"reason": "empty_name", "field": "attribute"},
    ),
    (
        "terrine.design.invalid",
        lambda: Design("user", FixedDate("2025-01-01"), gap="-1d"),
        {"reason": "negative", "field": "gap"},
    ),
    (
        "terrine.design.invalid",
        lambda: Design("user", FixedDate("2025-01-01"), features={"x": cast(Any, "orders")}),
        {"reason": "wrong_type", "field": "features", "name": "x"},
    ),
    (
        "terrine.design.invalid",
        lambda: Design("user", Schedule("1d", "2025-01-01", "2025-01-02"), shape=TimeToEvent("x")),
        {"reason": "single_entry_required", "field": "anchor"},
    ),
    # expressions
    (
        "terrine.design.invalid",
        lambda: asof("", before="0s"),
        {"reason": "empty_name", "field": "attribute"},
    ),
    (
        "terrine.design.invalid",
        lambda: agg("orders", window=cast(Any, ("-1d",))),
        {"reason": "not_a_pair", "field": "window"},
    ),
    (
        "terrine.design.invalid",
        lambda: quantile("orders", cast(Any, 0.5), window=("-1d", "0s")),
        {"reason": "wrong_type", "field": "probability"},
    ),
    (
        "terrine.design.invalid",
        lambda: event_time("purchase", window=("-1d", "0s"), occurrence="last", n=1),
        {"reason": "n_requires_nth", "field": "n"},
    ),
    (
        "terrine.design.invalid",
        lambda: gt(_METRIC, cast(Any, object())),
        {"reason": "not_json_scalar", "field": "value"},
    ),
    ("terrine.design.invalid", lambda: all_of(), {"reason": "empty", "field": "terms"}),
    (
        "terrine.design.invalid",
        lambda: SketchRequest("tdigest", "1", Decimal(0)),
        {"reason": "not_positive", "field": "rank_error"},
    ),
    # trajectories, cohorts and time to event
    ("terrine.design.invalid", lambda: TimeToEvent(""), {"reason": "empty_name", "field": "event"}),
    (
        "terrine.design.invalid",
        lambda: Trajectory("spend", "1d", 0),
        {"reason": "not_positive_integer", "field": "periods"},
    ),
    (
        "terrine.design.invalid",
        lambda: LifeTable(TimeToEvent("churn"), "1d", group_by=("t0",)),
        {"reason": "reserved_name", "field": "group_by", "names": ("t0",)},
    ),
    (
        "terrine.design.invalid",
        lambda: CohortGrouping("7d", datetime(2025, 1, 1)),
        {"reason": "naive_datetime", "field": "bucket_origin"},
    ),
    # sequential and event panels
    (
        "terrine.design.invalid_treatment_history",
        lambda: Initiation("upgrade", history_start=_HISTORY),
        {"reason": "naive_datetime", "field": "history_start"},
    ),
    (
        "terrine.design.invalid",
        lambda: TrialWeight("explicit", {"a": float("nan")}),
        {"reason": "not_positive", "field": "values"},
    ),
    (
        "terrine.design.invalid",
        lambda: MatchedT0Sample(("region",), 1, -1),
        {"reason": "not_nonnegative_integer", "field": "seed"},
    ),
    (
        "terrine.design.invalid",
        lambda: EventPanelSpec((0, 1), 86_400, -1),
        {"reason": "undeclared_name", "field": "reference_period"},
    ),
    (
        "terrine.design.invalid",
        lambda: CalendarComparisonSpec(
            Initiation("upgrade", history_start=datetime(2024, 1, 1, tzinfo=UTC)),
            cast(Any, "everyone"),
            -2,
            (0,),
        ),
        {"reason": "unsupported_value", "field": "control"},
    ),
    (
        "terrine.design.invalid",
        lambda: Sequential(
            cast(Any, None),
            treatment=Initiation("upgrade", history_start=datetime(2024, 1, 1, tzinfo=UTC)),
        ),
        {"reason": "wrong_type", "field": "base"},
    ),
    # state panels
    (
        "terrine.state_panel.invalid_state_source",
        lambda: StateObservations("health", ()),
        {"reason": "too_few_levels", "field": "levels"},
    ),
    (
        "terrine.state_panel.invalid_state_source",
        lambda: BinnedMetricState("orders", (2, 1), ("low", "mid", "high")),
        {"reason": "not_increasing", "field": "cutpoints"},
    ),
    (
        "terrine.state_panel.invalid_state_source",
        lambda: StatePanel(StateObservations("health", ("well", "ill")), max_periods=0),
        {"reason": "not_positive_integer", "field": "max_periods"},
    ),
    # sequences (already coded) and dataset definitions
    (
        "terrine.sequence.invalid_spec",
        lambda: EventSequence("purchase", Window("-1d", "0d"), max_events=0),
        {},
    ),
    (
        "terrine.dataset.invalid_definition",
        lambda: DatasetDefinition(schema_version=2, base_design=cast(Any, None)),
        {"reason": "wrong_type", "field": "base_design"},
    ),
    # design documents
    ("terrine.design.invalid", lambda: from_json(b"{"), {"reason": "malformed_document"}),
    (
        "terrine.design.invalid",
        lambda: from_json('{"schema_version":2,"design":{"unit":"u","anchor":{"type":"Future"}}}'),
        {"reason": "malformed_document"},
    ),
    (
        "terrine.design.invalid",
        lambda: to_json(cast(Any, None)),
        {"reason": "wrong_type", "field": "design"},
    ),
    # split policies
    (
        "terrine.dataset.invalid_split",
        lambda: TimeSplit(train_until="2025-01-02", validation_until="2025-01-01"),
        {"reason": "validation_not_after_train", "field": "validation_until"},
    ),
    (
        "terrine.dataset.invalid_split",
        lambda: TimeSplit(train_until=datetime(2025, 1, 1)),
        {"reason": "naive_datetime", "field": "train_until"},
    ),
    (
        "terrine.dataset.invalid_split",
        lambda: TimeSplit(train_until="2025-01-01", embargo="-1d"),
        {"reason": "invalid_duration", "field": "embargo"},
    ),
    (
        "terrine.dataset.invalid_split",
        lambda: GroupSplit(train_fraction=1.5),
        {"reason": "out_of_range", "field": "train_fraction"},
    ),
    # budgets
    (
        "terrine.budget.invalid_limit",
        lambda: Budget(max_rows=-1),
        {"reason": "not_nonnegative_integer", "field": "max_rows"},
    ),
    (
        "terrine.budget.invalid_limit",
        lambda: Budget(strict_scan=cast(Any, 1)),
        {"reason": "wrong_type", "field": "strict_scan"},
    ),
    # model binding inputs
    (
        "terrine.model.invalid_binding",
        lambda: Coverage("2025-01-31", "2025-01-01"),
        {"reason": "through_before_start", "field": "through"},
    ),
    (
        "terrine.model.invalid_binding",
        lambda: Coverage("2025-13-01", "2025-12-31"),
        {"reason": "invalid_date", "field": "start"},
    ),
    (
        "terrine.model.invalid_binding",
        lambda: BoundCoverage(datetime(2025, 1, 1), datetime(2025, 1, 2, tzinfo=UTC)),
        {"reason": "naive_datetime", "field": "start"},
    ),
    (
        "terrine.model.invalid_binding",
        lambda: UnitSource("", "id"),
        {"reason": "empty_name", "field": "table"},
    ),
    (
        "terrine.model.invalid_binding",
        lambda: UnitSource(pa.table({}), "id"),
        {"reason": "empty_table", "field": "table"},
    ),
    (
        "terrine.model.invalid_binding",
        lambda: Events("orders", time="at", key="id", labels=("status", "status")),
        {"reason": "duplicate_names", "field": "labels"},
    ),
    (
        "terrine.model.invalid_binding",
        lambda: Events("orders", time="at", key="id", complete=cast(Any, ("2025-01-01",))),
        {"reason": "not_a_pair", "field": "complete"},
    ),
    (
        "terrine.model.invalid_binding",
        lambda: History("plans", key="", column="plan", changed_at="at"),
        {"reason": "empty_name", "field": "key"},
    ),
]


@pytest.mark.parametrize(("code", "build", "context"), CASES)
def test_public_constructor_refusals_are_coded_and_portable(code, build, context):
    with pytest.raises(TerrineError) as caught:
        build()
    error = caught.value
    assert error.code == code
    assert {key: error.context.get(key) for key in context} == context
    with pytest.raises(TypeError):
        cast(dict[str, Any], error.context)["reason"] = "changed"
    for restored in (copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
        assert restored.code == code
        assert restored.context == error.context
        assert str(restored) == str(error)


def test_missing_reference_lists_available_names(readme_con):
    design = Design(
        "customer",
        FixedDate("2025-02-01"),
        features={"x": agg("orders_cout", window=("-1d", "0s"))},
    )
    with pytest.raises(TerrineError) as caught:
        design.validate(readme_model(readme_con))
    assert caught.value.code == "terrine.design.missing_reference"
    assert caught.value.context["available"] == ("orders", "spend")
