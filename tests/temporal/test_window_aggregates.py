from __future__ import annotations

import math
import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from fractions import Fraction

import ibis
import pyarrow as pa
import pytest

from terrine.backends.numeric import exact_literal
from terrine.data import BoundCoverage, RowCheck
from terrine.errors import RefusalSpec, TerrineError
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import (
    BoundFilter,
    BoundMeasure,
    BoundMetric,
    BoundSource,
    PropertyBinding,
    TableRef,
)
from terrine.temporal.daily import resolve_measure
from terrine.temporal.readiness import Readiness
from terrine.windows import Window
from tests._float_bound import ratio_bound, window_sum_bound

_DEFAULT_WINDOW = Window(0, "1d")
_REFUSAL = RefusalSpec("terrine.test.failed_rows", "{failed_count} rows failed")


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


def _local(
    con,
    values,
    *,
    aggregation="sum",
    dtype="float64",
    times=None,
    units=None,
    name="postings",
    boundary="UTC",
    active=None,
    exact=True,
):
    """Resolve a measure against real normalized source events, without aggregation."""
    n = len(values)
    fields = {"user_id": "string", "occurred_at": "timestamp('UTC')", "amount": dtype}
    data = {
        "user_id": units or ["a"] * n,
        "occurred_at": times or [datetime(2025, 1, 2, 12, tzinfo=UTC)] * n,
        "amount": values,
    }
    properties, filters = {}, ()
    if active is not None:
        fields["active"] = "string"
        data["active"] = active
        properties["active"] = PropertyBinding("active", "active", "string", "event_time", None)
        filters = (BoundFilter("active", "equals", ("yes",)),)
    raw = con.create_table(
        name, pa.Table.from_pydict(data, schema=ibis.schema(fields).to_pyarrow()), overwrite=True
    )
    source = BoundSource(
        name,
        TableRef(kind="table", name=name),
        "occurred_at",
        {"user": "user_id"},
        {"revenue": "amount"},
        properties,
        (),
        "duckdb",
        boundary,
    )
    events = normalize_events(raw, source, "user_id")
    measure = resolve_measure(
        events,
        BoundMeasure("revenue", name, aggregation, filters, None, None, exact=exact),
        source=source,
    )
    return measure, (events, source)


def _anchors(con, *, time="2025-01-02 00:00:00+00", units=("a",), extra=""):
    values = ",".join(f"('{unit}', '{index}')" for index, unit in enumerate(units))
    return con.sql(
        f"SELECT unit, anchor_id, TIMESTAMPTZ '{time}' AS t0 {extra} "
        f"FROM (VALUES {values}) AS anchors(unit, anchor_id)"
    )


def _result(
    anchors,
    pairs,
    *,
    window=_DEFAULT_WINDOW,
    kind="mean",
    readiness=None,
    boundary="UTC",
    resolution="event",
    output="requested",
):
    """Run one request through the same local reducer and finalizer as a batch."""
    return _batch(
        anchors,
        [(output, pairs, window, kind)],
        readiness=readiness,
        boundary=boundary,
        resolution=resolution,
    )


def _batch(anchors, specifications, *, readiness=None, boundary="UTC", resolution="event"):
    from terrine.temporal.aggregate import AggregateRequest, aggregate_windows

    requests, sources = [], {}
    for output, pairs, window, kind in specifications:
        measures = tuple(pair[0] for pair in pairs)
        metric = BoundMetric(
            output,
            "user",
            {m.source_name: "user_id" for m in measures},
            kind,
            measures,
            tuple(m.source_name for m in measures),
            (),
        )
        requests.append(AggregateRequest(metric, window, output, "outcome"))
        sources.update({m.source_name: source for m, source in pairs})
    if readiness is None:
        readiness = Readiness(
            {
                name: BoundCoverage(
                    datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 5, tzinfo=UTC)
                )
                for name in sources
            }
        )
    return aggregate_windows(
        anchors, requests, sources, readiness, boundary=boundary, resolution=resolution
    )


@pytest.mark.parametrize("resolution", ["event", "day"])
def test_batch_keeps_nonnull_denominators_half_open_bounds_and_anchor_instances(con, resolution):
    # Catches count reuse as a nullable mean's denominator and inclusive end bounds.
    t0 = datetime(2025, 1, 1, 18, 30, tzinfo=UTC)
    pair = _local(
        con,
        [100, 2, None, 3, 100],
        dtype="int64",
        times=[t0 - timedelta(microseconds=1), t0, t0, t0, t0 + timedelta(days=1)],
    )
    measure, source = pair
    result = _batch(
        _anchors(con, time=t0.isoformat(), units=("a", "a", "empty")),
        [
            ("counted", [(replace(measure, aggregation="count"), source)], _DEFAULT_WINDOW, "mean"),
            (
                "average",
                [(replace(measure, aggregation="avg_event"), source)],
                _DEFAULT_WINDOW,
                "mean",
            ),
            ("total", [pair], _DEFAULT_WINDOW, "mean"),
            ("wide", [pair], Window(0, "2d"), "mean"),
        ],
        boundary="UTC+05:30",
        resolution=resolution,
    )
    rows = sorted(_rows(con, result), key=lambda row: row["anchor_id"])
    assert [(r["counted"], r["average"], r["total"]) for r in rows] == [
        (3, 2.5, Decimal(5)),
        (3, 2.5, Decimal(5)),
        (0, None, Decimal(0)),
    ]
    assert [r[result.columns["average"].reason_column] for r in rows] == [
        None,
        None,
        "no_observations",
    ]
    assert all(r[result.columns["total"].reason_column] is None for r in rows)
    assert [row["wide"] for row in rows] == [Decimal(105), Decimal(105), Decimal(0)]


@pytest.mark.parametrize("exact", [False, True])
@pytest.mark.parametrize("resolution", ["event", "day"])
def test_batch_masks_unavailable_outputs_and_count_only_inputs_before_arithmetic(
    con, exact, resolution
):
    # Catches a wide count or unavailable output poisoning a narrower sum.
    pair = _local(
        con,
        [1e308, 2.0, float("nan"), 1e308],
        times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 2, 3, 4)],
        exact=exact,
    )
    measure, source = pair
    anchors = _anchors(con, extra=", TIMESTAMPTZ '2025-01-04 00:00:00+00' AS _observed_until")
    result = _batch(
        anchors,
        [
            ("narrow", [pair], _DEFAULT_WINDOW, "mean"),
            (
                "wide_count",
                [(replace(measure, aggregation="count"), source)],
                Window(0, "2d"),
                "mean",
            ),
            ("unavailable", [pair], Window(0, "3d"), "mean"),
        ],
        resolution=resolution,
    )
    (row,) = _rows(con, result)
    assert (row["narrow"], row["wide_count"], row["unavailable"]) == (2.0, 2, None)
    assert row[result.columns["narrow"].reason_column] is None
    assert row[result.columns["unavailable"].reason_column] == "incomplete_observation"
    assert row[result.columns["narrow"].observed_until_column] == datetime(2025, 1, 4, tzinfo=UTC)


@pytest.mark.parametrize("exact", [False, True])
@pytest.mark.parametrize("resolution", ["event", "day"])
def test_batch_selected_invalid_and_unknown_filter_candidates_still_refuse(con, exact, resolution):
    # Catches dropping unknown predicates or hiding invalid available candidates.
    pair = _local(con, [float("nan")], exact=exact)
    result = _batch(
        _anchors(con), [("bad", [pair], _DEFAULT_WINDOW, "mean")], resolution=resolution
    )
    with pytest.raises(TerrineError) as error:
        _rows(con, result)
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["reason"] == "nonfinite_input"
    assert set(error.value.context) == {"reason", "failed_count", "source", "measure", "output"}
    pair = _local(con, [1], dtype="int64", active=[None])
    result = _batch(
        _anchors(con), [("unknown", [pair], _DEFAULT_WINDOW, "mean")], resolution=resolution
    )
    with pytest.raises(TerrineError) as error:
        _rows(con, result)
    assert error.value.code == "terrine.filter.unknown"


@pytest.mark.parametrize("resolution", ["event", "day"])
@pytest.mark.parametrize("policy", ["duckdb", "generic"])
def test_batch_quantizes_each_input_and_preserves_exact_output_domain(con, resolution, policy):
    from terrine.backends.dialect import dialect_scope

    # Third exact source also catches bounded guard state escaping into source joins.
    with dialect_scope(policy):
        small = _local(con, [4e-10, 4e-10], name="small")
        wide = _local(con, [2**63 - 1, 2**63 - 1], dtype="int64", name="wide")
        plain = _local(con, [2, 3], dtype="int64", name="plain")
        native = _local(con, [4e-10, 4e-10], name="native", exact=False)
        result = _batch(
            _anchors(con),
            [
                ("small", [small], _DEFAULT_WINDOW, "mean"),
                ("wide", [wide], _DEFAULT_WINDOW, "mean"),
                ("plain", [plain], _DEFAULT_WINDOW, "mean"),
                ("native", [native], _DEFAULT_WINDOW, "mean"),
            ],
            resolution=resolution,
        )
        (row,) = _rows(con, result)
    assert row["small"] == 0.0
    assert row[result.columns["small"].reason_column] == "below_resolution"
    assert row["wide"] == Decimal(2**64 - 2)
    assert row["plain"] == Decimal(5)
    assert row["native"] == pytest.approx(8e-10, rel=1e-15)
    assert row[result.columns["native"].reason_column] is None


def test_daily_scope_excludes_unused_days_and_units(con):
    # An envelope or missing unit key would evaluate overflowing unused buckets.
    pair = _local(
        con,
        [2.0, 1e308, 1e308, 3.0, float("nan")],
        times=[datetime(2025, 1, day, 12, tzinfo=UTC) for day in (1, 2, 2, 3, 1)],
        units=["a", "a", "a", "a", "unrelated"],
        exact=False,
    )
    anchors = con.sql(
        "SELECT 'a' AS unit, id AS anchor_id, t0 FROM (VALUES "
        "('first', TIMESTAMPTZ '2025-01-01'), "
        "('last', TIMESTAMPTZ '2025-01-03')) AS a(id, t0)"
    )
    result = _batch(anchors, [("value", [pair], _DEFAULT_WINDOW, "mean")], resolution="day")
    rows = sorted(_rows(con, result), key=lambda row: row["anchor_id"])
    assert [row["value"] for row in rows] == [2.0, 3.0]
    assert all(row[result.columns["value"].reason_column] is None for row in rows)


def _rows(con, result):
    con.raw_sql("BEGIN TRANSACTION")
    try:
        result.validate(con.to_pyarrow)
        table = con.to_pyarrow(result.table)
        table.validate(full=True)
        return table.to_pylist()
    finally:
        con.raw_sql("ROLLBACK")


def _exact_sum(values):
    """The exact sum of the original inputs, including their binary64 values."""
    return sum((Fraction(value) for value in values), Fraction())


def _assert_quotient(returned, numerator: Fraction, denominator: Fraction, e_n=0.0, e_d=0.0):
    bound = ratio_bound(float(numerator), float(denominator), e_n, e_d)
    assert bound is not None
    assert abs(Fraction(returned) - numerator / denominator) <= bound


def test_long_history_small_window_meets_quantized_bound(con):
    generator = random.Random(2026)
    history = [generator.uniform(0, 10000) for _ in range(1000)]
    window = [1.25, 2.5, 3.16]
    first = datetime(2025, 1, 1, tzinfo=UTC)
    pair = _local(
        con,
        history + window,
        times=[first + timedelta(seconds=index) for index in range(len(history))]
        + [datetime(2025, 1, 2, 12, tzinfo=UTC)] * len(window),
    )
    result = _result(_anchors(con), [pair])
    row = _rows(con, result)[0]
    assert abs(Fraction(row["requested"]) - _exact_sum(window)) <= window_sum_bound(window)
    meta = result.columns["requested"]
    assert meta.numeric_policy is None


def test_float_window_selects_value_beyond_excluded_binary64_history(con):
    pair = _local(con, [1e16, 1.0], times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 2)])
    assert _rows(con, _result(_anchors(con), [pair]))[0]["requested"] == 1.0


def test_tiny_value_does_not_change_other_units(con):
    pair = _local(
        con,
        [1.0, 5e-324, -1.0, 0.1, 0.2],
        units=["tiny", "tiny", "tiny", "other", "other"],
    )
    result = _result(_anchors(con, units=("tiny", "other")), [pair])
    rows = sorted(_rows(con, result), key=lambda row: row["unit"])
    for row, values in zip(rows, ([0.1, 0.2], [1.0, 5e-324, -1.0]), strict=True):
        assert abs(Fraction(row["requested"]) - _exact_sum(values)) <= window_sum_bound(values)


@pytest.mark.parametrize("exact", [True, False])
def test_nonfinite_history_is_excluded_from_available_window(con, exact):
    pair = _local(
        con,
        [float("nan"), 2.0],
        times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 2)],
        exact=exact,
    )
    result = _result(_anchors(con), [pair])
    (row,) = _rows(con, result)
    assert row["requested"] == 2.0
    assert row[result.columns["requested"].reason_column] is None
    assert row[result.columns["requested"].complete_column] is True


@pytest.mark.parametrize("dialect", ["duckdb", "generic"])
def test_native_selected_overflow_refuses_without_poisoning_later_windows(con, dialect):
    """Nonfinite selected sums refuse; excluded earlier arithmetic is irrelevant."""
    from terrine.backends.dialect import dialect_scope

    with dialect_scope(dialect):
        pair = _local(
            con,
            [1.0, 1.7e308, 1.7e308, 1.0],
            times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (2, 3, 3, 4)],
            exact=False,
        )
        for time in ("2025-01-02 00:00:00+00", "2025-01-04 00:00:00+00"):
            assert _rows(con, _result(_anchors(con, time=time), [pair]))[0]["requested"] == 1.0
        with pytest.raises(TerrineError) as error:
            _rows(con, _result(_anchors(con, time="2025-01-03 00:00:00+00"), [pair]))
        assert error.value.code == "terrine.temporal.float64_precision"
        assert error.value.context["reason"] == "bound_overflow"


@pytest.mark.parametrize(
    "numerator,denominator,aggregation,kind,expected",
    [
        # Ordinary native division may underflow, but cannot publish infinity.
        ([5e-324, 0.0], None, "avg_event", "mean", 0.0),
        ([1e300], [1e-10], "sum", "ratio", "bound_overflow"),
        ([0.0], [1e-300], "sum", "ratio", 0.0),
        ([1e-300], [1e-10], "sum", "ratio", 1e-290),
    ],
)
def test_native_quotient_underflows_or_refuses_nonfinite_result(
    con, numerator, denominator, aggregation, kind, expected
):
    from terrine.backends.dialect import dialect_scope

    with dialect_scope("generic"):
        pairs = [_local(con, numerator, aggregation=aggregation, name="numerator", exact=False)]
        if denominator is not None:
            pairs.append(_local(con, denominator, name="denominator", exact=False))
        result = _result(_anchors(con), pairs, kind=kind)
        if isinstance(expected, str):
            with pytest.raises(TerrineError) as error:
                _rows(con, result)
            assert error.value.code == "terrine.temporal.float64_precision"
            assert error.value.context["reason"] == expected
        else:
            value = _rows(con, result)[0]["requested"]
            assert value == pytest.approx(expected, rel=2**-50)


def _recording(con):
    executed = []

    def execute(query):
        executed.append(query)
        return con.to_pyarrow(query)

    return executed, execute


def test_validate_runs_one_aggregate_for_every_row_check(con):
    pair = _local(con, [float("nan"), 2.0])
    result = _result(_anchors(con), [pair])
    assert len(result.row_checks) >= 2
    assert all(check.column in result.table.columns for check in result.row_checks)
    executed, execute = _recording(con)
    with pytest.raises(TerrineError) as error:
        result.validate(execute)
    assert len(executed) == 1
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["reason"] == "nonfinite_input"
    assert error.value.context["failed_count"] == 1


def test_row_check_columns_are_true_only_on_failing_rows(con):
    pair = _local(con, [float("nan"), 2.0], units=["bad", "good"])
    result = _result(_anchors(con, units=("bad", "good")), [pair])
    nonfinite = next(
        check for check in result.row_checks if check.context.get("reason") == "nonfinite_input"
    )
    table = result.table
    rows = con.to_pyarrow(
        table.select("unit", flag=table[nonfinite.column]).order_by("unit")
    ).to_pylist()
    assert rows == [{"unit": "bad", "flag": True}, {"unit": "good", "flag": False}]


@pytest.mark.parametrize(
    "aggregation,kind,expected,reasons,exact",
    [
        ("sum", "mean", [0, None], [None, "all_values_null"], True),
        ("sum", "mean", [0, None], [None, "all_values_null"], False),
        ("count", "mean", [0, 1], [None, None], True),
        ("sum", "conversion", [False, True], [None, None], True),
        ("avg_event", "mean", [None, None], ["no_observations", "all_values_null"], True),
        ("avg_event", "mean", [None, None], ["no_observations", "all_values_null"], False),
        ("avg_calendar_day", "mean", [0.0, None], [None, "all_values_null"], True),
        ("avg_calendar_day", "mean", [0.0, None], [None, "all_values_null"], False),
    ],
)
def test_complete_empty_and_all_null_remain_distinct(
    con, aggregation, kind, expected, reasons, exact
):
    pair = _local(
        con, [None], dtype="float64", aggregation=aggregation, units=["null"], exact=exact
    )
    result = _result(_anchors(con, units=("empty", "null")), [pair], kind=kind)
    rows = sorted(_rows(con, result), key=lambda r: r["unit"])
    meta = result.columns["requested"]
    assert [r["requested"] for r in rows] == expected
    assert [r[meta.reason_column] for r in rows] == reasons


def test_unit_observation_end_preserves_value_or_exact_unavailable_reason(con):
    pair = _local(con, [2, 2], dtype="int64", units=["complete", "incomplete"])
    anchors = _anchors(con, units=("complete", "incomplete"))
    anchors = anchors.mutate(
        _observed_until=(anchors.unit == "complete").ifelse(
            ibis.timestamp(datetime(2025, 1, 3, tzinfo=UTC)),
            ibis.timestamp(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        )
    )
    result = _result(anchors, [pair])
    meta = result.columns["requested"]
    rows = sorted(_rows(con, result), key=lambda row: row["unit"])
    assert [
        (row["unit"], row["requested"], row[meta.complete_column], row[meta.reason_column])
        for row in rows
    ] == [
        ("complete", Decimal(2), True, None),
        ("incomplete", None, False, "incomplete_observation"),
    ]


@pytest.mark.parametrize(
    "time,window",
    [
        ("2025-01-02 00:00:00+00", Window(0, "1h")),
        ("2025-01-02 12:00:00+00", Window(0, "1d")),
        ("2025-01-02 00:00:00.000001+00", Window(0, "1d")),
        ("2025-01-02 00:00:00+00", Window("1s", "86401s")),
    ],
)
def test_calendar_domain_precedes_empty_and_unavailable(con, time, window):
    pair = _local(con, [None], aggregation="avg_calendar_day", units=["other"])
    readiness = Readiness(
        {
            "postings": BoundCoverage(
                datetime(2025, 1, 4, tzinfo=UTC), datetime(2025, 1, 5, tzinfo=UTC)
            )
        }
    )
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(_anchors(con, time=time), [pair], window=window, readiness=readiness))
    assert error.value.code == "terrine.metric.unaligned_calendar_average"


def test_exact_window_excludes_wide_history_and_retains_decimal_scale(con):
    maximum = Decimal("99999999999999999999999999999999.999999")
    pair = _local(
        con,
        [maximum, maximum, maximum.copy_negate(), Decimal("0.000001")],
        dtype="decimal(38,6)",
        times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 1, 2, 2)],
    )
    result = _result(_anchors(con), [pair])
    row = _rows(con, result)[0]
    expected = Decimal("-99999999999999999999999999999999.999998")
    assert row["requested"] == expected
    assert row["requested"].as_tuple().exponent == -6
    assert result.columns["requested"].numeric_policy == "exact"
    overflow = _result(_anchors(con, time="2025-01-01 00:00:00+00"), [pair])
    with pytest.raises(TerrineError) as error:
        _rows(con, overflow)
    assert error.value.code == "terrine.temporal.exact_overflow"


def test_unknown_filter_is_window_local_and_unavailable_suppresses_refusal(con):
    pair = _local(
        con,
        [1, 2],
        dtype="int64",
        active=[None, "yes"],
        times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 2)],
    )
    assert _rows(con, _result(_anchors(con), [pair]))[0]["requested"] == 2
    anchors = _anchors(con, time="2025-01-01 00:00:00+00")
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(anchors, [pair]))
    assert error.value.code == "terrine.filter.unknown"
    unavailable = _result(
        anchors,
        [pair],
        readiness=Readiness(
            {
                "postings": BoundCoverage(
                    datetime(2025, 1, 2, tzinfo=UTC), datetime(2025, 1, 5, tzinfo=UTC)
                )
            }
        ),
    )
    assert (
        _rows(con, unavailable)[0][unavailable.columns["requested"].reason_column]
        == "incomplete_coverage"
    )


def test_count_and_conversion_ignore_nonfinite_float_inputs(con):
    pair = _local(con, [float("inf")])
    conversion = _result(_anchors(con), [pair], kind="conversion")
    assert _rows(con, conversion)[0]["requested"] is True
    assert conversion.columns["requested"].numeric_policy is None
    measure, source = pair
    count = _result(_anchors(con), [(replace(measure, aggregation="count"), source)])
    assert _rows(con, count)[0]["requested"] == 1


def test_count_metric_matches_sum_of_ones_and_has_no_unknown_check(con):
    from terrine.semantic.rules import UNKNOWN_FILTER

    pair = _local(con, [1.0, 2.0, 3.0], aggregation="count")
    result = _result(_anchors(con), [pair])
    assert _rows(con, result)[0]["requested"] == 3
    assert all(check.refusal is not UNKNOWN_FILTER for check in result.row_checks)


@pytest.mark.parametrize(
    "bad",
    [
        None,
        pa.array([None], type=pa.int64()),
        pa.array([-1]),
        pa.array(["0"]),
        pa.array([0.0]),
        pa.array([0, 0]),
    ],
)
def test_deferred_scalar_checks_fail_closed(con, bad):
    pair = _local(con, [1.0])
    result = _result(_anchors(con), [pair])

    def malformed(query):
        if bad is None:
            return None
        columns = {name: pa.array([0] * len(bad), type=pa.int64()) for name in query.columns}
        columns[query.columns[0]] = bad
        return pa.table(columns)

    with pytest.raises((TypeError, ValueError)):
        result.validate(malformed)


@pytest.mark.parametrize(
    "bad",
    [
        None,
        pa.array([None], type=pa.string()),
        pa.array(["-1"]),
        pa.array(["00"]),
        pa.array([0], type=pa.int64()),
        pa.array(["0", "0"]),
    ],
)
def test_aggregate_string_counts_fail_closed(con, bad):
    from terrine.temporal.aggregate import _Check

    anchors = _anchors(con)
    check = _Check(
        anchors.aggregate(failed_0=anchors.count().cast("string")),
        (RowCheck("failed_0", _REFUSAL, {}),),
    )

    def malformed(query):
        if bad is None:
            return None
        return pa.table({query.columns[0]: bad})

    with pytest.raises((TypeError, ValueError)):
        check.validate(malformed)


def test_integer_sum_widens_without_losing_units_above_binary64_precision(con):
    pair = _local(con, [2**63 - 1, 2**63 - 1], dtype="int64")
    result = _result(_anchors(con), [pair])
    row = _rows(con, result)[0]
    assert row["requested"] == Decimal(2**64 - 2)
    assert result.columns["requested"].numeric_policy == "exact"


def _finalized_counts(con, counts, *, kind="mean"):
    """Exercise count-domain publication directly, without fabricated source rows."""
    from terrine.temporal.aggregate import _Builder, _finalize_window, _local_component

    anchors = _anchors(con)
    builder = _Builder(anchors.mutate(count=exact_literal(counts)), "requested", practical=True)
    measure = BoundMeasure("revenue", "postings", "count", (), None, None)
    available = builder.put("available", ibis.literal(True))
    unavailable = builder.put("unavailable", ibis.null("string"))
    observed_from = builder.put("observed_from", anchors.t0)
    observed_until = builder.put("observed_until", anchors.t0 + ibis.interval(days=1))
    quantity = _local_component(
        builder,
        measure,
        {"event_count": "count"},
        days=1,
        conversion=False,
        context={"source": "postings", "measure": "revenue", "output": "requested"},
    )
    measures = (measure, measure) if kind == "ratio" else (measure,)
    metric = BoundMetric(
        "revenue", "user", {"postings": "user_id"}, kind, measures, ("postings",), ()
    )
    return _finalize_window(
        builder,
        anchors,
        metric,
        _DEFAULT_WINDOW,
        "requested",
        "outcome",
        [quantity] * len(measures),
        available,
        unavailable,
        observed_from,
        observed_until,
        [],
        (),
    )


def test_count_finalization_refuses_outside_int64_instead_of_wrapping(con):
    assert _rows(con, _finalized_counts(con, 2**63 - 1))[0]["requested"] == 2**63 - 1
    for count in (-1, 2**63, 2**127 - 1):
        with pytest.raises(TerrineError) as error:
            _rows(con, _finalized_counts(con, count))
        assert error.value.code == "terrine.temporal.exact_overflow"


def test_day_resolution_cannot_answer_an_intraday_request(con):
    pair = _local(con, [2], dtype="int64")
    result = _result(_anchors(con, time="2025-01-02 12:00:00+00"), [pair], resolution="day")
    with pytest.raises(TerrineError) as error:
        _rows(con, result)
    assert error.value.code == "terrine.temporal.unaligned_day_resolution"


@pytest.mark.parametrize("exact", [True, False])
def test_small_window_after_large_history_keeps_selected_values(con, exact):
    history, window = [1e12] * 1000, [0.01, 0.01, 0.01]
    pair = _local(
        con,
        history + window,
        times=[datetime(2025, 1, 1, 12, tzinfo=UTC)] * 1000
        + [datetime(2025, 1, 2, 12, tzinfo=UTC)] * 3,
        exact=exact,
    )
    value = _rows(con, _result(_anchors(con), [pair]))[0]["requested"]
    if exact:
        assert abs(Fraction(value) - sum(map(Fraction, window))) <= window_sum_bound(window)
    else:
        # Three positive selected inputs need only a few binary64 rounding units.
        assert value == pytest.approx(0.03, rel=1e-15, abs=0)


@pytest.mark.parametrize(
    "value,expected",
    [
        (4e-10, 0.0),
        (-4e-10, 0.0),
        (6e-10, 1e-9),
        (-6e-10, -1e-9),
        (1 / 1024, 0.000976563),
        (-1 / 1024, -0.000976563),
    ],
)
def test_quantization_rounds_to_nearest_nano_unit(con, value, expected):
    pair = _local(con, [value])
    actual = _rows(con, _result(_anchors(con), [pair]))[0]["requested"]
    assert actual == pytest.approx(expected, rel=1e-15, abs=1e-24)
    assert abs(Fraction(actual) - Fraction(value)) <= window_sum_bound([value])


@pytest.mark.parametrize(
    "values,aggregation,expected,reason",
    [
        # Every nonzero contribution quantizes to zero: the exact sum is zero and says so.
        ([1e-10, 2e-10, 4e-10], "sum", Fraction(0), "below_resolution"),
        # A mixed window publishes the retained sum and still notes the loss.
        ([4e-10, 1.0], "sum", Fraction(1), "below_resolution"),
        ([4e-10, 1.0], "avg_event", Fraction(1, 2), "below_resolution"),
        # Exact zeros are not discarded contributions.
        ([0.0, -0.0], "sum", Fraction(0), None),
        # A rounded contribution is retained, not discarded.
        ([6e-10, 1.0], "sum", Fraction(1) + Fraction(1, 10**9), None),
    ],
)
def test_exact_sum_notes_discarded_sub_resolution_contributions(
    con, values, aggregation, expected, reason
):
    pair = _local(con, values, aggregation=aggregation, exact=True)
    result = _result(_anchors(con), [pair])
    (row,) = _rows(con, result)
    meta = result.columns["requested"]
    divisor = len(values) if aggregation == "avg_event" else 1
    assert abs(Fraction(row["requested"]) - expected) <= window_sum_bound(values) / divisor
    assert row[meta.reason_column] == reason
    # The note describes the published value; it does not make the window incomplete.
    assert row[meta.complete_column] is True


def test_sub_resolution_note_counts_only_the_window_itself(con):
    """Excluded discarded terms do not note a later window; selected ones do."""
    pair = _local(
        con,
        [4e-10, 1.0],
        times=[datetime(2025, 1, 1, 12, tzinfo=UTC), datetime(2025, 1, 2, 12, tzinfo=UTC)],
        exact=True,
    )
    later = _result(_anchors(con), [pair])
    (row,) = _rows(con, later)
    assert (row["requested"], row[later.columns["requested"].reason_column]) == (1.0, None)
    earlier = _result(_anchors(con, time="2025-01-01 00:00:00+00"), [pair])
    (row,) = _rows(con, earlier)
    reason = row[earlier.columns["requested"].reason_column]
    assert (row["requested"], reason) == (0.0, "below_resolution")


def test_ratio_carries_an_operand_note_and_keeps_blocking_reasons_first(con):
    numerator = _local(con, [4e-10, 1.0], name="numerator", exact=True)
    denominator = _local(con, [2.0], name="denominator", exact=True)
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    (row,) = _rows(con, result)
    assert row["requested"] == 0.5
    assert row[result.columns["requested"].reason_column] == "below_resolution"
    # A quantized zero denominator is unavailable, whatever the numerator noted.
    zero = _local(con, [4e-10, -4e-10], name="denominator", exact=True)
    result = _result(_anchors(con), [numerator, zero], kind="ratio")
    (row,) = _rows(con, result)
    assert row["requested"] is None
    assert row[result.columns["requested"].reason_column] == "zero_denominator"


def test_native_sum_keeps_sub_resolution_contributions_without_a_note(con):
    values = [1e-10, 2e-10, 4e-10]
    pair = _local(con, values, exact=False)
    result = _result(_anchors(con), [pair])
    (row,) = _rows(con, result)
    assert row["requested"] == pytest.approx(7e-10, rel=1e-15, abs=0)
    assert row[result.columns["requested"].reason_column] is None


def test_ratio_with_zero_quantized_denominator_is_null(con):
    numerator = _local(con, [1.0], name="numerator")
    denominator = _local(con, [4e-10, -4e-10, 3e-10], name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    row = _rows(con, result)[0]
    assert row["requested"] is None
    assert row[result.columns["requested"].reason_column] == "zero_denominator"


def test_ratio_meets_propagated_bound(con):
    n, d = [0.1234567894, 2.0000000006], [0.3333333334, 1.0]
    pairs = [_local(con, n, name="numerator"), _local(con, d, name="denominator")]
    value = _rows(con, _result(_anchors(con), pairs, kind="ratio"))[0]["requested"]
    bound = ratio_bound(2.12345679, 1.333333333, window_sum_bound(n), window_sum_bound(d))
    assert bound is not None
    assert abs(Fraction(value) - sum(map(Fraction, n)) / sum(map(Fraction, d))) <= bound


def test_per_day_mean_bound_uses_actual_count(con):
    values = [6e-10, 0.1234567894]
    pair = _local(con, values, aggregation="avg_calendar_day")
    value = _rows(con, _result(_anchors(con), [pair], window=Window(0, "3d")))[0]["requested"]
    assert abs(Fraction(value) - sum(map(Fraction, values)) / 3) <= window_sum_bound(values) / 3


@pytest.mark.parametrize("exact", [True, False])
def test_mean_of_float_measure_is_not_rescaled(con, exact):
    pair = _local(con, [1.0], aggregation="avg_event", exact=exact)
    assert _rows(con, _result(_anchors(con), [pair]))[0]["requested"] == pytest.approx(1.0)


def test_mixed_scale_ratio_meets_propagated_bound(con):
    n, d = [Decimal("1.23"), Decimal("4.56")], [0.3333333334, 1.0]
    pairs = [
        _local(con, n, name="numerator", dtype="decimal(10, 2)"),
        _local(con, d, name="denominator"),
    ]
    value = _rows(con, _result(_anchors(con), pairs, kind="ratio"))[0]["requested"]
    bound = ratio_bound(5.79, 1.333333333, 0.0, window_sum_bound(d))
    assert bound is not None
    assert abs(Fraction(value) - sum(map(Fraction, n)) / sum(map(Fraction, d))) <= bound


def test_float_range_boundary_is_exact(con):
    for sign in (1, -1):
        pair = _local(con, [sign * 1e29])
        value = _rows(con, _result(_anchors(con), [pair]))[0]["requested"]
        assert abs(Fraction(value) - Fraction(sign * 1e29)) <= window_sum_bound([sign * 1e29])
        pair = _local(con, [sign * math.nextafter(1e29, math.inf)])
        with pytest.raises(TerrineError) as error:
            _rows(con, _result(_anchors(con), [pair]))
        assert error.value.code == "terrine.temporal.float64_precision"
        assert error.value.context["reason"] == "out_of_range"


@pytest.mark.parametrize("amount", [6e28, 9e28])
def test_wide_selected_float_sum_stays_quantized_on_duckdb(con, amount):
    values = [amount, amount]
    pair = _local(con, values)
    value = _rows(con, _result(_anchors(con), [pair]))[0]["requested"]
    assert abs(Fraction(value) - sum(map(Fraction, values))) <= window_sum_bound(values)


@pytest.mark.parametrize("resolution", ["event", "day"])
@pytest.mark.parametrize("same_bucket", [True, False])
def test_bounded_float_denominator_overflow_refuses_instead_of_publishing(
    con, resolution, same_bucket
):
    from terrine.backends.dialect import dialect_scope

    with dialect_scope("generic"):
        numerator = _local(con, [1.0], name="numerator")
        denominator = _local(
            con,
            [9e28, 9e28],
            name="denominator",
            times=[datetime(2025, 1, day, 12, tzinfo=UTC) for day in (2, 2 if same_bucket else 3)],
        )
        result = _result(
            _anchors(con),
            [numerator, denominator],
            kind="ratio",
            window=Window(0, "2d"),
            resolution=resolution,
        )
        with pytest.raises(TerrineError) as error:
            _rows(con, result)
    assert error.value.code == "terrine.temporal.exact_overflow"
    assert error.value.context["failed_count"] == 1
