from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from fractions import Fraction

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest

from terrine.data import BoundCoverage
from terrine.errors import TerrineError
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import BoundMeasure, BoundSource, TableRef
from terrine.temporal.daily import resolve_measure
from terrine.temporal.readiness import Readiness
from terrine.windows import Window
from tests._float_bound import window_sum_bound
from tests.temporal.test_window_aggregates import (
    _anchors,
    _assert_quotient,
    _batch,
    _exact_sum,
    _finalized_counts,
    _local,
    _result,
    _rows,
)


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


def _timestamp_source(con, instants, amounts, *, boundary="UTC", aware_microseconds=False):
    raw = con.create_table(
        "timestamp_edge_ticks" if aware_microseconds else "timestamp_edges",
        pa.table({"user_id": ["a"] * len(amounts), "occurred_at": instants, "amount": amounts}),
    )
    if aware_microseconds:

        @ibis.udf.scalar.builtin(
            name="make_timestamptz", signature=((dt.int64,), dt.Timestamp(timezone="UTC", scale=6))
        )
        def native_aware(value):
            """Construct a native UTC timestamp without an Arrow zone conversion."""

        raw = con.create_table(
            "timestamp_edges", raw.mutate(occurred_at=native_aware(raw.occurred_at))
        )
    source = BoundSource(
        "timestamp_edges",
        TableRef(kind="table", name="timestamp_edges"),
        "occurred_at",
        {"user": "user_id"},
        {"revenue": "amount"},
        {},
        (),
        "duckdb",
        boundary,
    )
    events = normalize_events(raw, source, "user_id")
    measure = resolve_measure(
        events, BoundMeasure("revenue", source.name, "sum", (), None, None), source=source
    )
    return measure, (events, source)


@pytest.mark.parametrize("boundary,minutes", [("UTC", 0), ("UTC+05:30", 330), ("UTC-05:00", -300)])
def test_local_day_batch_preserves_nanosecond_midnight_edges(con, boundary, minutes):
    # A negative fractional microsecond must stay in its original local day.
    t0 = datetime(1970, 1, 1, tzinfo=UTC) - timedelta(minutes=minutes)
    midnight_ns = -minutes * 60 * 10**9
    instants = pa.array([midnight_ns - 1, midnight_ns, midnight_ns + 1], pa.int64()).cast(
        pa.timestamp("ns")
    )
    pair = _timestamp_source(con, instants, [1, 2, 4], boundary=boundary)
    count = (replace(pair[0], aggregation="count"), pair[1])
    readiness = Readiness(
        {"timestamp_edges": BoundCoverage(t0 - timedelta(days=2), t0 + timedelta(days=2))}
    )
    for resolution in ("event", "day"):
        result = _batch(
            _anchors(con, time=t0.isoformat()),
            [
                ("before_sum", [pair], Window("-1d", 0), "mean"),
                ("before_count", [count], Window("-1d", 0), "mean"),
                ("after_sum", [pair], Window(0, "1d"), "mean"),
                ("after_count", [count], Window(0, "1d"), "mean"),
            ],
            readiness=readiness,
            boundary=boundary,
            resolution=resolution,
        )
        (row,) = _rows(con, result)
        assert tuple(row[name] for name in result.columns) == (Decimal(1), 1, Decimal(6), 2), (
            resolution
        )
        assert (row["unit"], row["anchor_id"], row["t0"]) == ("a", "0", t0)
        for meta in result.columns.values():
            assert row[meta.reason_column] is None
            assert row[meta.complete_column] is True


@pytest.mark.parametrize("aware", [False, True])
def test_local_day_batch_ignores_unused_event_on_final_supported_day(con, aware):
    # DuckDB's finite TIMESTAMP endpoint exceeds Python's datetime domain.
    boundary = "UTC+05:30"
    t0 = datetime(2025, 1, 1, 18, 30, tzinfo=UTC)
    requested = t0 + timedelta(hours=12)
    requested_us = (requested - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)
    instants = pa.array([requested_us, 2**63 - 2], pa.int64())
    if not aware:
        instants = instants.cast(pa.timestamp("us"))
    pair = _timestamp_source(con, instants, [3, 100], boundary=boundary, aware_microseconds=aware)
    count = (replace(pair[0], aggregation="count"), pair[1])
    for resolution in ("event", "day"):
        result = _batch(
            _anchors(con, time=t0.isoformat()),
            [
                ("sum", [pair], Window(0, "1d"), "mean"),
                ("count", [count], Window(0, "1d"), "mean"),
            ],
            resolution=resolution,
            boundary=boundary,
        )
        (row,) = _rows(con, result)
        assert (row["sum"], row["count"]) == (Decimal(3), 1)
        for meta in result.columns.values():
            assert row[meta.reason_column] is None
            assert row[meta.complete_column] is True
    unaligned = _batch(
        _anchors(con, time=(t0 + timedelta(microseconds=1)).isoformat()),
        [("sum", [pair], Window(0, "1d"), "mean")],
        boundary=boundary,
        resolution="day",
    )
    with pytest.raises(TerrineError) as error:
        _rows(con, unaligned)
    assert error.value.code == "terrine.temporal.unaligned_day_resolution"


@pytest.mark.parametrize("resolution", ["event", "day"])
@pytest.mark.parametrize("dtype", ["int64", "float64"])
def test_wide_local_batch_materializes_values_reasons_and_full_anchor_support(
    con, resolution, dtype
):
    # Catches independent outputs accumulating a serial projection chain that
    # cannot compile, and relinking that loses a window mask or anchor identity.
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = datetime(2025, 1, 15, tzinfo=UTC)
    t0 = datetime(2025, 1, 12, tzinfo=UTC)
    days, values = [2, 4, 7, 10, 4, 7], [2, None, 6, -2, None, 0]
    units = ["a", "a", "a", "a", "null", "zero"]
    measure, source = _local(
        con,
        values,
        dtype=dtype,
        exact=dtype == "int64",
        units=units,
        times=[start + timedelta(days=day - 1, hours=12) for day in days],
    )
    pairs = {
        "count": [(replace(measure, aggregation="count"), source)],
        "sum": [(measure, source)],
        "average": [(replace(measure, aggregation="avg_event"), source)],
        "ratio": [(measure, source), (replace(measure, aggregation="count"), source)],
        "conversion": [(measure, source)],
    }
    anchors = _anchors(con, time=t0.isoformat(), units=("a", "a", "null", "zero", "empty"))
    anchors = anchors.mutate(
        t0=(anchors.anchor_id == "1").ifelse(t0 - timedelta(days=1), anchors.t0),
        _observed_from=(anchors.anchor_id == "1").ifelse(start + timedelta(days=4), start),
        _observed_until=ibis.literal(end),
    )
    result = _batch(
        anchors,
        [
            (
                f"{kind}_{width}",
                pair,
                Window(f"-{width}d", 0),
                kind if kind in {"ratio", "conversion"} else "mean",
            )
            for kind, pair in pairs.items()
            for width in range(1, 11)
        ],
        readiness=Readiness({"postings": BoundCoverage(start, end)}),
        resolution=resolution,
    )
    rows = sorted(_rows(con, result), key=lambda row: row["anchor_id"])
    assert [(row["unit"], row["anchor_id"], row["t0"]) for row in rows] == [
        ("a", "0", t0),
        ("a", "1", t0 - timedelta(days=1)),
        ("null", "2", t0),
        ("zero", "3", t0),
        ("empty", "4", t0),
    ]
    for row in rows:
        assert row["_observed_from"] == start + timedelta(days=4 if row["anchor_id"] == "1" else 0)
        assert row["_observed_until"] == end
        for width in range(1, 11):
            lower = row["t0"] - timedelta(days=width)
            selected = [
                value
                for unit, day, value in zip(units, days, values, strict=True)
                if unit == row["unit"]
                and lower <= start + timedelta(days=day - 1, hours=12) < row["t0"]
            ]
            nonnull = [value for value in selected if value is not None]
            total = sum(nonnull)
            expected = {
                "count": len(selected),
                "sum": total if nonnull or not selected else None,
                "average": Fraction(total, len(nonnull)) if nonnull else None,
                "ratio": Fraction(total, len(selected)) if nonnull else None,
                "conversion": bool(selected),
            }
            for kind, value in expected.items():
                output = f"{kind}_{width}"
                meta = result.columns[output]
                complete = lower >= row["_observed_from"]
                reason = None
                if not complete:
                    value, reason = None, "incomplete_observation"
                elif kind in {"sum", "average", "ratio"} and selected and not nonnull:
                    reason = "all_values_null"
                elif kind == "average" and not selected:
                    reason = "no_observations"
                elif kind == "ratio" and not selected:
                    reason = "zero_denominator"
                if value is None:
                    assert row[output] is None
                elif kind == "conversion":
                    assert row[output] is value
                else:
                    assert row[output] == pytest.approx(float(value), rel=2**-50)
                assert row[meta.reason_column] == reason
                assert row[meta.complete_column] is complete
                assert row[meta.observed_from_column] == row["_observed_from"]
                assert row[meta.observed_until_column] == end


@pytest.mark.parametrize("resolution", ["event", "day"])
def test_local_batch_reduces_sources_before_joining_and_keeps_output_availability(con, resolution):
    # Catches source fan-out and batch-wide availability replacing per-output bounds.
    numerator = _local(
        con,
        [2, None, 3, 7, None, 2],
        dtype="int64",
        name="numerator",
        units=["a", "a", "a", "a", "null", "zero"],
        times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (2, 2, 2, 3, 2, 2)],
    )
    denominator = _local(
        con,
        [1, 1, 1, 0],
        dtype="int64",
        name="denominator",
        units=["a", "a", "null", "zero"],
    )
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = datetime(2025, 1, 4, tzinfo=UTC)
    readiness = Readiness(
        {
            "numerator": BoundCoverage(start, end),
            "denominator": BoundCoverage(start, end - timedelta(days=1)),
        }
    )
    anchors = _anchors(
        con,
        units=("a", "a", "null", "zero"),
        extra=", TIMESTAMPTZ '2025-01-03 12:00:00+00' AS _observed_until",
    )
    result = _batch(
        anchors,
        [
            ("ratio", [numerator, denominator], Window(0, "1d"), "ratio"),
            ("wide_ratio", [numerator, denominator], Window(0, "2d"), "ratio"),
            ("total", [numerator], Window(0, "1d"), "mean"),
            ("wide_total", [numerator], Window(0, "2d"), "mean"),
        ],
        readiness=readiness,
        resolution=resolution,
    )
    rows = sorted(_rows(con, result), key=lambda row: row["anchor_id"])
    assert [(r["total"], r["ratio"]) for r in rows] == [
        (Decimal(5), 2.5),
        (Decimal(5), 2.5),
        (None, None),
        (Decimal(2), None),
    ]
    assert [r[result.columns["ratio"].reason_column] for r in rows] == [
        None,
        None,
        "all_values_null",
        "zero_denominator",
    ]
    assert all(r[result.columns["wide_ratio"].reason_column] == "incomplete_coverage" for r in rows)
    assert all(
        r[result.columns["wide_total"].reason_column] == "incomplete_observation" for r in rows
    )
    row = rows[0]
    assert row[result.columns["total"].observed_until_column] == datetime(
        2025, 1, 3, 12, tzinfo=UTC
    )
    assert row[result.columns["ratio"].observed_until_column] == datetime(2025, 1, 3, tzinfo=UTC)


# These exercise native quotient/calendar compilation through DuckDB execution.
def test_ratio_coverage_and_observation_intersect_before_values(con):
    numerator = _local(con, [2], dtype="int64", name="numerator")
    denominator = _local(con, [0], dtype="int64", name="denominator")
    start, end = datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 3, tzinfo=UTC)
    readiness = Readiness(
        {
            "numerator": BoundCoverage(start, end),
            "denominator": BoundCoverage(start, end - timedelta(hours=12)),
        }
    )
    anchors = _anchors(con, extra=", TIMESTAMPTZ '2025-01-02 06:00:00+00' AS _observed_from")
    result = _result(anchors, [numerator, denominator], kind="ratio", readiness=readiness)
    row = _rows(con, result)[0]
    meta = result.columns["requested"]
    assert row["requested"] is None
    assert row[meta.reason_column] == "incomplete_coverage"
    assert row[meta.observed_from_column] == datetime(2025, 1, 2, 6, tzinfo=UTC)
    assert row[meta.observed_until_column] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert row[meta.complete_column] is False
    result = _result(anchors, [numerator, denominator], kind="ratio")
    assert (
        _rows(con, result)[0][result.columns["requested"].reason_column] == "incomplete_observation"
    )


def test_calendar_nonzero_offset_day_and_event_agree(con):
    time = "2025-01-01 18:30:00+00"
    for resolution in ("day", "event"):
        pair = _local(
            con,
            [2.0, 4.0],
            aggregation="avg_calendar_day",
            boundary="+05:30",
            times=[datetime(2025, 1, 1, 19, tzinfo=UTC), datetime(2025, 1, 2, 18, tzinfo=UTC)],
        )
        result = _result(_anchors(con, time=time), [pair], boundary="+05:30", resolution=resolution)
        assert _rows(con, result)[0]["requested"] == 6.0


def test_decimal_average_and_ratio_meet_the_quotient_bound(con):
    numerator = _local(
        con,
        [Decimal("0.10"), Decimal("0.20")],
        aggregation="avg_event",
        dtype="decimal(38,2)",
        name="numerator",
    )
    result = _result(_anchors(con), [numerator])
    _assert_quotient(_rows(con, result)[0]["requested"], Fraction(3, 10), Fraction(2))
    denominator = _local(con, [Decimal("0.70")], dtype="decimal(38,2)", name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    _assert_quotient(_rows(con, result)[0]["requested"], Fraction(3, 20), Fraction(7, 10))


def test_float_average_and_its_ratio_divide_the_exact_operands(con):
    numerator = _local(con, [0.1, 0.2, -0.05], aggregation="avg_event", name="numerator")
    average = _rows(con, _result(_anchors(con), [numerator]))[0]["requested"]
    _assert_quotient(
        average, _exact_sum([0.1, 0.2, -0.05]), Fraction(3), window_sum_bound([0.1, 0.2, -0.05])
    )
    denominator = _local(con, [3.0, 4.0], name="denominator")
    ratio = _result(_anchors(con), [numerator, denominator], kind="ratio")
    _assert_quotient(
        _rows(con, ratio)[0]["requested"],
        _exact_sum([0.1, 0.2, -0.05]) / 3,
        _exact_sum([3.0, 4.0]),
        window_sum_bound([0.1, 0.2, -0.05]) / 3,
        window_sum_bound([3.0, 4.0]),
    )


def test_zero_numerator_ratio_is_exactly_zero(con):
    numerator = _local(con, [1.0, -1.0] * 100, name="numerator")
    denominator = _local(con, [200], dtype="int64", name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    assert _rows(con, result)[0]["requested"] == 0.0


def test_exact_zero_denominators_are_a_reason_not_a_refusal(con):
    numerator = _local(con, [2.0], name="numerator")
    for name, values, dtype in (("zero", [0], "int64"), ("cancelled", [1.0, -1.0], "float64")):
        denominator = _local(con, values, dtype=dtype, name=name)
        result = _result(_anchors(con), [numerator, denominator], kind="ratio")
        row = _rows(con, result)[0]
        assert row["requested"] is None
        assert row[result.columns["requested"].reason_column] == "zero_denominator"


def test_negative_denominator_meets_bound_and_subnormal_vanishes(con):
    numerator = _local(con, [-0.1, 0.025], name="numerator")
    denominator = _local(con, [-0.7], name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    _assert_quotient(
        _rows(con, result)[0]["requested"],
        _exact_sum([-0.1, 0.025]),
        _exact_sum([-0.7]),
        window_sum_bound([-0.1, 0.025]),
        window_sum_bound([-0.7]),
    )
    tiny = _local(con, [float.fromhex("0x0.0000000000001p-1022")], name="tiny")
    result = _result(_anchors(con), [tiny])
    assert _rows(con, result)[0]["requested"] == 0.0


def test_fractional_results_do_not_narrow_internal_exact_states(con):
    maximum = Decimal("99999999999999999999999999999999999999")
    numerator = _local(con, [maximum, maximum], dtype="decimal(38,0)", name="numerator")
    denominator = _local(con, [maximum, maximum], dtype="decimal(38,0)", name="denominator")
    ratio = _result(_anchors(con), [numerator, denominator], kind="ratio")
    assert _rows(con, ratio)[0]["requested"] == 1.0
    average_pair = _local(
        con, [maximum, maximum], aggregation="avg_event", dtype="decimal(38,0)", name="average"
    )
    average = _result(_anchors(con), [average_pair])
    _assert_quotient(_rows(con, average)[0]["requested"], Fraction(2 * (10**38 - 1)), Fraction(2))
    assert _rows(con, _finalized_counts(con, 2**63, kind="ratio"))[0]["requested"] == 1.0


@pytest.mark.parametrize(
    "numerator,denominator",
    [
        ([1.25, -3.5, 0.75], None),
        ([1.0, -1.0] * 100, None),
        ([1.25, -3.5], [3]),
        ([1.25, -3.5], [3e-7, 2.5]),
        ([-1.25, -3.5], [-0.7]),
        ([1.0], [9e28]),
        ([Decimal("0.10"), Decimal("0.20")], None),
        ([Decimal("0.10"), Decimal("0.20")], [7]),
    ],
)
def test_mixed_scale_ratio_and_average_meet_bound(con, numerator, denominator):
    dtype = "decimal(38,2)" if isinstance(numerator[0], Decimal) else "float64"
    if denominator is None:
        pair = _local(con, numerator, aggregation="avg_event", dtype=dtype, name="numerator")
        result = _result(_anchors(con), [pair])
        p, q = _exact_sum(numerator), Fraction(len(numerator))
    else:
        pairs = [
            _local(con, numerator, dtype=dtype, name="numerator"),
            _local(
                con,
                denominator,
                dtype="int64" if isinstance(denominator[0], int) else "float64",
                name="denominator",
            ),
        ]
        result = _result(_anchors(con), pairs, kind="ratio")
        p, q = _exact_sum(numerator), _exact_sum(denominator)
    row = _rows(con, result)[0]
    e_n = window_sum_bound(numerator) if isinstance(numerator[0], float) else 0.0
    e_d = (
        window_sum_bound(denominator) if denominator and isinstance(denominator[0], float) else 0.0
    )
    _assert_quotient(row["requested"], p, q, e_n, e_d)
    meta = result.columns["requested"]
    assert meta.numeric_policy is None


def test_ratio_of_averages_divides_the_exact_averages(con):
    first, second = [0.1, 0.2, -0.05], [3.0, 4.0, 0.3]
    pairs = [
        _local(con, first, aggregation="avg_event", name="first"),
        _local(con, second, aggregation="avg_event", name="second"),
    ]
    ratio = _rows(con, _result(_anchors(con), pairs, kind="ratio"))[0]["requested"]
    _assert_quotient(
        ratio,
        _exact_sum(first) / 3,
        _exact_sum(second) / 3,
        window_sum_bound(first) / 3,
        window_sum_bound(second) / 3,
    )


def test_ratio_of_subnano_averages_has_zero_denominator(con):
    average = _local(con, [5e-324, 0.0, 0.0], aggregation="avg_event", name="average")
    total = _local(con, [5e-324], name="total")
    assert _rows(con, _result(_anchors(con), [average]))[0]["requested"] == 0.0
    for pairs in ([average, total], [total, average]):
        result = _result(_anchors(con), pairs, kind="ratio")
        row = _rows(con, result)[0]
        assert row["requested"] is None
        assert row[result.columns["requested"].reason_column] == "zero_denominator"
    cancelled = _local(con, [1.0, -1.0], aggregation="avg_event", name="cancelled")
    result = _result(_anchors(con), [total, cancelled], kind="ratio")
    row = _rows(con, result)[0]
    assert row["requested"] is None
    assert row[result.columns["requested"].reason_column] == "zero_denominator"


def test_out_of_range_ratio_input_refuses(con):
    numerator = _local(con, [1e308], name="numerator")
    denominator = _local(con, [1.0], name="denominator")
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(_anchors(con), [numerator, denominator], kind="ratio"))
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["reason"] == "out_of_range"


def test_exact_instants_half_open_boundaries_and_named_results(con):
    t0 = datetime(2025, 1, 2, 12, tzinfo=UTC)
    pair = _local(
        con,
        [1, 2, 4, 8, 16],
        dtype="int64",
        times=[
            t0 - timedelta(microseconds=1),
            t0,
            t0 + timedelta(microseconds=1),
            t0 + timedelta(hours=1),
            t0 + timedelta(hours=1),
        ],
    )
    anchors = _anchors(con, time=t0.isoformat(), units=("a", "a"))
    pre = _result(anchors, [pair], window=Window("-1h", 0), output="pre")
    post = _result(pre.table, [pair], window=Window(0, "1h"), output="post")
    rows = _rows(con, post)
    assert len(rows) == 2
    assert [(r["pre"], r["post"], r["t0"]) for r in rows] == [(Decimal(1), Decimal(6), t0)] * 2
    assert post.columns["post"].window == Window(0, "1h")
    assert post.columns["post"].role == "outcome"


def test_all_null_ratio_numerator_is_not_a_zero_denominator(con):
    numerator = _local(con, [None], dtype="int64", name="numerator")
    denominator = _local(con, [0], dtype="int64", name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    row = _rows(con, result)[0]
    assert row["requested"] is None
    assert row[result.columns["requested"].reason_column] == "all_values_null"


def test_local_window_preserves_ns_events_around_utc_microsecond_anchors(con):
    anchor_us = [-1, -1, 1_735_819_200_000_000]
    edges = (-1_000_000_001, -1_000_000_000, -999_999_999, -1, 0, 1, 999_999_999, 10**9, 10**9 + 1)
    instants = [origin * 1000 + edge for origin in dict.fromkeys(anchor_us) for edge in edges]
    amounts = list(range(1, len(instants) + 1))
    pair = _timestamp_source(con, pa.array(instants, type=pa.timestamp("ns")), amounts)
    anchors = con.create_table(
        "microsecond_anchors",
        pa.table(
            {
                "unit": ["a"] * len(anchor_us),
                "anchor_id": [str(index) for index in range(len(anchor_us))],
                "t0": pa.array(anchor_us, type=pa.timestamp("us", tz="UTC")),
            }
        ),
    )
    windows = {"before": (-1, 0), "after": (0, 1), "straddling": (-1, 1)}
    result = _batch(
        anchors,
        [
            (name, [pair], Window(f"{start}s", f"{end}s"), "mean")
            for name, (start, end) in windows.items()
        ],
        readiness=Readiness(
            {
                "timestamp_edges": BoundCoverage(
                    datetime(1960, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)
                )
            }
        ),
    )
    rows = _rows(con, result)
    assert len(rows) == len(anchor_us)
    for row in rows:
        origin_ns = anchor_us[int(row["anchor_id"])] * 1000
        for name, (start, end) in windows.items():
            expected = sum(
                amount
                for instant, amount in zip(instants, amounts, strict=True)
                if origin_ns + start * 10**9 <= instant < origin_ns + end * 10**9
            )
            assert row[name] == expected, (row["anchor_id"], name)
            meta = result.columns[name]
            assert row[meta.reason_column] is None
            assert row[meta.complete_column] is True
