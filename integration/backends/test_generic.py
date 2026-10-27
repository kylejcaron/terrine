"""Known answers for the portable base expression policy on real backends.

Every case forces the base dialect through the central selector while the
DuckDB or PostgreSQL connection, provider and SQL dialect stay native. Oracles
are independent Python ``Decimal``/integer arithmetic, defined before any
expression is built.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from decimal import Decimal
from fractions import Fraction

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest

import terrine as tr
from integration.conftest import force_generic_dialect
from integration.warehouse_execution.postgres_fixtures import (
    postgres_scratch as postgres_scratch,  # noqa: F401
)
from terrine.backends.dialect import current_dialect
from terrine.backends.joins import asof_join
from terrine.backends.numeric import (
    decimal_coefficient,
    exact_float,
    exact_sum,
    exact_type,
    truncating_quotient,
)
from terrine.backends.timestamps import timestamp_parts, utc_instant, utc_wall_clock
from terrine.errors import TerrineError
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import (
    BoundFilter,
    BoundMeasure,
    BoundSource,
    PropertyBinding,
    TableRef,
)
from terrine.temporal.aggregate import narrow_exact
from terrine.temporal.daily import sparse_daily
from terrine.temporal.prefix import sparse_prefix

_WIDE = dt.Decimal(38, 0)
_FIXED = dt.Decimal(38, 9)
_INT64_MAX = 2**63 - 1
_INT64_MIN = -(2**63)
_TICK = Decimal("0.000000001")
_BUDGET = tr.Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)


@pytest.fixture(autouse=True)
def generic_policy(monkeypatch):
    force_generic_dialect(monkeypatch)


@pytest.fixture(params=["duckdb", pytest.param("postgres", marks=pytest.mark.postgres)])
def connection(request):
    if request.param == "duckdb":
        con = ibis.duckdb.connect()
        try:
            yield con
        finally:
            con.disconnect()
        return
    yield request.getfixturevalue("postgres_scratch").con


def _rows(connection, table):
    return connection.to_pyarrow(table).to_pylist()


def _quantize(value: float | None) -> Decimal | None:
    """Independent oracle: truncate the shortest decimal text toward zero at nine places."""
    if value is None or not math.isfinite(value):
        return None
    # int() of a Fraction truncates toward zero.
    return Decimal(int(Fraction(repr(value)) * 10**9)).scaleb(-9)


def _truncating_quotient(dividend: int | None, divisor: int | None) -> int | None:
    """Independent oracle: integer division truncating toward zero, NULL for zero or NULL."""
    if dividend is None or divisor is None or divisor == 0:
        return None
    magnitude = abs(dividend) // abs(divisor)
    return magnitude if (dividend < 0) == (divisor < 0) else -magnitude


# --- fixed-scale entry and coefficient domain -------------------------------------------

_FLOATS = {
    "positive": 0.1234567899,
    "negative": -0.1234567899,
    "below_tick": 1e-10,
    "null": None,
    "inside_domain": math.nextafter(1e29, 0),
    "outside_domain": 1e29,
    "negative_outside": -1e29,
    "infinite": math.inf,
    "not_a_number": math.nan,
}
_QUANTIZED = [Decimal("0.123456789"), Decimal("-0.123456789"), Decimal("0"), None]
_AMOUNTS = [Decimal("9007199254740993.01"), Decimal("-9007199254740993.00"), Decimal("0.02")]


def test_fixed_scale_entry_and_coefficient_domain(connection):
    assert exact_type() == dt.Decimal(38, 9)
    assert current_dialect().wide_type() == dt.Decimal(38, 0)
    floats = connection.create_table(
        "floats",
        pa.table(
            {
                "case": list(_FLOATS),
                "value": pa.array(list(_FLOATS.values()), pa.float64()),
            }
        ),
    )
    exact, nonfinite, unrepresentable = exact_float(floats.value)
    assert exact.type() == _FIXED
    _, text = current_dialect(floats.value).finite_text(floats.value)
    rows = {
        row["case"]: row
        for row in _rows(
            connection,
            floats.select(
                "case", exact=exact, nonfinite=nonfinite, unrepresentable=unrepresentable, text=text
            ),
        )
    }
    quantized = [rows[name]["exact"] for name in ("positive", "negative", "below_tick", "null")]
    assert quantized == _QUANTIZED
    for name, value in _FLOATS.items():
        row = rows[name]
        assert row["nonfinite"] is (value is not None and not math.isfinite(value))
        if value is None or not math.isfinite(value):
            assert row["exact"] is None and row["unrepresentable"] is False
            continue
        # Error model: the backend text is within half an ulp of the binary64 value
        # (shortest round-trip text) and quantization perturbs it by less than 10**-9.
        printed = Fraction(row["text"])
        assert abs(printed - Fraction(value)) <= Fraction(math.ulp(value)) / 2
        if row["exact"] is None:
            assert row["unrepresentable"] is True
            assert abs(printed) >= 10**29
            continue
        assert row["unrepresentable"] is False
        assert row["exact"] == _quantize(value)
        assert abs(Fraction(row["exact"]) - printed) < Fraction(1, 10**9)
    assert rows["inside_domain"]["exact"] == Decimal("99999999999999970000000000000")
    assert rows["outside_domain"]["exact"] is None

    # Source decimals keep their scale-2 coefficients in the wide domain, not scale 9.
    amounts = connection.create_table(
        "amounts", pa.table({"amount": pa.array(_AMOUNTS, pa.decimal128(20, 2))})
    )
    coefficient = decimal_coefficient(amounts.amount, 2)
    assert coefficient.type() == _WIDE
    total = exact_sum(coefficient)
    assert total.type() == _WIDE
    (row,) = _rows(
        connection,
        amounts.aggregate(total=total).select(
            difference=lambda t: narrow_exact(t.total, 2, dt.Decimal(38, 2))
        ),
    )
    source_decimal_difference = row["difference"]
    assert source_decimal_difference == Decimal("0.03")
    assert source_decimal_difference == sum(_AMOUNTS)


# --- exact integer quotients ---------------------------------------------------------

_QUOTIENT_CASES = [
    (9007199254740993, 3),
    (-9007199254740993, 3),
    (-7, 3),
    (-7, -3),
    (None, 3),
    (_INT64_MAX, 1),
    (_INT64_MIN, 1),
]
_QUOTIENT_EDGES = [(_INT64_MIN, -1), (5, 0), (0, -3), (_INT64_MAX, _INT64_MAX)]


def test_signed_integer_quotient_is_exact_after_residual_correction(connection):
    cases = _QUOTIENT_CASES + _QUOTIENT_EDGES
    table = connection.create_table(
        "quotients",
        pa.table(
            {
                "case": list(range(len(cases))),
                "a": pa.array([a for a, _ in cases], pa.int64()),
                "b": pa.array([b for _, b in cases], pa.int64()),
            }
        ),
    )
    quotient = truncating_quotient(table.a, table.b, like=table.a)
    by_literal = truncating_quotient(table.a, 3, like=table.a)
    assert quotient.type().is_int64()
    rows = sorted(
        _rows(connection, table.select("case", quotient=quotient, by_three=by_literal)),
        key=lambda row: row["case"],
    )
    quotients = [row["quotient"] for row in rows[: len(_QUOTIENT_CASES)]]
    assert quotients == [
        3002399751580331,
        -3002399751580331,
        -2,
        2,
        None,
        9223372036854775807,
        -9223372036854775808,
    ]
    assert quotients == [_truncating_quotient(a, b) for a, b in _QUOTIENT_CASES]
    # INT64_MIN / -1 leaves int64 and a zero divisor has no quotient: NULL, never a wrapped
    # value or a backend error.
    assert [row["quotient"] for row in rows[len(_QUOTIENT_CASES) :]] == [None, None, 0, 1]
    assert [row["by_three"] for row in rows] == [_truncating_quotient(a, 3) for a, _ in cases]

    # The wide domain certifies quotients below 10**19 from 38-digit multiples,
    # including dividends within a few 10**19 of 10**38, and INT64_MIN / -1.
    wide_cases = [
        (10**38 - 1, 10**19 + 7),
        (-(10**38 - 1), 10**19 + 7),
        (99999999999999999989999999999999999999, 9999999999999999999),
        (_INT64_MIN, -1),
        (10**37, -(10**19 - 1)),
    ]
    wide = connection.create_table(
        "wide_quotients",
        pa.table(
            {
                "case": list(range(len(wide_cases))),
                "a": pa.array([Decimal(a) for a, _ in wide_cases], pa.decimal128(38, 0)),
                "b": pa.array([Decimal(b) for _, b in wide_cases], pa.decimal128(38, 0)),
            }
        ),
    )
    policy = current_dialect(wide.a)
    wide_quotient = policy.wide_quotient(wide.a, wide.b)
    assert wide_quotient.type() == _WIDE
    rows = sorted(
        _rows(connection, wide.select("case", quotient=wide_quotient)), key=lambda r: r["case"]
    )
    assert [int(row["quotient"]) for row in rows] == [
        _truncating_quotient(a, b) for a, b in wide_cases
    ]
    assert rows[0]["quotient"] == Decimal("9999999999999999993")
    assert rows[2]["quotient"] == Decimal("9999999999999999999")
    assert rows[3]["quotient"] == Decimal(2**63)


# --- union/window as-of fallback -----------------------------------------------------


def test_union_window_asof_preserves_request_multiplicity(connection):
    left = connection.create_table(
        "requests",
        pa.table(
            {
                "tag": ["a", "b", "c", "d"],
                "unit": ["u", "u", "v", "u"],
                "request": pa.array([2, 2, 2, None], pa.int64()),
                # Case-colliding candidate alias names must not be reused.
                "__TERRINE_ASOF_UNIT": [1, 1, 1, 1],
            }
        ),
    )
    right = connection.create_table(
        "states",
        pa.table(
            {
                "state_unit": ["u", "u"],
                "change": pa.array([1, 2], pa.int64()),
                "payload": ["old", "new"],
                "__terrine_asof_KEY": [0, 0],
            }
        ),
    )
    assert not current_dialect(left.request).has_native_asof
    payloads = {}
    for inclusive in (False, True):
        joined = asof_join(
            left,
            right,
            left_unit="unit",
            left_key="request",
            right_unit="state_unit",
            right_key="change",
            inclusive=inclusive,
        )
        rows = sorted(_rows(connection, joined), key=lambda row: row["tag"])
        assert [row["tag"] for row in rows] == ["a", "b", "c", "d"]
        assert all(row["__TERRINE_ASOF_UNIT"] == 1 for row in rows)
        payloads[inclusive] = [row["payload"] for row in rows]
    strict_payloads, inclusive_payloads = payloads[False], payloads[True]
    assert strict_payloads == ["old", "old", None, None]
    assert inclusive_payloads == ["new", "new", None, None]


# --- filtered source decimals keep the wide coefficient domain --------------------------

_BIG = Decimal("12345678901234567890123456789.12")


def test_filtered_source_decimal_keeps_wide_coefficient_domain(connection):
    coefficient = int("".join(map(str, _BIG.as_tuple().digits)))
    assert coefficient > 10**29
    source = BoundSource(
        "postings",
        TableRef(kind="table", name="postings"),
        "occurred_at",
        {"user": "user_id"},
        {"revenue": "amount"},
        {"price": PropertyBinding("price", "amount", "float", "event_time", None)},
        (),
        connection.name,
        "UTC",
    )
    raw = connection.create_table(
        "postings",
        pa.table(
            {
                "occurred_at": pa.array(
                    [datetime(2025, 3, 1, 12, tzinfo=UTC)] * 3, pa.timestamp("us", tz="UTC")
                ),
                "user_id": ["u", "u", "u"],
                "amount": pa.array([_BIG, Decimal("1.00"), None], pa.decimal128(38, 2)),
            }
        ),
    )
    events = normalize_events(raw, source, "user_id")
    # Thresholds and typed NULL literals must anchor to the scale-0 coefficient operand.
    measure = BoundMeasure(
        "revenue",
        "postings",
        "sum",
        (BoundFilter("price", "gt", ("1",)), BoundFilter("price", "in", (str(_BIG), None))),
        None,
        None,
    )
    daily = sparse_daily(events, measure, "UTC", "user_id", source=source)
    prefix = sparse_prefix(daily, measure, resolution="day")
    schema = prefix.schema()
    assert schema["sum_value"] == _WIDE
    assert schema["event_count"] == _WIDE
    assert schema["unknown_filter_count"] == _WIDE
    (row,) = _rows(
        connection,
        prefix.select(
            "sum_value",
            "event_count",
            "nonnull_count",
            "unknown_filter_count",
            narrowed=narrow_exact(prefix.sum_value, 2, dt.Decimal(38, 2)),
        ),
    )
    assert row["sum_value"] == Decimal(coefficient)
    assert row["narrowed"] == _BIG
    assert (row["event_count"], row["nonnull_count"]) == (Decimal(1), Decimal(1))
    # 1.00 is in neither value, and the NULL amount's membership is unknown.
    assert row["unknown_filter_count"] == Decimal(1)


# --- window results through the public API -----------------------------------------

_CATALOG = {
    "schema_version": 1,
    "day_boundary": "UTC",
    "sources": {
        "orders": {
            "relation": {"kind": "table", "name": "events"},
            "timestamp": "occurred_at",
            "entities": {"user": "user_id"},
            "facts": ["order", "quantity", "amount", "weight"],
            "properties": [],
            "dimensions": [],
        }
    },
    "facts": {
        "order": {"source": "orders", "column": None},
        "quantity": {"source": "orders", "column": "qty"},
        "amount": {"source": "orders", "column": "amount"},
        "weight": {"source": "orders", "column": "weight"},
    },
    "properties": {},
    "dimensions": {},
    "measures": {
        "order_count": {"fact": "order", "aggregation": "count"},
        "quantity_average": {"fact": "quantity", "aggregation": "avg_event"},
        "amount_total": {"fact": "amount", "aggregation": "sum"},
        "amount_average": {"fact": "amount", "aggregation": "avg_event"},
        "weight_total": {"fact": "weight", "aggregation": "sum"},
        "weight_average": {"fact": "weight", "aggregation": "avg_event"},
    },
    "metrics": {
        "orders": {"kind": "mean", "entity": "user", "measure": "order_count"},
        "spend": {"kind": "mean", "entity": "user", "measure": "amount_total"},
        "average_spend": {"kind": "mean", "entity": "user", "measure": "amount_average"},
        "spend_per_order": {
            "kind": "ratio",
            "entity": "user",
            "numerator": "amount_total",
            "denominator": "order_count",
        },
        "weight": {"kind": "mean", "entity": "user", "measure": "weight_total"},
        "average_weight": {"kind": "mean", "entity": "user", "measure": "weight_average"},
        "weight_per_order": {
            "kind": "ratio",
            "entity": "user",
            "numerator": "weight_total",
            "denominator": "order_count",
        },
        "weight_per_quantity": {
            "kind": "ratio",
            "entity": "user",
            "numerator": "weight_average",
            "denominator": "quantity_average",
        },
    },
}
_T0 = datetime(2025, 1, 2, tzinfo=UTC)
_AMOUNT_TYPE = pa.decimal128(38, 2)


def _model(connection, events):
    schema = pa.schema(
        [
            ("user_id", pa.string()),
            ("occurred_at", pa.timestamp("us", tz="UTC")),
            ("qty", pa.int64()),
            ("amount", _AMOUNT_TYPE),
            ("weight", pa.float64()),
        ]
    )
    connection.create_table("users", pa.table({"user_id": ["a", "b", "c"]}))
    connection.create_table("events", pa.Table.from_pylist(events, schema=schema))
    return tr.Model.from_catalog(
        {**_CATALOG, "dialect": connection.name},
        con=connection,
        units={"user": tr.UnitSource("users", "user_id")},
        readiness={"orders": tr.Coverage("2024-12-01", "2025-02-01")},
    )


def _event(user, when, *, qty=1, amount="0.00", weight=0.0):
    return {
        "user_id": user,
        "occurred_at": when,
        "qty": qty,
        "amount": Decimal(amount),
        "weight": weight,
    }


def _values(model, **features):
    design = tr.Design("user", tr.FixedDate("2025-01-02"), features=features)
    table = design.build(model).materialize(_BUDGET).table
    return {row["unit"]: row for row in table.to_pylist()}


def test_window_quotients_cancellation_and_order_invariance(connection):
    history = datetime(2024, 12, 20, tzinfo=UTC)
    inside = [datetime(2025, 1, 1, 12, minute, tzinfo=UTC) for minute in (0, 1, 2)]
    # Unit b receives unit a's window events in a different order; both cancel
    # 1e20 history exactly and add three quantized ticks.
    window = [
        _event("a", inside[0], qty=3, amount="0.01", weight=1e-9),
        _event("a", inside[1], qty=4, amount="0.02", weight=2e-9),
    ]
    events = [
        _event("a", history, qty=1, amount="6" + "0" * 34 + ".00", weight=1e20),
        _event("a", history, qty=1, amount="-6" + "0" * 34 + ".00", weight=-1e20),
        *window,
        _event("b", history, qty=1, weight=-1e20),
        _event("b", history, qty=1, weight=1e20),
        {**window[1], "user_id": "b"},
        {**window[0], "user_id": "b"},
    ]
    model = _model(connection, events)
    rows = _values(
        model,
        orders=tr.agg("orders", window=("-1d", "0s")),
        spend=tr.agg("spend", window=("-1d", "0s")),
        weight=tr.agg("weight", window=("-1d", "0s")),
        average_weight=tr.agg("average_weight", window=("-1d", "0s")),
        weight_per_order=tr.agg("weight_per_order", window=("-1d", "0s")),
        weight_per_quantity=tr.agg("weight_per_quantity", window=("-1d", "0s")),
    )
    # Independent oracles in exact arithmetic.
    weight_sum = Fraction(3, 10**9)
    ratio_of_averages = (weight_sum / 2) / Fraction(7, 2)
    for unit in "ab":
        row = rows[unit]
        assert row["orders"] == 2
        assert row["spend"] == Decimal("0.03")
        assert row["weight"] == float(weight_sum)
        assert row["average_weight"] == pytest.approx(float(weight_sum / 2), rel=4 * 2**-53)
        assert row["weight_per_order"] == pytest.approx(float(weight_sum / 2), rel=4 * 2**-53)
        # A nested quotient divides two binary64 quotients once more.
        assert row["weight_per_quantity"] == pytest.approx(float(ratio_of_averages), rel=8 * 2**-53)
        assert row["weight_per_quantity__reason"] is None
    assert rows["c"]["orders"] == 0
    assert rows["c"]["spend"] == Decimal("0.00")
    assert rows["c"]["weight"] == 0.0
    assert rows["c"]["average_weight"] is None
    assert rows["c"]["average_weight__reason"] == "no_observations"
    assert rows["c"]["weight_per_order"] is None
    assert rows["c"]["weight_per_order__reason"] == "zero_denominator"


@pytest.mark.parametrize(
    ("events", "metric", "code", "failure"),
    [
        (
            [
                _event("a", datetime(2024, 12, 20, tzinfo=UTC), weight=math.nan),
                _event("a", datetime(2025, 1, 1, 12, tzinfo=UTC), weight=1.0),
            ],
            "weight",
            "terrine.temporal.float64_precision",
            "nonfinite_input",
        ),
        (
            [_event("a", datetime(2025, 1, 1, 12, tzinfo=UTC), weight=1e29)],
            "weight",
            "terrine.temporal.float64_precision",
            "bound_overflow",
        ),
        # Opposite-sign prefixes whose difference leaves the fixed-scale domain.
        (
            [
                _event("a", datetime(2024, 12, 20, tzinfo=UTC), weight=-6e28),
                _event("a", datetime(2025, 1, 1, 12, tzinfo=UTC), weight=6e28),
                _event("a", datetime(2024, 12, 31, 12, tzinfo=UTC), weight=6e28),
            ],
            "weight",
            "terrine.temporal.float64_precision",
            "bound_overflow",
        ),
        (
            [
                _event(
                    "a",
                    datetime(2024, 12, 20, tzinfo=UTC),
                    amount="-600000000000000000000000000000000000.00",
                ),
                _event(
                    "a",
                    datetime(2025, 1, 1, 12, tzinfo=UTC),
                    amount="600000000000000000000000000000000000.00",
                ),
                _event(
                    "a",
                    datetime(2024, 12, 31, 12, tzinfo=UTC),
                    amount="600000000000000000000000000000000000.00",
                ),
            ],
            "spend",
            "terrine.temporal.exact_overflow",
            None,
        ),
    ],
)
def test_domain_failures_are_coded_refusals(connection, events, metric, code, failure):
    model = _model(connection, events)
    # A two-day window spans both day groups, so each prefix stays inside the domain
    # and only the endpoint difference leaves it.
    with pytest.raises(TerrineError) as error:
        _values(model, value=tr.agg(metric, window=("-2d", "0s")))
    assert error.value.code == code
    if failure is not None:
        assert error.value.context["failure"] == failure


_HUGE_AMOUNT = "6" + "0" * 35 + ".00"
_SAME_DAY_HUGE_AMOUNTS = [
    _event("a", datetime(2025, 1, 1, 12, tzinfo=UTC), amount=_HUGE_AMOUNT),
    _event("a", datetime(2025, 1, 1, 13, tzinfo=UTC), amount=_HUGE_AMOUNT),
]


@pytest.mark.parametrize(
    ("events", "metric", "code", "failure"),
    [
        # Two valid facts in one day group need 1.2e29: the group accumulation overflows.
        (
            [
                _event("a", datetime(2025, 1, 1, 12, tzinfo=UTC), weight=6e28),
                _event("a", datetime(2025, 1, 1, 13, tzinfo=UTC), weight=6e28),
            ],
            "weight",
            "terrine.temporal.float64_precision",
            "bound_overflow",
        ),
        # Successive prefixes overflow although each day group fits.
        (
            [
                _event("a", datetime(2024, 12, 31, 12, tzinfo=UTC), weight=6e28),
                _event("a", datetime(2025, 1, 1, 12, tzinfo=UTC), weight=6e28),
            ],
            "weight",
            "terrine.temporal.float64_precision",
            "bound_overflow",
        ),
        # Three facts scale to 1.8e38, beyond a 128-bit accumulator: the group and
        # prefix sums must never receive them.
        (
            [
                _event("a", datetime(2025, 1, 1, hour, tzinfo=UTC), weight=6e28)
                for hour in (12, 13, 14)
            ],
            "weight",
            "terrine.temporal.float64_precision",
            "bound_overflow",
        ),
        (
            [_event("a", datetime(2024, 12, day, 12, tzinfo=UTC), weight=6e28) for day in (30, 31)]
            + [_event("a", datetime(2025, 1, 1, 12, tzinfo=UTC), weight=6e28)],
            "weight",
            "terrine.temporal.float64_precision",
            "bound_overflow",
        ),
        (_SAME_DAY_HUGE_AMOUNTS, "spend", "terrine.temporal.exact_overflow", None),
        (
            [
                _event("a", datetime(2024, 12, 31, 12, tzinfo=UTC), amount=_HUGE_AMOUNT),
                _event("a", datetime(2025, 1, 1, 12, tzinfo=UTC), amount=_HUGE_AMOUNT),
            ],
            "spend",
            "terrine.temporal.exact_overflow",
            None,
        ),
        # A withheld coefficient state must refuse through every consumer, not only
        # the exact output: the average's quotient and a ratio's numerator.
        (_SAME_DAY_HUGE_AMOUNTS, "average_spend", "terrine.temporal.exact_overflow", None),
        (_SAME_DAY_HUGE_AMOUNTS, "spend_per_order", "terrine.temporal.exact_overflow", None),
    ],
)
def test_accumulated_state_overflow_is_a_coded_refusal(connection, events, metric, code, failure):
    model = _model(connection, events)
    with pytest.raises(TerrineError) as error:
        _values(model, value=tr.agg(metric, window=("-2d", "0s")))
    assert error.value.code == code
    if failure is not None:
        assert error.value.context["failure"] == failure


def test_aware_deltas_across_daylight_saving_are_correct_or_refused(connection):
    """A New York session: DuckDB answers exactly; PostgreSQL's Ibis delta lowering is
    session-dependent, so generic admission refuses the operation before source access."""
    from terrine.execution._generic import NoSnapshotSession

    statement = "SET TimeZone = 'America/New_York'"
    if connection.name == "duckdb":
        connection.raw_sql(statement)
    else:
        connection.raw_sql(statement).close()
    summer = datetime(2025, 7, 1, 12, 0, 0, 123456, tzinfo=UTC)
    table = connection.create_table(
        "dst", pa.table({"aware": pa.array([summer], pa.timestamp("us", tz="UTC"))})
    )
    query = table.select(wall=utc_wall_clock(table.aware))
    session = NoSnapshotSession(connection)
    try:
        session.admit(query)
    except TerrineError as refusal:
        assert connection.name == "postgres"
        assert refusal.code == "terrine.backend.capability_unavailable"
        assert refusal.context["operation"] == "utc_wall_clock"
        assert refusal.context["backend"] == "postgres"
    else:
        assert connection.name == "duckdb"
        (row,) = _rows(connection, query)
        assert row["wall"] == summer.replace(tzinfo=None)
    finally:
        session.close()


def test_zero_and_null_are_distinct_from_nonfinite_flags(connection):
    events = [
        _event("a", datetime(2025, 1, 1, 12, tzinfo=UTC), weight=0.0),
        _event("a", datetime(2025, 1, 1, 13, tzinfo=UTC), weight=-0.0),
        {**_event("b", datetime(2025, 1, 1, 12, tzinfo=UTC)), "weight": None},
    ]
    model = _model(connection, events)
    rows = _values(
        model,
        weight=tr.agg("weight", window=("-1d", "0s")),
        average_weight=tr.agg("average_weight", window=("-1d", "0s")),
    )
    assert rows["a"]["weight"] == 0.0
    assert rows["a"]["average_weight"] == 0.0
    assert rows["b"]["weight"] is None
    assert rows["b"]["weight__reason"] == "all_values_null"
    assert rows["b"]["average_weight"] is None
    assert rows["b"]["average_weight__reason"] == "all_values_null"


# --- UTC conversions and timestamp parts --------------------------------------------

_INSTANTS = {
    "standard_time": datetime(2025, 1, 15, 12, 0, 0, 654321, tzinfo=UTC),
    "pre_epoch": datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
    "far_future": datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
}


def test_naive_utc_round_trip_and_signed_parts_ignore_session_zone(connection):
    statement = "SET TimeZone = 'America/New_York'"
    if connection.name == "duckdb":
        connection.raw_sql(statement)
    else:
        connection.raw_sql(statement).close()
    table = connection.create_table(
        "instants",
        pa.table(
            {
                "case": list(_INSTANTS),
                "naive": pa.array(
                    [value.replace(tzinfo=None) for value in _INSTANTS.values()],
                    pa.timestamp("us"),
                ),
            }
        ),
    )
    instant = utc_instant(table.naive)
    rank, seconds, nanos = timestamp_parts(table.naive)
    rows = {
        row["case"]: row
        for row in _rows(
            connection,
            table.select(
                "case",
                instant=instant,
                wall=utc_wall_clock(instant),
                rank=rank,
                seconds=seconds,
                nanos=nanos,
            ),
        )
    }
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    for name, value in _INSTANTS.items():
        row = rows[name]
        assert row["instant"] == value
        assert row["wall"] == value.replace(tzinfo=None)
        delta = value - epoch
        micros = (delta.days * 86400 + delta.seconds) * 10**6 + delta.microseconds
        expected_seconds = micros // 10**6 if micros >= 0 else -((-micros) // 10**6)
        assert (row["rank"], row["seconds"]) == (0, expected_seconds)
        assert row["nanos"] == (micros - expected_seconds * 10**6) * 1000
    assert rows["pre_epoch"]["seconds"] == 0
    assert rows["pre_epoch"]["nanos"] == -1000


# Offsets from the epoch in nanoseconds, one tick either side of a whole second
# and of the epoch itself; the microsecond variant scales each tick by 1000.
_SUBSECOND_OFFSETS = {
    "after_second": 10**9 + 1,
    "before_second": -(10**9) - 1,
    "after_epoch": 1,
    "before_epoch": -1,
}


def test_subsecond_parts_split_whole_seconds_and_signed_remainders(connection):
    """Whole seconds truncate toward zero and the remainder keeps the sign, at the
    microsecond precision every backend holds and at nanoseconds where the backend
    has them (PostgreSQL timestamps stop at microseconds)."""
    units = ["us", "ns"] if connection.name == "duckdb" else ["us"]
    for unit in units:
        tick = 1 if unit == "ns" else 1000
        offsets = {name: offset * tick for name, offset in _SUBSECOND_OFFSETS.items()}
        table = connection.create_table(
            f"ticks_{unit}",
            pa.table(
                {
                    "case": list(offsets),
                    "naive": pa.array(
                        [offset // tick for offset in offsets.values()], pa.int64()
                    ).cast(pa.timestamp(unit)),
                }
            ),
        )
        rank, seconds, nanos = timestamp_parts(table.naive)
        rows = {
            row["case"]: row
            for row in _rows(
                connection, table.select("case", rank=rank, seconds=seconds, nanos=nanos)
            )
        }
        for name, offset in offsets.items():
            expected_seconds = _truncating_quotient(offset, 10**9)
            assert expected_seconds is not None
            expected = (0, expected_seconds, offset - expected_seconds * 10**9)
            assert (rows[name]["rank"], rows[name]["seconds"], rows[name]["nanos"]) == expected, (
                unit,
                name,
            )
