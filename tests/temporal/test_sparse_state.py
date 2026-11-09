import math
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from itertools import permutations

import ibis
import pyarrow as pa
import pytest

from terrine.backends.numeric import is_exact_type
from terrine.backends.timestamps import local_day
from terrine.errors import TerrineError
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import (
    BoundDimension,
    BoundFilter,
    BoundMeasure,
    BoundSource,
    PropertyBinding,
    SqlQuery,
    TableRef,
)
from terrine.semantic.time import boundary_timezone
from terrine.temporal.aggregate import narrow_exact
from terrine.temporal.attributes import enrich_events
from terrine.temporal.daily import (
    _UNKNOWN_FIELDS,
    contributions,
    native_arithmetic,
    reduce_payload,
    resolve_measure,
)


@pytest.fixture
def connection():
    con = ibis.duckdb.connect()
    try:
        yield con
    finally:
        con.disconnect()


def _source(*, properties=None, dimensions=(), boundary="UTC", fact="amount"):
    return BoundSource(
        "postings",
        TableRef(kind="table", name="postings"),
        "occurred_at",
        {"user": "user_id", "account": "account_id"},
        {"revenue": fact},
        properties or {},
        dimensions,
        "duckdb",
        boundary,
    )


def _measure(*, filters=(), aggregation="sum", exact=True):
    return BoundMeasure("revenue", "postings", aggregation, filters, None, None, exact=exact)


def _events(connection, amounts, *, dtype="float64", times=None, units=None, **columns):
    n = len(amounts)
    table = pa.Table.from_pydict(
        {
            "occurred_at": times or [datetime(2025, 3, 1, 12, tzinfo=UTC)] * n,
            "user_id": units or ["u"] * n,
            "account_id": [str(i) for i in range(n)],
            "amount": amounts,
            **columns,
        },
        schema=ibis.schema(
            {
                "occurred_at": "timestamp('UTC')",
                "user_id": "string",
                "account_id": "string",
                "amount": dtype,
                **{key: "string" for key in columns},
            }
        ).to_pyarrow(),
    )
    return connection.create_table("postings", table, overwrite=True)


def _rows(connection, table):
    # DuckDB BIGNUM is an internal coefficient, not a valid Arrow numeric column.
    projected = table.select(
        *(
            table[name].cast("string").name(name) if dtype.is_unknown() else table[name]
            for name, dtype in table.schema().items()
        )
    )
    result = connection.to_pyarrow(projected)
    result.validate(full=True)
    return result.to_pylist()


def _day_payload(events, measure, boundary, *, source):
    """Reduce real per-event contributions into fixed-offset buckets."""
    measure = resolve_measure(events, measure, source=source)
    projected, fields = contributions(events, measure, source=source)
    offset = boundary_timezone(boundary).utcoffset(None)
    minutes = (offset.days * 86400 + offset.seconds) // 60
    day, bucket = local_day(projected._timestamp, minutes)
    leaf = projected.select("_unit", local_day=day, _timestamp=bucket, **fields)
    native = native_arithmetic(measure)
    grouped, _ = reduce_payload(
        leaf,
        ["_unit", "local_day", "_timestamp"],
        native_values=["sum_value"] if native else [],
        exact_values=[] if native else ["sum_value"],
    )
    return grouped


def _daily(connection, amounts, *, dtype="float64", times=None, units=None, exact=True):
    source = _source()
    raw = _events(connection, amounts, dtype=dtype, times=times, units=units)
    events = normalize_events(raw, source, "user_id")
    measure = resolve_measure(events, _measure(exact=exact), source=source)
    return _day_payload(events, measure, "UTC", source=source), measure


@pytest.mark.parametrize("aggregation", ["min", "max", "count_distinct"])
def test_resolver_uses_shared_unsupported_aggregation_refusal(aggregation):
    events = ibis.table({"amount": "int64"}, name="postings")
    with pytest.raises(TerrineError) as error:
        resolve_measure(events, _measure(aggregation=aggregation), source=_source())
    assert error.value.code == "terrine.metric.unsupported_aggregation"
    assert error.value.context["aggregation"] == aggregation


def test_sparse_days_keep_multiplicity_nulls_and_selected_unit(connection):
    times = [datetime(2025, 3, day, 12, tzinfo=UTC) for day in (1, 1, 3)]
    daily, measure = _daily(connection, [2**53 + 1, None, 7], dtype="int64", times=times)
    rows = _rows(connection, daily.order_by("local_day"))
    assert len(rows) == 2
    assert [row["event_count"] for row in rows] == ["2", "1"]
    assert [row["nonnull_count"] for row in rows] == ["1", "1"]
    assert [int(row["event_count"]) - int(row["nonnull_count"]) for row in rows] == [1, 0]
    assert [row["sum_value"] for row in rows] == [str(2**53 + 1), "7"]
    assert measure.value_type == "int64"
    null_daily, _ = _daily(connection, [None], dtype="int64")
    null_row = _rows(connection, null_daily)[0]
    assert (null_row["event_count"], null_row["nonnull_count"], null_row["sum_value"]) == (
        "1",
        "0",
        None,
    )
    empty_daily, _ = _daily(connection, [], dtype="int64")
    assert _rows(connection, empty_daily) == []


def test_day_bucket_preserves_same_named_event_and_filter_columns(connection):
    # Catches a bucket projection replacing physical local_day facts or predicates.
    source = replace(
        _source(fact="local_day"),
        properties={"score": PropertyBinding("score", "local_day", "float", "event_time", None)},
    )
    raw = _events(connection, [2, 3, 100], dtype="int64").rename(local_day="amount")
    events = normalize_events(raw, source, "user_id")
    measure = _measure(filters=(BoundFilter("score", "between", (1, 10)),))
    (row,) = _rows(connection, _day_payload(events, measure, "UTC", source=source))
    assert (row["local_day"], row["event_count"], row["sum_value"]) == (
        date(2025, 3, 1),
        "2",
        "5",
    )


def test_fixed_offset_day_state_is_independent_of_session_timezone(connection):
    source = _source(boundary="UTC-05:00")
    raw = _events(
        connection,
        [1, 2],
        dtype="int64",
        times=[
            datetime(2025, 3, 1, 4, 59, 59, 999999, tzinfo=UTC),
            datetime(2025, 3, 1, 5, tzinfo=UTC),
        ],
    )
    events = normalize_events(raw, source, "user_id")
    daily = _day_payload(events, _measure(), source.day_boundary, source=source)
    expected = [
        (date(2025, 2, 28), datetime(2025, 2, 28, 5, tzinfo=UTC)),
        (date(2025, 3, 1), datetime(2025, 3, 1, 5, tzinfo=UTC)),
    ]
    for zone in ("UTC", "America/Los_Angeles"):
        connection.raw_sql(f"SET TimeZone='{zone}'")
        rows = _rows(connection, daily.order_by("local_day"))
        assert [(row["local_day"], row["_timestamp"]) for row in rows] == expected


def test_exact_decimal_groups_cross_hugeint_without_losing_units(connection):
    maximum = 10**38 - 1
    values = [Decimal("99999999999999999999999999999999.999999")] * 2
    values += [Decimal("-99999999999999999999999999999999.999999"), Decimal("0.000001")]
    times = [datetime(2025, 3, day, tzinfo=UTC) for day in (1, 1, 2, 3)]
    daily, measure = _daily(connection, values, dtype="decimal(38,6)", times=times)
    rows = _rows(connection, daily.order_by("local_day"))
    assert [row["sum_value"] for row in rows] == [str(2 * maximum), str(-maximum), "1"]
    assert measure.value_type == "decimal(38, 6)"
    ordinary, _ = _daily(
        connection,
        [Decimal("12345678901234567890.123400"), Decimal("-12345678901234567890.123399")],
        dtype="decimal(26,6)",
    )
    assert _rows(connection, ordinary)[0]["sum_value"] == "1"


def test_filters_retain_unknown_provenance_and_false_overrides_unknown(connection):
    tier = PropertyBinding("tier", "tier_value", "string", "pre_exposure", "accounts")
    dimension = BoundDimension(
        "accounts", TableRef(kind="table", name="accounts"), "account", "id", None, {"tier": tier}
    )
    active = PropertyBinding("active", "active_value", "string", "event_time", None)
    source = _source(properties={"active": active}, dimensions=(dimension,))
    raw = _events(
        connection, [1, 2, 3, 4, 5], dtype="int64", active_value=["yes", "yes", None, "no", "yes"]
    )
    events = normalize_events(raw, source, "user_id")
    history = connection.create_table(
        "accounts",
        ibis.memtable({"id": ["0", "1", "3", "4"], "tier_value": ["gold", None, None, "silver"]}),
    )
    enriched = enrich_events(events, source, {"accounts": history})
    measure = _measure(
        filters=(
            BoundFilter("tier", "equals", ("gold",)),
            BoundFilter("active", "equals", ("yes",)),
        )
    )
    row = _rows(connection, _day_payload(enriched, measure, "UTC", source=source))[0]
    assert (row["event_count"], row["sum_value"], row["unknown_filter_count"]) == ("1", "1", "2")
    assert (row["unknown_filter_null_count"], row["unknown_filter_missing_count"]) == ("2", "1")
    null_literal = _measure(filters=(BoundFilter("active", "equals", (None,)),))
    row = _rows(connection, _day_payload(enriched, null_literal, "UTC", source=source))[0]
    assert (
        row["event_count"],
        row["unknown_filter_count"],
        row["unknown_filter_literal_count"],
    ) == ("0", "5", "5")
    in_literal = replace(null_literal, filters=(BoundFilter("active", "in", ("yes", None)),))
    row = _rows(connection, _day_payload(enriched, in_literal, "UTC", source=source))[0]
    assert (row["event_count"], row["unknown_filter_count"]) == ("3", "2")


def test_unfiltered_measures_have_no_unknown_filter_state(connection):
    daily, _ = _daily(connection, [1.0, 2.0])
    assert not set(_UNKNOWN_FIELDS) & set(daily.columns)
    assert _rows(connection, daily)[0]["event_count"] == "2"


def test_event_grouping_uses_same_filters_and_exact_original_counts(connection):
    source = _source(fact=None)
    raw = _events(connection, [None, None, None])
    events = normalize_events(raw, source, "user_id")
    measure = _measure(aggregation="count")
    projected, fields = contributions(
        events, resolve_measure(events, measure, source=source), source=source, counting=True
    )
    leaf = projected.select("_unit", "_timestamp", **fields)
    grouped, _ = reduce_payload(leaf, ["_unit", "_timestamp"], native_values=[], exact_values=[])
    row = _rows(connection, grouped)[0]
    assert row["event_count"] == "3"


@pytest.mark.parametrize(
    "values",
    [
        [10.0, -3.0, 0.25, 0.125],
        [1e16, 1.0, -1e16],
        [math.nextafter(1.0, 2.0), 1.0, -0.5],
        [math.ulp(0.0), math.ulp(0.0), -math.ulp(0.0)],
        [9e28, 9e28, -9e28],
    ],
)
def test_float_day_groups_meet_per_input_quantized_bound(connection, values):
    from tests._float_bound import window_sum_bound

    # Every bucket quantizes its original leaves before their exact addition.
    for order in list(dict.fromkeys(permutations(values)))[:3]:
        for days in ([1] * len(order), list(range(1, len(order) + 1))):
            times = [datetime(2025, 3, day, tzinfo=UTC) for day in days]
            daily, _ = _daily(connection, list(order), times=times)
            rows = _rows(connection, daily.order_by("local_day"))
            assert len(rows) == len(set(days))
            for row in rows:
                leaves = [
                    x for x, day in zip(order, days, strict=True) if day == row["local_day"].day
                ]
                exact = sum(map(Fraction, leaves), Fraction())
                assert is_exact_type(daily.schema()["sum_value"])
                assert abs(Fraction(int(row["sum_value"]), 10**9) - exact) <= window_sum_bound(
                    leaves
                )
                assert int(row["nonnull_count"]) == len(leaves)


def test_nonfinite_float_leaves_are_counted_not_summed(connection):
    times = [datetime(2025, 3, day, tzinfo=UTC) for day in (1, 2, 3)]
    daily, _ = _daily(connection, [math.inf, 2.5, math.nan], times=times)
    rows = _rows(connection, daily.order_by("local_day"))
    assert [
        (row["sum_value"], row["float64_nonfinite_count"], row["nonnull_count"]) for row in rows
    ] == [(None, "1", "1"), ("2500000000", "0", "1"), (None, "1", "1")]
    assert all(row["float64_unrepresentable_count"] == "0" for row in rows)


def test_native_float_day_groups_keep_finite_values_and_nonfinite_counts(connection):
    values = [0.1, math.inf, 0.2, math.nan, -0.3, 1e300]
    days = [1, 1, 2, 3, 3, 3]
    times = [datetime(2025, 3, day, tzinfo=UTC) for day in days]
    daily, _ = _daily(connection, values, times=times, exact=False)
    assert daily.schema()["sum_value"].is_float64()
    assert "float64_unrepresentable_count" not in daily.columns
    rows = _rows(connection, daily.order_by("local_day"))
    assert len(rows) == 3
    expected = {1: 0.1, 2: 0.2, 3: 1e300}
    for row in rows:
        leaves = [x for x, day in zip(values, days, strict=True) if day == row["local_day"].day]
        assert row["sum_value"] == pytest.approx(expected[row["local_day"].day], rel=1e-15, abs=0)
        assert row["float64_nonfinite_count"] == str(sum(not math.isfinite(x) for x in leaves))
        assert row["nonnull_count"] == str(len(leaves))


def test_only_exact_grouping_requires_backend_exact_capability(connection, monkeypatch):
    from terrine.backends.engines.duckdb import DuckDBDialect
    from tests.temporal.test_window_aggregates import _anchors, _local, _result
    from tests.temporal.test_window_aggregates import _rows as window_rows

    monkeypatch.setattr(DuckDBDialect, "supports_exact_prefix", lambda self, backend: False)
    native = _result(_anchors(connection), [_local(connection, [1.5], exact=False)])
    assert window_rows(connection, native)[0]["requested"] == 1.5
    with pytest.raises(TerrineError) as error:
        _result(_anchors(connection), [_local(connection, [1.5])])
    assert error.value.code == "terrine.temporal.exact_unsupported"


def test_construction_never_executes_queries(connection, monkeypatch):
    raw = _events(connection, [1.25, 2.5])
    source = _source()
    events = normalize_events(raw, source, "user_id")

    def blocked(*args, **kwargs):
        raise AssertionError("lazy state construction executed a query")

    monkeypatch.setattr(connection, "raw_sql", blocked)
    monkeypatch.setattr(connection, "execute", blocked)
    monkeypatch.setattr(connection, "to_pyarrow", blocked)
    measure = resolve_measure(events, _measure(), source=source)
    daily = _day_payload(events, measure, "UTC", source=source)
    schema = daily.schema()
    assert is_exact_type(schema["sum_value"])
    assert is_exact_type(schema["event_count"])


def test_numeric_filters_do_not_round_integer_properties_or_fractional_thresholds(connection):
    prop = PropertyBinding("score", "amount", "int", "event_time", None)
    source = _source(properties={"score": prop})
    raw = _events(connection, [2**53, 2**53 + 1], dtype="int64")
    events = normalize_events(raw, source, "user_id")
    measure = _measure(filters=(BoundFilter("score", "equals", (2**53 + 1,)),))
    row = _rows(connection, _day_payload(events, measure, "UTC", source=source))[0]
    assert (row["event_count"], row["sum_value"]) == ("1", str(2**53 + 1))
    raw = _events(connection, [1, 2, None], dtype="int64")
    events = normalize_events(raw, source, "user_id")
    measure = _measure(filters=(BoundFilter("score", "gt", (1.5,)),))
    row = _rows(connection, _day_payload(events, measure, "UTC", source=source))[0]
    assert (row["event_count"], row["sum_value"], row["unknown_filter_count"]) == ("1", "2", "1")


def test_high_scale_decimal_keeps_its_coefficient_and_scale(connection):
    daily, _ = _daily(
        connection,
        [Decimal("0.123456789012345678901234567890"), Decimal("-0.123456789012345678901234567889")],
        dtype="decimal(38,30)",
    )
    published = daily.mutate(published=narrow_exact(daily.sum_value, 30, "decimal(38,30)"))
    row = _rows(connection, published)[0]
    assert (row["sum_value"], row["published"], row["nonnull_count"]) == (
        "1",
        Decimal("1e-30"),
        "2",
    )


@pytest.mark.parametrize(
    "op,values,expected",
    [
        ("equals", ("0.1",), ["1", "0"]),
        ("equals", (0.1,), ["0", "0"]),
        ("not_equals", ("0.1",), ["0", "1"]),
        ("gt", ("0.105",), ["0", "1"]),
        ("gte", ("0.1",), ["1", "1"]),
        ("lt", ("0.105",), ["1", "0"]),
        ("lte", ("0.1",), ["1", "0"]),
        ("in", ("0.1", None), ["1", "0"]),
        ("not_in", ("0.1",), ["0", "1"]),
        ("between", ("0.10", "0.105"), ["1", "0"]),
    ],
)
def test_quoted_decimal_thresholds_are_exact_for_every_comparator(connection, op, values, expected):
    source = _source(
        properties={"price": PropertyBinding("price", "amount", "float", "event_time", None)}
    )
    raw = _events(
        connection,
        [Decimal("0.10"), Decimal("0.11")],
        dtype="decimal(8,2)",
        times=[datetime(2025, 3, day, tzinfo=UTC) for day in (1, 2)],
    )
    events = normalize_events(raw, source, "user_id")
    measure = _measure(filters=(BoundFilter("price", op, values),))
    daily = _day_payload(events, measure, "UTC", source=source)
    rows = _rows(connection, daily.order_by("local_day"))
    # Known-false rows contribute zero; NULL membership retains unknown provenance.
    counts = {row["local_day"].day: row["event_count"] for row in rows}
    assert [counts.get(day, "0") for day in (1, 2)] == expected
    if op == "in":
        assert rows[1]["unknown_filter_count"] == "1"


@pytest.mark.parametrize(
    "spelling", [" 0.1", "0.1 ", "+0.1", "01", ".1", "1.", "NaN", "Infinity", "1_0", "1e"]
)
def test_numeric_strings_require_strict_json_number_grammar(connection, spelling):
    source = _source(
        properties={"price": PropertyBinding("price", "amount", "float", "event_time", None)}
    )
    raw = _events(connection, [Decimal("0.10")], dtype="decimal(8,2)")
    events = normalize_events(raw, source, "user_id")
    measure = _measure(filters=(BoundFilter("price", "equals", (spelling,)),))
    with pytest.raises(TerrineError) as caught:
        _day_payload(events, measure, "UTC", source=source)
    assert caught.value.code == "terrine.semantic.schema_mismatch"


def test_decimal_exponents_lower_without_constructing_huge_integers(connection):
    source = _source(
        properties={"score": PropertyBinding("score", "amount", "int", "event_time", None)}
    )
    raw = _events(connection, [-1, 0, 1], dtype="int64")
    events = normalize_events(raw, source, "user_id")
    for spelling, op, expected in (
        ("1e999999999999999999999999999", "lt", "3"),
        ("-1e999999999999999999999999999", "gt", "3"),
        ("1e-999999999999999999999999999", "gte", "1"),
        ("-1e-999999999999999999999999999", "lte", "1"),
        ("0e999999999999999999999999999", "equals", "1"),
    ):
        measure = _measure(filters=(BoundFilter("score", op, (spelling,)),))
        row = _rows(connection, _day_payload(events, measure, "UTC", source=source))[0]
        assert row["event_count"] == expected


def test_numeric_looking_strings_on_string_properties_remain_strings(connection):
    source = _source(
        properties={"tag": PropertyBinding("tag", "tag", "string", "event_time", None)}
    )
    raw = _events(connection, [1, 2], dtype="int64", tag=["01", "1"])
    events = normalize_events(raw, source, "user_id")
    measure = _measure(filters=(BoundFilter("tag", "equals", ("01",)),))
    row = _rows(connection, _day_payload(events, measure, "UTC", source=source))[0]
    assert (row["event_count"], row["sum_value"]) == ("1", "1")


@pytest.mark.parametrize("unit_column", ("value_type", "event_count", "sum_value"))
def test_payload_names_cannot_merge_distinct_physical_units(connection, unit_column):
    raw = _events(connection, [1, 2], dtype="int64", units=["a", "b"])
    raw = raw.rename({unit_column: "user_id"})
    source = replace(_source(), units={"user": unit_column, "account": "account_id"})
    events = normalize_events(raw, source, unit_column)
    measure = resolve_measure(events, _measure(), source=source)
    daily = _day_payload(events, measure, "UTC", source=source)
    rows = _rows(connection, daily.order_by("_unit"))
    assert [(row["_unit"], row["sum_value"], row["event_count"]) for row in rows] == [
        ("a", "1", "1"),
        ("b", "2", "1"),
    ]


@pytest.mark.parametrize(
    ("value", "lower", "upper"),
    (
        ("150000000000000000000000000000000000000", "1.4e38", "1.6e38"),
        ("-150000000000000000000000000000000000000", "-1.6e38", "-1.4e38"),
    ),
)
def test_native_integer_alias_filter_does_not_trust_decimal38_range(
    connection, value, lower, upper
):
    query = (
        "SELECT 'u' AS user_id, 'a' AS account_id, "
        "TIMESTAMP '2025-03-01' AS occurred_at, "
        f"CAST('{value}' AS HUGEINT) AS amount"
    )
    raw = connection.sql(query)
    source = replace(
        _source(),
        relation=SqlQuery(kind="sql", sql=query),
        properties={"score": PropertyBinding("score", "amount", "float", "event_time", None)},
    )
    events = normalize_events(raw, source, "user_id")
    measure = _measure(filters=(BoundFilter("score", "between", (lower, upper)),))
    rows = _rows(connection, _day_payload(events, measure, "UTC", source=source))
    assert [(row["_unit"], row["event_count"], row["sum_value"]) for row in rows] == [
        ("u", "1", value)
    ]


@pytest.mark.parametrize(
    "boundary,offset_seconds",
    (
        ("UTC", 0),
        ("UTC+05:30", 19800),
        ("UTC-05:30", -19800),
        ("UTC+00:01", 60),
        ("UTC-00:01", -60),
    ),
)
def test_sparse_day_floors_negative_nanosecond_instants(connection, boundary, offset_seconds):
    nanoseconds = [
        -(2**63),
        -(2**63) + 2,
        -86400000000001,
        -999,
        -1,
        0,
        19799999999999,
        -19800000000001,
        2**63 - 2,
    ]
    raw = connection.create_table(
        "postings",
        pa.table(
            {
                "user_id": [str(i) for i in range(len(nanoseconds))],
                "account_id": ["a"] * len(nanoseconds),
                "occurred_at": pa.array(nanoseconds, type=pa.timestamp("ns")),
                "amount": pa.array([1] * len(nanoseconds), type=pa.int64()),
            }
        ),
    )
    source = _source(boundary=boundary)
    events = normalize_events(raw, source, "user_id")
    rows = _rows(connection, _day_payload(events, _measure(), boundary, source=source))
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    for row in rows:
        instant = nanoseconds[int(row["_unit"])]
        expected = (epoch + timedelta(seconds=instant // 10**9 + offset_seconds)).date()
        assert row["local_day"] == expected
        assert row["_timestamp"] == datetime.combine(
            expected, datetime.min.time(), UTC
        ) - timedelta(seconds=offset_seconds)


@pytest.mark.parametrize("op,values", (("gt", (0,)), ("in", (1.0, 2.0))))
def test_nonfinite_filter_properties_remain_unknown_without_poisoning_other_days(
    connection, op, values
):
    raw = connection.create_table(
        "postings",
        pa.table(
            {
                "user_id": ["u"] * 8,
                "account_id": ["a"] * 8,
                "occurred_at": pa.array(
                    [datetime(2025, 3, 1, tzinfo=UTC)] * 7 + [datetime(2025, 3, 3, tzinfo=UTC)],
                    type=pa.timestamp("us", "UTC"),
                ),
                "amount": pa.array([7] * 7 + [11], type=pa.int64()),
                "score": pa.array(
                    [math.nan, math.inf, -math.inf, math.nan, None, 1.0, math.nan, 2.0]
                ),
                "gate": [True, True, True, False, True, True, False, True],
            }
        ),
    )
    source = _source(
        properties={
            "score": PropertyBinding("score", "score", "float", "event_time", None),
            "gate": PropertyBinding("gate", "gate", "bool", "event_time", None),
        }
    )
    events = normalize_events(raw, source, "user_id")
    measure = _measure(
        filters=(BoundFilter("score", op, values), BoundFilter("gate", "equals", (True,)))
    )
    daily = _day_payload(events, measure, "UTC", source=source)
    rows = _rows(connection, daily.order_by("local_day"))
    assert [row["unknown_filter_count"] for row in rows] == ["4", "0"]
    assert [row["unknown_filter_nonfinite_count"] for row in rows] == ["3", "0"]
    assert [row["unknown_filter_null_count"] for row in rows] == ["1", "0"]
    assert [row["sum_value"] for row in rows] == ["7", "11"]


def test_exact_grouping_refusal_names_minimum_version(monkeypatch):
    from terrine.backends.dialect import dialect_scope
    from terrine.backends.engines import duckdb as duckdb_engine
    from terrine.temporal.daily import _require_backend

    table = ibis.table({"value": "int64"})

    class Backend:
        name = "duckdb"

    monkeypatch.setattr(ibis.Table, "_find_backend", lambda *args, **kwargs: Backend())
    monkeypatch.setattr(duckdb_engine, "version", lambda _: "1.4.0")

    with dialect_scope("duckdb"), pytest.raises(TerrineError) as caught:
        _require_backend(table)

    assert caught.value.context["minimum"] == "1.5"
