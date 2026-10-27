from __future__ import annotations

import copy
import math
import pickle
import subprocess
import sys
from decimal import Decimal
from fractions import Fraction
from types import SimpleNamespace

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest
from ibis.backends.sqlite import Backend

from terrine import Model
from terrine.backends.dialect import dialect_scope
from terrine.errors import TerrineError
from tests.execution.test_inputs import _catalog


class Unregistered(Backend):
    name = "unregistered"

    def table(self, *args, **kwargs):
        raise AssertionError("source metadata accessed")

    def sql(self, *args, **kwargs):
        raise AssertionError("source SQL accessed")

    def get_schema(self, *args, **kwargs):
        raise AssertionError("source schema accessed")


def test_unregistered_admission_requires_explicit_choice():
    from terrine.backends import registry

    con = Unregistered()
    with pytest.raises(TerrineError) as caught:
        registry.resolve_engine(con)
    assert caught.value.code == "terrine.backend.capability_unavailable"
    assert {k: v for k, v in caught.value.context.items() if k != "route"} == {
        "backend": "unregistered",
        "operation": "query",
        "status": "unverified",
    }
    assert "allow_unverified=True" in caught.value.context["route"]
    from terrine import UnverifiedBackendWarning

    with pytest.warns(UnverifiedBackendWarning):
        assert registry.resolve_engine(con, allow_unverified=True) is registry.GENERIC
    for restored in (copy.deepcopy(caught.value), pickle.loads(pickle.dumps(caught.value))):
        assert restored.code == caught.value.code
        assert dict(restored.context) == dict(caught.value.context)
    with pytest.raises(TypeError):
        caught.value.context["backend"] = "changed"  # ty: ignore[invalid-assignment]


def test_unverified_read_permission_cannot_enable_writes():
    from terrine.backends.registry import GENERIC

    caps = GENERIC.capabilities
    for operation in ("dialect_admission", "exact_timestamps", "exact_decimals", "arrow_batches"):
        assert getattr(caps, operation) is None
        with pytest.raises(TerrineError):
            caps.require(operation, backend="unregistered", route="verified engine")
        caps.require(
            operation, backend="unregistered", route="verified engine", allow_unverified=True
        )
    for operation in (
        "snapshot",
        "transaction_view",
        "retained_snapshot",
        "table_publication",
        "atomic_manifest_publish",
        "runtime_reader",
        "native_parquet_unload",
        "scan_estimate",
    ):
        with pytest.raises(TerrineError) as caught:
            caps.require(
                operation, backend="unregistered", route="verified engine", allow_unverified=True
            )
        assert caught.value.context["status"] == (
            "unverified" if operation == "scan_estimate" else "unsupported"
        )


def test_generic_feature_metadata_is_driver_free():
    code = """
import builtins
original = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'ibis' or name.startswith('ibis.'):
        raise ImportError('Ibis unavailable')
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
from terrine.backends.registry import ENGINES, get_engine, engine_supports
assert 'generic' not in ENGINES
assert get_engine('generic').status == 'unverified'
assert get_engine('sqlite') is None
for feature in ('naive_anchor_utc', 'first_occurrence', 'event_resolution'):
    assert engine_supports('generic', feature)
for feature in ('quantile', 'approx_distinct', 'warehouse_preflight'):
    assert not engine_supports('generic', feature)
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=30)


@pytest.mark.parametrize("constructor", ["from_catalog", "from_tables", "from_increment"])
def test_model_binding_flag_is_boolean(constructor):
    kwargs = {"con": Unregistered(), "allow_unverified": "true"}
    if constructor == "from_tables":
        kwargs.update(units={}, events={})
        args = ()
    else:
        kwargs["readiness"] = {}
        args = (_catalog(),)
    with pytest.raises(TerrineError) as caught:
        getattr(Model, constructor)(*args, **kwargs)
    assert caught.value.code == "terrine.model.invalid_binding"


def test_admitted_engine_cannot_be_reused_for_another_identity():
    from terrine.backends.registry import ENGINES, GENERIC
    from terrine.execution.backend import backend_provider

    con = ibis.duckdb.connect()
    try:
        for connection, engine in ((con, GENERIC), (Unregistered(), ENGINES["duckdb"])):
            with pytest.raises(TerrineError):
                backend_provider(connection, engine=engine)
    finally:
        con.disconnect()


def test_portable_policy_types_and_flags():
    from terrine.backends.dialect import Dialect, dialect_for_name
    from terrine.backends.numeric import exact_literal, exact_type, is_exact_type

    policy = dialect_for_name("generic")
    assert isinstance(policy, Dialect) and policy.name == "generic"
    assert policy.exact_type() == dt.Decimal(38, 9)
    assert policy.wide_type() == dt.Decimal(38, 0)
    assert policy.coefficient_type() == dt.Decimal(38, 0)
    assert not policy.has_scaled_float_state
    assert not policy.has_native_asof
    assert policy.supports_exact_prefix("sqlite")
    assert is_exact_type(dt.Decimal(38, 9)) and is_exact_type(dt.Decimal(38, 0))
    assert not is_exact_type(dt.Decimal(38, 2))
    with dialect_scope("generic"):
        assert exact_type() == dt.Decimal(38, 9)
        count = exact_literal(1)
        assert count.type() == dt.Decimal(38, 0)
        state = ibis.literal(Decimal("0.5"), type=dt.Decimal(38, 9))
        coefficient = ibis.literal(7, type=dt.Decimal(38, 0))
        # Literals anchor to the first exact operand; other operands do not change them.
        assert exact_literal(0, state).type() == dt.Decimal(38, 9)
        assert exact_literal(0, ibis.literal(1.5), coefficient).type() == dt.Decimal(38, 0)
        assert exact_literal(0, ibis.literal(2.5)).type() == dt.Decimal(38, 0)


def test_portable_policy_compiles_every_operation_without_native_bindings():
    """Each base operation builds an Ibis expression; nothing raises or needs a UDF."""
    from terrine.backends.dialect import dialect_for_name

    policy = dialect_for_name("generic")
    table = ibis.table(
        {
            "f": "float64",
            "n": "int64",
            "c": "decimal(38, 0)",
            "s": "decimal(38, 9)",
            "t": "timestamp(6)",
            "a": "timestamp('UTC', 6)",
        },
        name="events",
    )
    exact, nonfinite, unrepresentable = policy.exact_float(table.f)
    assert (exact.type(), nonfinite.type(), unrepresentable.type()) == (
        dt.Decimal(38, 9),
        dt.boolean,
        dt.boolean,
    )
    assert policy.exact_ratio(table.s, table.c).type().is_float64()
    assert policy.exact_is_nonfinite(table.s).type().is_boolean()
    assert policy.exact_to_float(table.s)[0].type().is_float64()
    assert policy.exact_sum(table.c).type() == dt.Decimal(38, 0)
    assert policy.exact_subtract(table.s, table.s).type() == dt.Decimal(38, 9)
    assert policy.truncating_quotient(table.n, 7).type().is_int64()
    assert policy.wide_quotient(table.c, table.c).type() == dt.Decimal(38, 0)
    assert policy.decimal_coefficient(table.s, 9).type() == dt.Decimal(38, 0)
    assert policy.narrow_exact(table.c, 2, dt.Decimal(38, 2)).type() == dt.Decimal(38, 2)
    assert policy.utc_wall_clock(table.a).type() == dt.Timestamp(scale=6)
    assert policy.utc_instant(table.t).type() == dt.Timestamp(timezone="UTC", scale=6)
    assert policy.timestamp_text(table.a).type().is_string()
    assert policy.timestamp_order_key(table.a).equals(table.a)
    assert policy.reverse_time_key(table.t).type() == dt.Decimal(38, 0)
    assert policy.timestamp_extremes(dt.Timestamp(scale=6))[1].type() == dt.Timestamp(scale=6)
    assert policy.integer_sequence(3, "ordinal").schema()["ordinal"].is_int64()
    with pytest.raises(TerrineError) as caught:
        policy.timestamp_parts(table.t.cast(dt.Timestamp(scale=3)))
    assert caught.value.code == "terrine.temporal.timestamp_order_unsupported"
    with pytest.raises(ValueError):
        policy.exact_to_float(table.s, 2)


_UNIT_ROUNDOFF = Fraction(1, 2**53)


def _admission_threshold(scale: int, count: int) -> Fraction:
    """Independent oracle: the largest admissible magnitude sum for ``count`` rows.

    With each magnitude converted within ``u`` and summed through ``count - 1``
    roundings, the computed bound ``B`` satisfies ``|B - S| <= e * S`` for
    ``e = (1 + u)(1 + gamma) - 1`` and ``gamma = (n - 1)u / (1 - (n - 1)u)``, so
    the true sum is below the limit whenever ``B < limit * (1 - e)``.
    """
    limit = Fraction(10**38 - 1, 10**scale)
    additions = (count - 1) * _UNIT_ROUNDOFF
    gamma = additions / (1 - additions)
    error = (1 + _UNIT_ROUNDOFF) * (1 + gamma) - 1
    return limit * (1 - error)


def _double_at_least(value: Fraction) -> float:
    rounded = float(value)
    return rounded if Fraction(rounded) >= value else math.nextafter(rounded, math.inf)


def _double_at_most(value: Fraction) -> float:
    rounded = float(value)
    return rounded if Fraction(rounded) <= value else math.nextafter(rounded, 0.0)


@pytest.mark.parametrize("scale", [9, 0])
def test_accumulation_admission_is_sound_and_tight_at_the_threshold(scale):
    """Admission never trusts a bound that could hide an overflow, and gives up at most
    the derived slack plus a few units of the guard's own rounding."""
    from terrine.backends.dialect import dialect_for_name

    policy = dialect_for_name("generic")
    counts = [0, 1, 2, 3, 1000, 2**40]
    cases = {}
    for count in counts:
        threshold = _admission_threshold(scale, max(count, 1))
        cases[f"at_{count}"] = (_double_at_least(threshold), count, False)
        # The closed-form guard spends at most (n + 16)u beyond the derived threshold.
        margin = threshold * (1 - (count + 16) * _UNIT_ROUNDOFF)
        cases[f"below_{count}"] = (_double_at_most(margin), count, True)
    limit = Fraction(10**38 - 1, 10**scale)
    cases["limit_single"] = (_double_at_most(limit), 1, False)
    cases["empty"] = (0.0, 0, True)
    cases["null_bound"] = (None, 0, True)
    # Beyond 2**52 additions the rounding model no longer bounds anything.
    cases["void_model"] = (0.0, 2**53, False)
    con = ibis.duckdb.connect()
    try:
        table = con.create_table(
            "bounds",
            pa.table(
                {
                    "case": list(cases),
                    "bound": pa.array([bound for bound, _, _ in cases.values()], pa.float64()),
                    "rows": pa.array([count for _, count, _ in cases.values()], pa.int64()),
                }
            ),
        )
        admitted = policy.admits_accumulation(table.bound, table.rows, dt.Decimal(38, scale))
        rows = con.to_pyarrow(table.select("case", admitted=admitted)).to_pylist()
    finally:
        con.disconnect()
    assert {row["case"]: row["admitted"] for row in rows} == {
        name: expected for name, (_, _, expected) in cases.items()
    }


@pytest.mark.parametrize("operation", ["utc_wall_clock", "utc_instant"])
def test_portable_nanosecond_utc_conversion_refuses_before_any_query(operation):
    """The portable conversions rebuild instants from microsecond intervals, so a
    nanosecond column is refused with the capability code while the expression is
    built, not with a backend or Ibis error at execution."""
    from terrine.backends import timestamps

    con = ibis.duckdb.connect()
    try:
        table = con.create_table(
            "ticks",
            pa.table({"naive": pa.array([1, -1], pa.int64()).cast(pa.timestamp("ns"))}),
        )
        assert table.naive.type() == dt.Timestamp(scale=9)
        with dialect_scope("generic"), pytest.raises(TerrineError) as caught:
            getattr(timestamps, operation)(table.naive)
    finally:
        con.disconnect()
    assert caught.value.code == "terrine.backend.capability_unavailable"
    assert caught.value.context["operation"] == operation
    assert caught.value.context["backend"] == "generic"


_DAY_NS = 86_400 * 10**9
_MINUTE_NS = 60 * 10**9


def _local_day_oracle(instant_ns: int, offset_minutes: int):
    """Independent oracle in integer nanoseconds: the local calendar day under a fixed
    offset and the UTC instant at which it starts."""
    from datetime import UTC, date, datetime, timedelta

    local = instant_ns + offset_minutes * _MINUTE_NS
    day_number = local // _DAY_NS
    start_ns = day_number * _DAY_NS - offset_minutes * _MINUTE_NS
    assert start_ns % 10**9 == 0
    return (
        date(1970, 1, 1) + timedelta(days=day_number),
        datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=start_ns // 10**9),
    )


@pytest.mark.parametrize("policy", ["generic", "duckdb"])
@pytest.mark.parametrize("unit", ["us", "ns"])
def test_local_day_boundaries_match_the_fixed_offset_oracle(policy, unit):
    """Day buckets and their start instants, portable and native, at instants one
    tick before, at and after local midnight under west, UTC and east offsets, before
    and after the epoch; aware microseconds and naive nanoseconds (which denote UTC,
    and which DuckDB holds only without a zone)."""
    from datetime import UTC

    from terrine.backends.timestamps import local_day

    tick = 1000 if unit == "us" else 1
    # UTC midnights: a 2025 day, the day before the epoch, and the epoch itself.
    midnights_ns = [1_743_292_800 * 10**9, -_DAY_NS, 0]
    offsets = [-300, 0, 330, -720, 840]
    cases: list[tuple[int, int]] = []
    for offset in offsets:
        for midnight in midnights_ns:
            local_midnight = midnight - offset * _MINUTE_NS
            cases.extend(
                (offset, at)
                for at in (local_midnight - tick, local_midnight, local_midnight + tick)
            )
    scale = 1 if unit == "ns" else 1000
    stored = pa.timestamp("ns") if unit == "ns" else pa.timestamp("us", tz="UTC")
    con = ibis.duckdb.connect()
    try:
        table = con.create_table(
            "instants",
            pa.table(
                {
                    "case": list(range(len(cases))),
                    "offset": pa.array([offset for offset, _ in cases], pa.int64()),
                    "at": pa.array([at // scale for _, at in cases], pa.int64()).cast(stored),
                }
            ),
        )
        results = {}
        with dialect_scope(policy):
            for offset in offsets:
                subset = table.filter(table.offset == offset)
                day, start = local_day(subset.at, offset)
                assert day.type() == dt.date
                start_type = start.type()
                assert isinstance(start_type, dt.Timestamp) and start_type.timezone == "UTC"
                for row in con.to_pyarrow(subset.select("case", day=day, start=start)).to_pylist():
                    results[row["case"]] = (row["day"], row["start"])
    finally:
        con.disconnect()
    for index, (offset, at) in enumerate(cases):
        day, start = results[index]
        assert (day, start.astimezone(UTC)) == _local_day_oracle(at, offset), (offset, at)


def test_cohort_summary_uses_the_passed_policy_scope(monkeypatch):
    from terrine.backends import dialect as policies
    from terrine.compile.shapes import cohorts

    seen = []

    def record(study, *, grouping, value_column):
        seen.append(policies.current_dialect().name)
        return study

    monkeypatch.setattr(cohorts, "_compile_cohorts", record)
    study = SimpleNamespace(table=ibis.table({"value": "int64"}, name="study"))
    cohorts.compile_cohorts(study, grouping=None, value_column="value", dialect="generic")  # ty: ignore[invalid-argument-type]
    assert seen == ["generic"]
    assert policies.current_dialect().name == "duckdb"
