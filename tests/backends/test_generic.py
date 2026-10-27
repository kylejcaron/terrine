from __future__ import annotations

import copy
import pickle
import subprocess
import sys
from decimal import Decimal
from types import SimpleNamespace

import ibis
import ibis.expr.datatypes as dt
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
