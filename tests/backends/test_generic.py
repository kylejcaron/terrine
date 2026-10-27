from __future__ import annotations

import copy
import pickle
import subprocess
import sys

import ibis
import pytest
from ibis.backends.sqlite import Backend

from terrine import Model
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
