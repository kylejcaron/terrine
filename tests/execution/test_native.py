from __future__ import annotations

from decimal import Decimal

import duckdb
import ibis
import pytest

from terrine.backends.registry import ENGINES
from terrine.errors import TerrineError
from terrine.execution import _native


def test_exact_witness_runs_once_per_execution_context(monkeypatch, native_inputs):
    from terrine.execution.inputs import open_execution

    catalog, con = native_inputs
    calls = []
    monkeypatch.setattr(_native, "verify_exact_arithmetic", lambda _: calls.append(None))

    with open_execution(catalog, con) as context:
        context.admit(ibis.literal(1).as_table())
        assert calls == []

        exact_query = ibis.literal(Decimal("1.0")).as_table()
        context.admit(exact_query)
        context.admit(exact_query)

    assert calls == [None]


def test_witness_follows_exact_coefficients_not_stored_counts(monkeypatch, native_inputs):
    from terrine.execution.inputs import open_execution

    catalog, con = native_inputs
    calls = []
    monkeypatch.setattr(_native, "verify_exact_arithmetic", lambda _: calls.append(None))
    coefficient_type = ENGINES["duckdb"].dialect("duckdb").exact_type()
    coefficient = ibis.literal(1).cast(coefficient_type)
    assert not coefficient.type().is_decimal()
    # A persisted count layer reads its HUGEINT counts back as decimal(38, 0).
    layer = ibis.table({"event_count": "decimal(38, 0)"}, name="layer")

    with open_execution(catalog, con) as context:
        context.admit(layer.select(window=layer.event_count - layer.event_count))
        assert calls == []
        context.admit(coefficient.as_table())

    assert calls == [None]



def test_exact_witness_accepts_known_answers():
    con = ibis.duckdb.connect()
    try:
        assert _native.verify_exact_arithmetic(con) is None
    finally:
        con.disconnect()


def test_exact_witness_refuses_on_wrong_known_answer(monkeypatch):
    con = ibis.duckdb.connect()
    try:
        monkeypatch.setattr(_native, "_exact_arithmetic_answer", lambda _: ("SELECT 1", (2,)))

        with pytest.raises(TerrineError) as caught:
            _native.verify_exact_arithmetic(con)

        assert caught.value.code == "terrine.backend.capability_unavailable"
        assert caught.value.context["status"] == "witness_failed"
    finally:
        con.disconnect()


def test_exact_witness_refuses_shadowed_aggregate_before_udf():
    con = duckdb.connect()
    calls = []
    con.create_function(
        "witness_probe",
        lambda: calls.append(True) or 1,
        return_type=duckdb.sqltype("INTEGER"),
    )
    con.execute("CREATE MACRO sum(value) AS witness_probe()")
    try:
        with pytest.raises(TerrineError) as caught:
            _native.verify_exact_arithmetic(ibis.duckdb.from_connection(con))

        assert caught.value.code == "terrine.execution.snapshot_unavailable"
        assert calls == []
    finally:
        con.close()
