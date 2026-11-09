from __future__ import annotations

import duckdb
import ibis
import pytest

from terrine.backends.registry import ENGINES
from terrine.errors import TerrineError
from terrine.execution import _native


def test_bignum_without_decimal_operands_triggers_witness(monkeypatch, native_inputs):
    from terrine.execution.inputs import open_execution

    catalog, con = native_inputs
    monkeypatch.setattr(_native, "_exact_arithmetic_answer", lambda _: ("SELECT 1", (2,)))
    coefficient_type = ENGINES["duckdb"].dialect("duckdb").exact_type()
    coefficient = ibis.literal(1).cast(coefficient_type)

    with open_execution(catalog, con) as context:
        with pytest.raises(TerrineError) as caught:
            context.admit(coefficient.as_table())

    assert caught.value.code == "terrine.backend.capability_unavailable"
    assert caught.value.context["operation"] == "exact_arithmetic"
    assert caught.value.context["status"] == "witness_failed"


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
