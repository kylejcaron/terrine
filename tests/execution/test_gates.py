from __future__ import annotations

from types import SimpleNamespace

import ibis
import pyarrow as pa
import pytest

from terrine.data import EligibilitySummary, RowCheck, RowCheckGroup
from terrine.errors import RefusalSpec, TerrineError
from terrine.execution.gates import (
    GatedStream,
    fold,
    gate_result,
    gate_totals,
    gated_output,
    require_resolved,
    run_schedule,
)

_A = RefusalSpec("terrine.test.gate_a", "check a failed {failed_count}")
_B = RefusalSpec("terrine.test.gate_b", "check b failed {failed_count} for {output}")
_C = RefusalSpec("terrine.test.gate_c", "check c failed {failed_count}")
_SUMMARY = ("candidate", "excluded_false", "unknown", "retained")


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


def _audit(con, eligible, flag_a, flag_b):
    return con.create_table(
        "audit",
        pa.table(
            {
                "unit": list("abc")[: len(eligible)],
                "__terrine_eligible": pa.array(eligible, pa.bool_()),
                "fa": pa.array(flag_a, pa.bool_()),
                "fb": pa.array(flag_b, pa.bool_()),
            }
        ),
        overwrite=True,
    )


def _count_check(relation, refusal):
    """A one-row aggregate check shaped like the temporal engine's string counts."""
    return SimpleNamespace(
        table=relation.aggregate(failed_0=relation.count().cast("string")),
        failures=(RowCheck("failed_0", refusal, {}),),
    )


def _record(con, relation):
    rows = con.to_pyarrow(relation).to_pylist()
    assert len(rows) == 1
    return rows[0]


def _summary_columns(*counts):
    return {f"__terrine_gate_{name}": count for name, count in zip(_SUMMARY, counts, strict=True)}


def test_gate_totals_counts_each_check_and_summary_in_one_relation(con):
    audit = _audit(con, [True, False, None], [False, None, True], [True, True, False])
    run = [RowCheckGroup(audit, (RowCheck("fa", _A, {}), RowCheck("fb", _B, {"output": "x"})))]
    record = _record(con, gate_totals(audit, run))
    assert record == {
        "__terrine_gate_candidate": 3,
        "__terrine_gate_excluded_false": 1,
        "__terrine_gate_unknown": 1,
        "__terrine_gate_retained": 1,
        "__terrine_gate_check_0": 1,
        "__terrine_gate_check_1": 2,
    }
    assert all(
        pa.types.is_int64(field.type) for field in con.to_pyarrow(gate_totals(audit, run)).schema
    )


def test_gate_totals_cross_joins_a_group_on_another_relation(con):
    audit = _audit(con, [True, False, None], [False, None, True], [True, True, False])
    narrowed = audit.filter(audit.unit != "c")
    run = [
        RowCheckGroup(narrowed, (RowCheck("fa", _A, {}),)),
        _count_check(audit.filter(audit.unit == "a"), _C),
        RowCheckGroup(audit, (RowCheck("fb", _B, {"output": "x"}),)),
    ]
    record = _record(con, gate_totals(audit, run))
    assert record == {
        "__terrine_gate_candidate": 3,
        "__terrine_gate_excluded_false": 1,
        "__terrine_gate_unknown": 1,
        "__terrine_gate_retained": 1,
        "__terrine_gate_check_0": 0,
        "__terrine_gate_check_1": "1",
        "__terrine_gate_check_2": 2,
    }
    assert _record(con, gate_totals(None, run[:2])) == {
        "__terrine_gate_check_0": 0,
        "__terrine_gate_check_1": "1",
    }
    with pytest.raises(ValueError):
        gate_totals(None, [])


def test_gate_result_raises_in_schedule_order_across_kinds(con):
    audit = _audit(con, [True, False, None], [False, None, True], [True, True, False])
    group = RowCheckGroup(audit, (RowCheck("fa", _A, {}), RowCheck("fb", _B, {"output": "x"})))
    check = _count_check(audit.filter(audit.unit == "a"), _C)

    run = [group, check]
    with pytest.raises(TerrineError) as error:
        gate_result(_record(con, gate_totals(audit, run)), run, summary=True)
    assert error.value.code == "terrine.test.gate_a"
    assert error.value.context == {"failed_count": 1}

    run = [check, group]
    with pytest.raises(TerrineError) as error:
        gate_result(_record(con, gate_totals(audit, run)), run, summary=True)
    assert error.value.code == "terrine.test.gate_c"
    assert error.value.context == {"failed_count": 1}

    run = [RowCheckGroup(audit, (RowCheck("fb", _B, {"output": "x"}),)), check]
    with pytest.raises(TerrineError) as error:
        gate_result(_record(con, gate_totals(None, run)), run, summary=False)
    assert error.value.code == "terrine.test.gate_b"
    assert error.value.context == {"output": "x", "failed_count": 2}

    clean = _audit(con, [True, False, None], [False, None, False], [None, False, False])
    run = [RowCheckGroup(clean, (RowCheck("fa", _A, {}), RowCheck("fb", _B, {"output": "x"})))]
    record = _record(con, gate_totals(clean, run))
    assert gate_result(record, run, summary=False) is None
    summary = gate_result(record, run, summary=True)
    assert summary == EligibilitySummary(candidate=3, excluded_false=1, unknown=1, retained=1)
    with pytest.raises(TerrineError) as error:
        require_resolved(summary)
    assert error.value.code == "terrine.study.eligibility_unknown"
    assert error.value.context == {
        "candidate": 3,
        "excluded_false": 1,
        "unknown": 1,
        "retained": 1,
    }
    resolved = EligibilitySummary(candidate=3, excluded_false=1, unknown=0, retained=2)
    assert require_resolved(resolved) is resolved


def test_gate_result_rejects_a_malformed_record():
    audit = ibis.table({"unit": "string", "fa": "boolean"}, name="audit")
    run = [RowCheckGroup(audit, (RowCheck("fa", _A, {}),)), _count_check(audit, _C)]
    summary = _summary_columns(3, 1, 0, 2)
    good = {"__terrine_gate_check_0": 0, "__terrine_gate_check_1": "0", **summary}
    assert gate_result(good, run, summary=True) == EligibilitySummary(3, 1, 0, 2)
    malformed = [
        {},
        {**good, "__terrine_gate_check_1": "12x"},
        {**good, "__terrine_gate_check_1": 0},
        {**good, "__terrine_gate_check_0": "0"},
        {**good, "__terrine_gate_check_0": True},
        {**good, "__terrine_gate_check_0": -1},
        {**good, "__terrine_gate_check_0": None},
        {**good, "__terrine_gate_retained": 1},
        {key: value for key, value in good.items() if key != "__terrine_gate_unknown"},
    ]
    for record in malformed:
        with pytest.raises(TerrineError) as error:
            gate_result(record, run, summary=True)
        assert error.value.code == "terrine.metadata.invalid_eligibility_summary"
    checks_only = {"__terrine_gate_check_0": 0, "__terrine_gate_check_1": "0"}
    assert gate_result(checks_only, run, summary=False) is None
    with pytest.raises(TerrineError) as error:
        gate_result(checks_only, run, summary=True)
    assert error.value.code == "terrine.metadata.invalid_eligibility_summary"


def test_an_earlier_refusal_precedes_a_later_malformed_count_or_summary():
    audit = ibis.table({"unit": "string", "fa": "boolean", "fb": "boolean"}, name="audit")
    run = [
        RowCheckGroup(audit, (RowCheck("fa", _A, {}), RowCheck("fb", _B, {"output": "x"}))),
        _count_check(audit, _C),
    ]
    flagged = {"__terrine_gate_check_0": 0, "__terrine_gate_check_1": 2}
    later_malformed = {**flagged, "__terrine_gate_check_2": "2x", **_summary_columns(3, 1, 0, 2)}
    summary_malformed = {**flagged, "__terrine_gate_check_2": "0", **_summary_columns(3, 1, 0, 9)}
    summary_missing = {**flagged, "__terrine_gate_check_2": "0"}
    for record in (later_malformed, summary_malformed, summary_missing):
        with pytest.raises(TerrineError) as error:
            gate_result(record, run, summary=True)
        assert error.value.code == "terrine.test.gate_b"
        assert error.value.context == {"output": "x", "failed_count": 2}
    # Within one item every count is still validated before its refusal.
    with pytest.raises(TerrineError) as error:
        gate_result({**later_malformed, "__terrine_gate_check_0": -1}, run, summary=True)
    assert error.value.code == "terrine.metadata.invalid_eligibility_summary"


def test_fold_keeps_unfoldable_checks_in_place():
    audit = ibis.table({"unit": "string", "fa": "boolean"}, name="audit")
    group = RowCheckGroup(audit, (RowCheck("fa", _A, {}),))
    check = _count_check(audit, _C)
    unfoldable = SimpleNamespace(validate=lambda execute: None, reads=lambda: ())
    assert fold([group, check, unfoldable, group]) == [[group, check], unfoldable, [group]]
    assert fold([]) == [[]]
    assert fold([unfoldable]) == [unfoldable, []]
    assert fold([unfoldable, unfoldable, check]) == [unfoldable, unfoldable, [check]]
    assert fold([group, unfoldable]) == [[group], unfoldable, []]


def test_run_schedule_executes_runs_and_unfoldable_checks_in_order(con):
    audit = _audit(con, [True, False, None], [False, None, True], [True, True, False])
    events = []

    def execute(query):
        events.append(query)
        return con.to_pyarrow(query)

    def unfoldable(label):
        def validate(execute):
            events.append(label)

        return SimpleNamespace(validate=validate, reads=lambda: ())

    anchors = SimpleNamespace(validate=lambda *, execute: events.append("anchors"))
    clean = RowCheckGroup(audit, (RowCheck("fb", _B, {"output": "x"}),))
    passing = RowCheckGroup(audit.filter(audit.unit == "a"), (RowCheck("fa", _A, {}),))
    check = _count_check(audit.filter(audit.unit == "zz"), _C)
    final = RowCheckGroup(audit, (RowCheck("fa", _A, {}),))
    compiled = SimpleNamespace(
        _anchors=anchors,
        _checks=(passing, check, unfoldable("first"), unfoldable("second"), final),
    )
    assert run_schedule(compiled, execute) == [final]
    assert events[0] == "anchors"
    assert events[1].equals(gate_totals(None, [passing, check]))
    assert events[2:] == ["first", "second"]

    events.clear()
    compiled = SimpleNamespace(_anchors=anchors, _checks=(clean, unfoldable("never"), final))
    with pytest.raises(TerrineError) as error:
        run_schedule(compiled, execute)
    assert error.value.code == "terrine.test.gate_b"
    assert error.value.context == {"output": "x", "failed_count": 2}
    assert events[0] == "anchors" and len(events) == 2

    events.clear()
    compiled = SimpleNamespace(_anchors=anchors, _checks=(unfoldable("only"),))
    assert run_schedule(compiled, execute) == []
    assert events == ["anchors", "only"]


def _batches(schema, *rows):
    return pa.RecordBatchReader.from_batches(
        schema, [pa.RecordBatch.from_pylist(list(batch), schema=schema) for batch in rows]
    )


def test_gated_stream_resolves_an_empty_stream_from_the_aggregate(con):
    audit = _audit(con, [False, False, None], [True, True, False], [False, False, False])
    run = [RowCheckGroup(audit, (RowCheck("fa", _A, {}), RowCheck("fb", _B, {"output": "x"})))]
    output = audit.filter(audit.__terrine_eligible.fill_null(False)).select("unit")
    reader = con.to_pyarrow_batches(gated_output(output, audit, run), chunk_size=1)
    stream = GatedStream(reader, run, ["unit"], lambda: con.to_pyarrow(gate_totals(audit, run)))
    assert stream.schema == pa.schema([("unit", pa.string())])
    assert stream.summary is None
    with pytest.raises(TerrineError) as error:
        list(stream)
    assert error.value.code == "terrine.test.gate_a"
    assert error.value.context == {"failed_count": 2}
    stream.close()

    unresolved = _audit(con, [False, None, None], [False, False, False], [False, False, False])
    run = [RowCheckGroup(unresolved, (RowCheck("fa", _A, {}),))]
    schema = pa.schema([("unit", pa.string()), ("__terrine_gate_check_0", pa.int64())])
    resolve_calls = []

    def resolve_empty():
        resolve_calls.append(True)
        return con.to_pyarrow(gate_totals(unresolved, run))

    stream = GatedStream(_batches(schema), run, ["unit"], resolve_empty)
    with pytest.raises(TerrineError) as error:
        list(stream)
    assert error.value.code == "terrine.study.eligibility_unknown"
    assert error.value.context == {
        "candidate": 3,
        "excluded_false": 1,
        "unknown": 2,
        "retained": 0,
    }
    assert resolve_calls == [True]

    excluded = _audit(con, [False, False], [False, False], [False, False])
    run = [RowCheckGroup(excluded, (RowCheck("fa", _A, {}),))]
    stream = GatedStream(
        _batches(schema), run, ["unit"], lambda: con.to_pyarrow(gate_totals(excluded, run))
    )
    assert list(stream) == []
    assert stream.summary == EligibilitySummary(2, 2, 0, 0)


def test_gated_stream_strips_gate_columns_and_reads_the_first_batch(con):
    audit = _audit(con, [True, False, True], [False, None, False], [None, False, False])
    run = [RowCheckGroup(audit, (RowCheck("fa", _A, {}), RowCheck("fb", _B, {"output": "x"})))]
    output = audit.filter(audit.__terrine_eligible.fill_null(False)).select("unit", "fa")
    reader = con.to_pyarrow_batches(gated_output(output, audit, run), chunk_size=1)

    def resolve_empty():
        raise AssertionError("a stream with rows resolves its gate from the first batch")

    stream = GatedStream(reader, run, ["unit", "fa"], resolve_empty)
    assert stream.schema == pa.schema([("unit", pa.string()), ("fa", pa.bool_())])
    batches = list(stream)
    stream.close()
    assert len(batches) == 2
    assert all(batch.schema == stream.schema for batch in batches)
    assert sorted(row["unit"] for batch in batches for row in batch.to_pylist()) == ["a", "c"]
    assert stream.summary == EligibilitySummary(3, 1, 0, 2)

    schema = pa.schema(
        [
            ("unit", pa.string()),
            *((f"__terrine_gate_{name}", pa.int64()) for name in _SUMMARY),
            ("__terrine_gate_check_0", pa.int64()),
            ("__terrine_gate_check_1", pa.int64()),
        ]
    )
    gate = _summary_columns(3, 1, 0, 2)
    flagged = {**gate, "__terrine_gate_check_0": 0, "__terrine_gate_check_1": 1}
    stream = GatedStream(
        _batches(schema, [{"unit": "a", **flagged}], [{"unit": "c", **flagged}]),
        run,
        ["unit"],
        resolve_empty,
    )
    batches = iter(stream)
    with pytest.raises(TerrineError) as error:
        next(batches)
    assert error.value.code == "terrine.test.gate_b"
    assert error.value.context == {"output": "x", "failed_count": 1}
    assert stream.summary is None

    clean = {**gate, "__terrine_gate_check_0": 0, "__terrine_gate_check_1": 0}
    stream = GatedStream(
        _batches(schema, [], [{"unit": "a", **clean}], [{"unit": "c", **clean}]),
        run,
        ["unit"],
        resolve_empty,
    )
    assert [row["unit"] for batch in stream for row in batch.to_pylist()] == ["a", "c"]
    assert stream.summary == EligibilitySummary(3, 1, 0, 2)


def test_gated_stream_resolves_the_gate_before_the_first_batch_leaves():
    run = [RowCheckGroup(ibis.memtable({"fa": [False]}), (RowCheck("fa", _A, {}),))]
    schema = pa.schema(
        [
            ("unit", pa.string()),
            *((f"__terrine_gate_{name}", pa.int64()) for name in _SUMMARY),
            ("__terrine_gate_check_0", pa.int64()),
        ]
    )
    clean = {**_summary_columns(3, 1, 0, 2), "__terrine_gate_check_0": 0}

    def resolve_empty():
        raise AssertionError("a stream with rows resolves its gate from the first batch")

    stream = GatedStream(
        _batches(schema, [], [{"unit": "a", **clean}], [{"unit": "c", **clean}]),
        run,
        ["unit"],
        resolve_empty,
    )
    assert stream.resolve() == EligibilitySummary(3, 1, 0, 2)
    assert stream.resolve() is stream.summary
    assert [row["unit"] for batch in stream for row in batch.to_pylist()] == ["a", "c"]

    resolved = []

    def resolve_once():
        resolved.append(True)
        return pa.table({name: pa.array([value], pa.int64()) for name, value in clean.items()})

    stream = GatedStream(_batches(schema, []), run, ["unit"], resolve_once)
    assert stream.resolve() == EligibilitySummary(3, 1, 0, 2)
    assert list(stream) == []
    assert resolved == [True]
