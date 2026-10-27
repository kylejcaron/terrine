from __future__ import annotations

import asyncio
from decimal import Decimal

import ibis
import pyarrow as pa
import pytest

from terrine.errors import TerrineError
from tests.backends.test_generic import Unregistered
from tests.execution.test_inputs import _catalog


class Reader:
    schema = pa.schema([("value", pa.int64())])

    def __init__(self, failure=None):
        self.failure = failure
        self.closed = False
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index == 0:
            self.index += 1
            return pa.record_batch([[1]], schema=self.schema)
        if self.failure is not None:
            raise self.failure
        raise StopIteration

    def close(self):
        self.closed = True


class FailingBackend(Unregistered):
    def __init__(self, phase=None, failure: BaseException | None = None):
        self.phase = phase
        self.failure = failure
        self.reader = Reader(failure if phase == "iteration" else None)
        self.decimal_reads = 0
        self.witness_answers: dict[str, Decimal | int] = {}

    def compile(self, query, **kwargs):
        if self.phase == "compile":
            assert self.failure is not None
            raise self.failure
        return "SELECT 1 AS value"

    def to_pyarrow(self, query, **kwargs):
        if self.phase == "execute":
            assert self.failure is not None
            raise self.failure
        if any(dtype.is_decimal() for dtype in query.as_table().schema().types):
            self.decimal_reads += 1
            return pa.Table.from_pylist(
                [{name: value for name, value in self.witness_answers.items()}]
            )
        return pa.table({"value": [1]})

    def to_pyarrow_batches(self, query, **kwargs):
        if self.phase == "reader":
            assert self.failure is not None
            raise self.failure
        return self.reader


def context_for(con):
    from terrine.execution._generic import PROVIDER

    return PROVIDER.open_execution(_catalog().model_copy(update={"dialect": "sqlite"}), con)


@pytest.mark.parametrize("phase", ["compile", "execute", "reader", "iteration"])
def test_backend_failures_are_coded_at_the_read_boundary(phase):
    failure = RuntimeError("unsupported primitive")
    con = FailingBackend(phase, failure)
    with context_for(con) as context:
        with pytest.raises(TerrineError) as caught:
            if phase in {"reader", "iteration"}:
                list(context.to_arrow_batches(ibis.literal(1).name("value")))
            else:
                context.execute(ibis.literal(1).name("value"))
        assert caught.value.code == "terrine.backend.capability_unavailable"
        assert {k: v for k, v in caught.value.context.items() if k != "route"} == {
            "backend": "unregistered",
            "operation": "query",
            "status": "unverified",
        }
        assert caught.value.__cause__ is failure
    if phase == "iteration":
        assert con.reader.closed


@pytest.mark.parametrize(
    "failure",
    [
        TerrineError("numeric", code="terrine.test.numeric", context={}),
        KeyboardInterrupt(),
        SystemExit(),
        asyncio.CancelledError(),
    ],
)
@pytest.mark.parametrize("phase", ["compile", "execute", "iteration"])
def test_owned_refusals_and_process_control_are_preserved(failure, phase):
    con = FailingBackend(phase, failure)
    with context_for(con) as context, pytest.raises(type(failure)) as caught:
        if phase == "iteration":
            list(context.to_arrow_batches(ibis.literal(1).name("value")))
        else:
            context.execute(ibis.literal(1).name("value"))
    assert caught.value is failure


def test_terrine_preflight_errors_are_not_backend_errors():
    with context_for(FailingBackend()) as context:
        failure = TypeError("invalid owned graph traversal")

        def preflight(query):
            raise failure

        context._preflight = preflight
        with pytest.raises(TypeError) as caught:
            context.execute(ibis.literal(1))
        assert caught.value is failure


@pytest.mark.parametrize("finish", ["early", "exhausted", "context"])
def test_batch_readers_release_only_owned_resources(finish):
    con = FailingBackend()
    with context_for(con) as context:
        reader = context.to_arrow_batches(ibis.literal(1).name("value"))
        assert next(reader).to_pylist() == [{"value": 1}]
        if finish == "early":
            reader.close()
        elif finish == "exhausted":
            assert list(reader) == []
        else:
            context.close()
        assert con.reader.closed
    with pytest.raises(TerrineError) as caught:
        context.execute(ibis.literal(1))
    assert caught.value.code == "terrine.execution.snapshot_unavailable"
    assert con.to_pyarrow(ibis.literal(1)).to_pylist() == [{"value": 1}]


def test_decimal_admission_is_lazy_and_cached_per_session():
    from terrine.execution import _generic

    con = FailingBackend()
    witnesses, answers = _generic._decimal_witnesses()
    con.witness_answers = answers
    with context_for(con) as context:
        context.execute(ibis.literal(1).name("value"))
        assert con.decimal_reads == 0
        context._preflight(witnesses)
        context._preflight(witnesses)
        assert con.decimal_reads == 1
    with context_for(con) as context:
        context._preflight(witnesses)
        assert con.decimal_reads == 2
    assert Decimal("9007199254740993.000000001") in answers.values()
    assert Decimal("0.000000001") in answers.values()
    assert Decimal("99999999999999999999999999999.999999999") in answers.values()
    assert 9223372036854775807 in answers.values()
    assert -9223372036854775808 in answers.values()


def test_frame_admission_witnesses_once_and_only_when_needed():
    """``context.admit`` takes every relation a Frame will run: a decimal relation
    anywhere among them runs the witness once, before any of them executes;
    decimal-free relations run none; per-statement preflight then finds it cached."""
    from terrine.execution import _generic

    con = FailingBackend()
    _witnesses, answers = _generic._decimal_witnesses()
    con.witness_answers = answers
    plain = ibis.literal(1).name("value").as_table()
    decimal = ibis.literal(Decimal("1.000000001"), type="decimal(38,9)").name("value").as_table()
    with context_for(con) as context:
        context.admit(plain, plain)
        assert con.decimal_reads == 0
        context.admit(plain, decimal, plain)
        assert con.decimal_reads == 1
        # The double counts the user's own decimal read; no second witness runs.
        context.execute(decimal)
        context.admit(decimal)
        assert con.decimal_reads == 2
    con.witness_answers = {"rounded": 0}
    with context_for(con) as context, pytest.raises(TerrineError) as caught:
        context.admit(plain, decimal)
    assert caught.value.context["operation"] == "exact_state"
    with context_for(con) as context:
        context.close()
        with pytest.raises(TerrineError) as closed:
            context.admit(plain)
    assert closed.value.code == "terrine.execution.snapshot_unavailable"


def test_inexact_decimal_witness_never_admits_user_query():
    con = FailingBackend()
    con.witness_answers = {"rounded": 0}
    with context_for(con) as context, pytest.raises(TerrineError) as caught:
        context.execute(ibis.literal(Decimal("1.000000001"), type="decimal(38,9)"))
    assert caught.value.context["operation"] == "exact_state"
    assert caught.value.context["backend"] == "unregistered"


@pytest.mark.parametrize("phase", ["iterator", "schema"])
def test_reader_setup_failure_releases_the_statement(phase):
    failure = RuntimeError("reader setup failed")

    class BrokenReader(Reader):
        def __iter__(self):
            if phase == "iterator":
                raise failure
            return self

        @property
        def schema(self):
            if phase == "schema":
                raise failure
            return Reader.schema

    con = FailingBackend()
    con.reader = BrokenReader()
    with context_for(con) as context, pytest.raises(TerrineError) as caught:
        context.to_arrow_batches(ibis.literal(1))
    assert caught.value.__cause__ is failure
    assert con.reader.closed


def test_close_failure_does_not_leave_other_statement_readers_open():
    failure = RuntimeError("reader release failed")

    class BrokenClose(Reader):
        def close(self):
            super().close()
            raise failure

    con = FailingBackend()
    with context_for(con) as context:
        con.reader = BrokenClose()
        first = context.to_arrow_batches(ibis.literal(1))
        broken = con.reader
        con.reader = BrokenClose()
        second = context.to_arrow_batches(ibis.literal(1))
        with pytest.raises(TerrineError) as caught:
            context.close()
        assert caught.value.__cause__ is failure
        assert broken.closed and con.reader.closed
        first.close()
        second.close()


def test_reader_cleanup_does_not_replace_cancellation():
    con = FailingBackend()
    cancelled = asyncio.CancelledError()

    class BrokenClose(Reader):
        def close(self):
            super().close()
            raise RuntimeError("release failed")

    con.reader = BrokenClose()
    with pytest.raises(asyncio.CancelledError) as caught, context_for(con) as context:
        context.to_arrow_batches(ibis.literal(1))
        raise cancelled
    assert caught.value is cancelled
    assert con.reader.closed


@pytest.mark.parametrize("close_fails", [False, True])
def test_source_close_also_releases_its_distinct_iterator(close_fails):
    failure = RuntimeError("source close failed")

    class StreamingReader(Reader):
        def __init__(self):
            super().__init__()
            self.released = False
            self.iterator = self.stream()

        def stream(self):
            try:
                yield pa.record_batch([[1]], schema=self.schema)
                yield pa.record_batch([[2]], schema=self.schema)
            finally:
                self.released = True

        def __iter__(self):
            return self.iterator

        def close(self):
            super().close()
            if close_fails:
                raise failure

    con = FailingBackend()
    con.reader = StreamingReader()
    with context_for(con) as context:
        reader = context.to_arrow_batches(ibis.literal(1))
        assert next(reader).to_pylist() == [{"value": 1}]
        if close_fails:
            with pytest.raises(TerrineError) as caught:
                reader.close()
            assert caught.value.__cause__ is failure
        else:
            reader.close()
        assert con.reader.released
