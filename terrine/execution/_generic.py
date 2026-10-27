"""Explicitly admitted read-only Ibis execution without a shared snapshot."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.types as ir
import pyarrow as pa

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE
from terrine.backends.registry import GENERIC, backend_name, supported_engine_route
from terrine.errors import TerrineError
from terrine.execution._metadata import invalidate_metadata
from terrine.semantic.models import Catalog, RelationSpec, TableRef
from terrine.semantic.sql import DIALECT_MISMATCH, admit_read_query


def _decimal_witnesses() -> tuple[Any, dict[str, Decimal | int]]:
    """Known answers require exact arithmetic, not merely an accepted type name."""
    fixed = dt.Decimal(38, 9)
    wide = dt.Decimal(38, 0)
    low = cast(ir.DecimalScalar, ibis.literal(Decimal("9007199254740993.000000001"), type=fixed))
    whole = cast(ir.DecimalScalar, ibis.literal(Decimal("9007199254740993"), type=fixed))
    edge = cast(
        ir.DecimalScalar,
        ibis.literal(Decimal("99999999999999999999999999999.999999999"), type=fixed),
    )
    tick = cast(ir.DecimalScalar, ibis.literal(Decimal("0.000000001"), type=fixed))
    seven = cast(ir.DecimalScalar, ibis.literal(Decimal(-7), type=wide))
    three = cast(ir.DecimalScalar, ibis.literal(Decimal(3), type=wide))
    maximum = ibis.literal(9223372036854775807, type=wide)
    minimum = ibis.literal(-9223372036854775808, type=wide)
    expressions = {
        "low_digits": low,
        "cancellation": low - whole,
        "addition": (whole + tick) - whole,
        "positive_edge": edge,
        "negative_edge": -edge,
        "edge_subtraction": edge - (edge - tick),
        "negative_quotient": ((seven - seven % three) / three).cast(wide),
        "positive_quotient": ((seven - seven % -three) / -three).cast(wide),
        "int64_max": maximum.cast("int64"),
        "int64_min": minimum.cast("int64"),
        "wide_edge": ibis.literal(Decimal("9" * 38), type=wide),
    }
    answers = {
        "low_digits": Decimal("9007199254740993.000000001"),
        "cancellation": Decimal("0.000000001"),
        "addition": Decimal("0.000000001"),
        "positive_edge": Decimal("99999999999999999999999999999.999999999"),
        "negative_edge": Decimal("-99999999999999999999999999999.999999999"),
        "edge_subtraction": Decimal("0.000000001"),
        "negative_quotient": -2,
        "positive_quotient": 2,
        "int64_max": 9223372036854775807,
        "int64_min": -9223372036854775808,
        "wide_edge": Decimal("9" * 38),
    }
    return ibis.literal(0).as_table().select(**expressions), answers


def _has_decimal(dtype: dt.DataType) -> bool:
    if isinstance(dtype, dt.Array):
        return _has_decimal(dtype.value_type)
    if isinstance(dtype, dt.Map):
        return _has_decimal(dtype.key_type) or _has_decimal(dtype.value_type)
    if isinstance(dtype, dt.Struct):
        return any(_has_decimal(item) for item in dtype.values())
    return dtype.is_decimal()


class _Reader:
    """Release the borrowed statement's reader even when consumption stops early."""

    def __init__(
        self, session: NoSnapshotSession, source: Any, iterator: Any, schema: pa.Schema
    ) -> None:
        self._session = session
        self._source: Any = source
        self._iterator: Any = iterator
        self.schema = schema
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self) -> pa.RecordBatch:
        if self._closed:
            raise StopIteration
        try:
            return self._session._call_backend("query", lambda: next(self._iterator))
        except BaseException as exc:
            try:
                self.close()
            except Exception as cleanup:
                exc.add_note(f"Reader cleanup also failed: {cleanup}")
            raise

    def read_next_batch(self) -> pa.RecordBatch:
        return next(self)

    def read_all(self) -> pa.Table:
        return pa.Table.from_batches(self, schema=self.schema)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._session._readers.discard(self)
        failure: BaseException | None = None
        try:
            try:
                self._session._call_backend("query", self._source.close)
            except BaseException as exc:
                failure = exc
            try:
                if self._iterator is not self._source:
                    close_iterator = getattr(self._iterator, "close", None)
                    if close_iterator is not None:
                        self._session._call_backend("query", close_iterator)
            except BaseException as exc:
                if failure is None:
                    failure = exc
                else:
                    failure.add_note(f"Reader cleanup also failed: {exc}")
        finally:
            self._source = None
            self._iterator = None
        if failure is not None:
            raise failure

    def __enter__(self):
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class NoSnapshotSession:
    """Borrow a caller connection; each statement has its own source view.

    Ibis schema inspection may end an open caller transaction. Use a connection
    with no pending writes or a dedicated read connection.
    """

    def __init__(self, con: Any) -> None:
        self.con = con
        self.backend = backend_name(con)
        self._decimal_admitted = False
        self._readers: set[_Reader] = set()

    def _unavailable(self, operation: str) -> TerrineError:
        return _CAPABILITY_UNAVAILABLE.error(
            backend=self.backend,
            operation=operation,
            status="unverified",
            route=supported_engine_route(operation),
        )

    def _call_backend(self, operation: str, action: Callable[[], Any]) -> Any:
        try:
            return action()
        except (TerrineError, StopIteration):
            raise
        except Exception as exc:
            raise self._unavailable(operation) from exc
        finally:
            invalidate_metadata()

    def admit(self, query: Any) -> str:
        # Inspect owned expression metadata outside the backend exception boundary.
        needs_decimal = any(
            _has_decimal(dtype) for dtype in query.as_table().schema().types
        ) or any(_has_decimal(value.dtype) for value in query.op().find(ops.Value))
        if needs_decimal and not self._decimal_admitted:
            witness, answers = _decimal_witnesses()
            self._call_backend("exact_state", lambda: self.con.compile(witness))
            result = self._call_backend("exact_state", lambda: self.con.to_pyarrow(witness))
            if result.to_pylist() != [answers]:
                raise self._unavailable("exact_state")
            self._decimal_admitted = True
        return self._call_backend(
            "exact_state" if needs_decimal else "query", lambda: self.con.compile(query)
        )

    def check_query(self, sql: str) -> None:
        dialect = PROVIDER.dialect(self.con)
        admit_read_query(sql, dialect, dialect)

    def execute(self, con: Any, query: Any, sql: str) -> pa.Table:
        relation = query.as_table()
        return self._call_backend("query", lambda: con.to_pyarrow(relation))

    def batches(self, con: Any, query: Any, sql: str, *, chunk_size: int) -> _Reader:
        relation = query.as_table()
        source = self._call_backend(
            "query", lambda: con.to_pyarrow_batches(relation, chunk_size=chunk_size)
        )
        try:
            iterator = self._call_backend("query", lambda: iter(source))
            schema = self._call_backend("query", lambda: source.schema)
        except BaseException as exc:
            try:
                self._call_backend("query", source.close)
            except BaseException as cleanup:
                exc.add_note(f"Reader cleanup also failed: {cleanup}")
            raise
        reader = _Reader(self, source, iterator, schema)
        self._readers.add(reader)
        return reader

    def close(self) -> None:
        failure: BaseException | None = None
        for reader in tuple(self._readers):
            try:
                reader.close()
            except BaseException as exc:
                if failure is None:
                    failure = exc
                else:
                    failure.add_note(f"Reader cleanup also failed: {exc}")
        invalidate_metadata()
        if failure is not None:
            raise failure


class _GenericProvider:
    capabilities = GENERIC.capabilities

    def matches(self, con: Any) -> bool:
        try:
            GENERIC.identify(con)
        except TerrineError:
            return False
        return True

    def dialect(self, con: Any) -> str:
        dialect = con.dialect
        return dialect.casefold() if isinstance(dialect, str) else dialect.__name__.casefold()

    def resolve_relation(self, con: Any, relation: RelationSpec) -> Any:
        session = NoSnapshotSession(con)
        if isinstance(relation, TableRef):
            return session._call_backend("query", lambda: con.table(relation.name))
        dialect = self.dialect(con)
        admit_read_query(relation.sql, dialect, dialect)
        return session._call_backend("query", lambda: con.sql(relation.sql))

    def required_relations(
        self, catalog: Catalog, con: Any, *, units: Mapping[str, Any] | None = None
    ) -> tuple[Any, ...]:
        from terrine.execution.inputs import PhysicalRelationId

        roots = [source.relation for source in catalog.sources.values()]
        roots.extend(dimension.relation for dimension in catalog.dimensions.values())
        roots.extend(TableRef(kind="table", name=unit.table) for unit in (units or {}).values())
        return tuple(
            dict.fromkeys(
                PhysicalRelationId("table", None, None, root.name)
                if isinstance(root, TableRef)
                else PhysicalRelationId(
                    "sql_result", None, None, hashlib.sha256(root.sql.encode()).hexdigest()
                )
                for root in roots
            )
        )

    def bind_resolver(
        self,
        catalog: Catalog,
        con: Any,
        *,
        units: Mapping[str, Any] | None = None,
        input_binding: Any = None,
    ) -> Callable[[RelationSpec], Any]:
        if input_binding is not None:
            self.capabilities.require(
                "retained_snapshot",
                backend=backend_name(con),
                route="Materialize directly without retained inputs.",
            )
        dialect = self.dialect(con)
        if catalog.dialect.casefold() != dialect:
            raise DIALECT_MISMATCH.error(declared=catalog.dialect, execution=dialect)
        return lambda relation: self.resolve_relation(con, relation)

    @contextmanager
    def open_execution(
        self,
        catalog: Catalog,
        con: Any,
        *,
        units: Mapping[str, Any] | None = None,
        input_binding: Any = None,
    ) -> Generator[Any]:
        from terrine.execution.inputs import ExecutionContext, _execution_snapshot

        resolver = self.bind_resolver(catalog, con, units=units, input_binding=input_binding)
        session = NoSnapshotSession(con)
        context = ExecutionContext(
            con,
            _execution_snapshot(catalog, units, backend=session.backend),
            resolver,
            _session=session,
            owner_con=con,
            _preflight=session.admit,
        )
        try:
            yield context
        except BaseException as exc:
            try:
                context.close()
            except BaseException as cleanup:
                exc.add_note(f"Reader cleanup also failed: {cleanup}")
            raise
        else:
            context.close()


PROVIDER = _GenericProvider()
