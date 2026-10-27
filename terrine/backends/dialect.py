"""The SQL dialect a study expression compiles for."""

from __future__ import annotations

import math
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import cache
from typing import Any, ClassVar, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
from ibis.common.exceptions import IbisError

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE
from terrine.backends.registry import get_engine, supported_engine_route
from terrine.errors import TerrineError

__all__ = ["Dialect", "current_dialect", "dialect_for_name", "dialect_scope", "unavailable"]

_DIALECT: ContextVar[str | None] = ContextVar("terrine_compile_dialect", default=None)


def _non_null(values: tuple[Any, ...]) -> tuple[Any, ...]:
    """Replace each NULL with another argument; NULL only when all are NULL.

    A substitute is already one of the arguments, so the extremum is unchanged.
    """
    expressions = [
        value if isinstance(value, ibis.Value) else ibis.literal(value) for value in values
    ]
    return tuple(
        ibis.coalesce(value, *(other for other in expressions if other is not value))
        for value in expressions
    )


@dataclass(frozen=True, slots=True)
class Dialect:
    """Immutable expression policy; unknown engines retain coded refusals."""

    name: str
    operations: ClassVar[frozenset[str]] = frozenset()
    has_scaled_float_state: ClassVar[bool] = False
    has_native_asof: ClassVar[bool] = False

    def exact_type(self) -> dt.DataType:
        raise unavailable("exact_state", self.name)

    def decimal_coefficient(self, value: ir.Value, scale: int) -> ir.Value:
        text = value.cast("string")
        return (text.replace(".", "") if scale else text).cast(self.exact_type())

    def coefficient_bounds(self, value: ir.Value) -> tuple[int, int]:
        dtype = value.type()
        if isinstance(dtype, (dt.SignedInteger, dt.UnsignedInteger)):
            lower, upper = dtype.bounds
            return lower, upper
        if not isinstance(dtype, dt.Decimal) or dtype.precision is None:
            raise TypeError(f"{dtype} is not an exact type with a known precision")
        maximum = 10**dtype.precision - 1
        return -maximum, maximum

    def finite_text(self, value: ir.Value) -> tuple[ir.FloatingValue, ir.StringValue]:
        widened = cast(ir.FloatingValue, value.cast(dt.float64))
        finite = self.float_is_finite(widened).ifelse(widened, ibis.null(dt.float64))
        return widened, finite.cast("string")

    def binary64(self, text: ir.StringValue) -> ir.FloatingValue:
        return cast(ir.FloatingValue, text.cast(dt.float64))

    def exact_float(
        self, value: ir.Value, scale: int | ir.IntegerValue | None = None
    ) -> tuple[ir.Value, ir.BooleanValue, ir.BooleanValue]:
        raise unavailable("exact_float", self.name)

    def exact_ratio(self, numerator: ir.Value, denominator: ir.Value) -> ir.Value:
        raise unavailable("exact_ratio", self.name)

    def exact_is_nonfinite(self, value: ir.Value) -> ir.BooleanValue:
        raise unavailable("exact_is_nonfinite", self.name)

    def exact_to_float(
        self, value: ir.Value, scale: int | ir.IntegerValue = 0
    ) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
        if not isinstance(scale, int) or scale:
            raise ValueError(f"{self.name} exact state is unscaled; scale must be 0")
        raise unavailable("exact_is_nonfinite", self.name)

    def wide_type(self) -> dt.DataType:
        raise unavailable("normalized", self.name)

    def wide_abs(self, value: ir.Value) -> ir.Value:
        return cast(ir.NumericValue, value).abs()

    def wide_quotient(self, value: ir.Value, divisor: ir.Value) -> ir.Value:
        raise unavailable("normalized", self.name)

    def trim_leading_zeros(self, digits: ir.StringValue) -> ir.StringValue:
        raise unavailable("normalized", self.name)

    def exact_sum(self, value: ir.Value) -> ir.Value:
        raise unavailable("exact_state", self.name)

    def exact_subtract(self, left: ir.Value, right: ir.Value) -> ir.Value:
        raise unavailable("exact_state", self.name)

    def truncating_quotient(
        self, value: ir.IntegerValue, divisor: int | ir.IntegerValue
    ) -> ir.IntegerValue:
        raise unavailable("integer_quotient", self.name)

    def float_is_finite(self, value: ir.Value) -> ir.BooleanValue:
        # PostgreSQL has no isfinite(double precision), and NaN equals NaN there.
        nonfinite = [
            ibis.literal(item, type=dt.float64) for item in (math.inf, -math.inf, math.nan)
        ]
        return cast(ir.FloatingValue, value.cast(dt.float64)).notin(nonfinite)

    def timestamp_is_finite(self, value: ir.TimestampValue) -> ir.BooleanValue:
        raise unavailable("timestamp_is_finite", self.name)

    def timestamp_extremes(
        self, dtype: dt.Timestamp
    ) -> tuple[ir.TimestampScalar, ir.TimestampScalar]:
        return (
            cast(ir.TimestampScalar, ibis.literal("-infinity").cast(dtype)),
            cast(ir.TimestampScalar, ibis.literal("infinity").cast(dtype)),
        )

    def utc_wall_clock(self, value: ir.Value) -> ir.TimestampValue:
        raise unavailable("utc_wall_clock", self.name)

    def utc_instant(self, value: ir.Value) -> ir.TimestampValue:
        raise unavailable("utc_instant", self.name)

    def timestamp_text(self, value: ir.TimestampValue) -> ir.StringValue:
        raise unavailable("timestamp_text", self.name)

    def timestamp_parts(
        self, value: ir.TimestampValue
    ) -> tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]:
        from terrine.backends.timestamps import _UNSUPPORTED

        dtype = value.type()
        if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6, 9):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        raise unavailable("timestamp_is_finite", self.name)

    def timestamp_order_key(self, value: ir.TimestampValue) -> ir.Value:
        from terrine.backends.timestamps import _UNSUPPORTED

        dtype = value.type()
        if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6, 9):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        raise unavailable("timestamp_is_finite", self.name)

    def reverse_time_key(self, value: ir.TimestampValue) -> ir.Value:
        # One exact scalar: finite instants' nanoseconds stay far below 10**38.
        rank, seconds, nanos = self.timestamp_parts(value)
        wide = dt.Decimal(38, 0)
        billion = cast(ir.DecimalValue, ibis.literal(10**9, type=wide))
        rank_weight = cast(ir.DecimalValue, ibis.literal(10**23, type=wide))
        coordinate = cast(ir.DecimalValue, seconds.cast(wide)) * billion + cast(
            ir.DecimalValue, nanos.cast(wide)
        )
        return cast(ir.DecimalValue, ibis.literal(0, type=wide)) - (
            cast(ir.DecimalValue, rank.cast(wide)) * rank_weight + coordinate
        )

    def bounded_nanoseconds(self, scale: int | None) -> bool:
        return False

    def narrow_exact(
        self, coefficient: ir.Value, scale: int, target: str | dt.DataType
    ) -> ir.Value:
        from terrine.backends.numeric import _scaled_text

        return _scaled_text(coefficient, scale).try_cast(target)

    def asof_join(
        self,
        left: ibis.Table,
        right: ibis.Table,
        *,
        left_unit: str,
        left_key: str,
        right_unit: str,
        right_key: str,
        inclusive: bool,
    ) -> ibis.Table:
        used = {name.casefold() for name in (*left.columns, *right.columns)}

        def fresh(stem: str) -> str:
            name = stem
            while name.casefold() in used:
                name = f"_{name}"
            used.add(name.casefold())
            return name

        unit, key, kind, match = (
            fresh(f"__terrine_asof_{part}") for part in ("unit", "key", "kind", "match")
        )
        # At an equal key, the right row sorts first when it is admitted.
        left_kind, right_kind = (1, 0) if inclusive else (0, 1)
        requests = left.select(
            **{unit: left[left_unit], key: left[left_key], kind: ibis.literal(left_kind)}
        ).distinct()
        states = right.select(
            **{unit: right[right_unit], key: right[right_key], kind: ibis.literal(right_kind)}
        )
        timeline = requests.union(states, distinct=False)
        frame = ibis.window(
            group_by=timeline[unit], order_by=[timeline[key], timeline[kind]], rows=(None, 0)
        )
        admitted = cast(ir.Column, ibis.ifelse(timeline[kind] == right_kind, timeline[key], None))
        timeline = timeline.mutate(**{match: admitted.max().over(frame)})
        mapping = timeline.filter(timeline[kind] == left_kind).select(unit, key, match)
        mapped = left.left_join(
            mapping, [left[left_unit] == mapping[unit], left[left_key] == mapping[key]]
        )
        mapped = mapped.select(*left.columns, match)
        joined = mapped.left_join(
            right, [mapped[left_unit] == right[right_unit], mapped[match] == right[right_key]]
        )
        return joined.select(*left.columns, *right.columns)

    def least(self, *values: Any) -> Any:
        return ibis.least(*_non_null(values))

    def greatest(self, *values: Any) -> Any:
        return ibis.greatest(*_non_null(values))

    def integer_sequence(self, stop: int, name: str) -> ibis.Table:
        return ibis.range(0, stop).unnest().cast("int64").name(name).as_table()

    def supports_exact_prefix(self, backend_name: str) -> bool:
        """Whether this expression policy admits the backend's exact prefix state."""
        return False


@cache
def _registered_dialect(name: str) -> Dialect:
    engine = get_engine(name)
    assert engine is not None
    return engine.dialect(name)


def dialect_for_name(name: str) -> Dialect:
    """Select expression policy without querying or admitting a connection."""
    name = name.casefold()
    if get_engine(name) is None:
        return Dialect(name)
    return _registered_dialect(name)


@contextmanager
def dialect_scope(dialect: str) -> Generator[None]:
    """Compile every expression inside the block, bound or literal, for ``dialect``."""
    token = _DIALECT.set(dialect.casefold())
    try:
        yield
    finally:
        _DIALECT.reset(token)


def current_dialect(*values: ir.Value) -> Dialect:
    """The enclosing scope's dialect, else the backend of the first bound operand.

    A compile scope is set from the Model's admitted expression policy, so
    operands need no graph traversal there. Ibis cannot infer a backend from
    unbound literals; outside any scope those
    default to DuckDB.
    """
    scoped = _DIALECT.get()
    if scoped is not None:
        return dialect_for_name(scoped)
    for value in values:
        try:
            return dialect_for_name(value._find_backend(use_default=False).name)
        except IbisError:
            continue
    return dialect_for_name("duckdb")


def unavailable(operation: str, dialect: str) -> TerrineError:
    return _CAPABILITY_UNAVAILABLE.error(
        backend=dialect,
        operation=operation,
        status="unsupported",
        route=supported_engine_route(operation),
    )
