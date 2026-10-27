"""The expression policy a study expression compiles for.

The base ``Dialect`` is the portable policy for explicitly admitted,
unverified Ibis backends: every operation is an ordinary Ibis expression.
Float facts enter as fixed-scale ``DECIMAL(38, 9)`` state, quantized once by
text truncation; counts and source-decimal coefficients stay in a separate
``DECIMAL(38, 0)`` domain. Registered engines subclass it with native types.
"""

from __future__ import annotations

import math
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from fractions import Fraction
from functools import cache
from typing import Any, ClassVar, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
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


_WIDE = dt.Decimal(38, 0)
_FIXED = dt.Decimal(38, 9)
_FIXED_SCALE = 9
_INT64_BOUNDS = (-(2**63), 2**63 - 1)
# A literal divisor of this size keeps an int64 quotient below 2**51, where one
# rounded binary64 division of the exact multiple is already the exact quotient.
_DIRECT_DIVISOR = 2**12
# Binary64 unit roundoff; a wide quotient seed is scaled down by 16u so that
# its integer rounding never exceeds the true quotient.
_UNIT_ROUNDOFF = 2.0**-53
_SEED_BIAS = 1 - 16 * _UNIT_ROUNDOFF


def _decimal_limit(dtype: dt.Decimal) -> ir.DecimalValue:
    """The largest magnitude of a fixed-precision decimal, typed as that decimal."""
    assert dtype.precision is not None and dtype.scale is not None
    digits = "9" * (dtype.precision - dtype.scale)
    text = digits + ("." + "9" * dtype.scale if dtype.scale else "")
    return cast(ir.DecimalValue, ibis.literal(text).cast(dtype))


def _typed_null(dtype: dt.DataType) -> ir.Value:
    return ibis.null().cast(dtype)


def _rounded(quotient: ir.FloatingValue) -> ir.IntegerValue:
    """The nearest integer to a binary64 quotient known to lie within 1/2 of one."""
    return cast(ir.IntegerValue, (quotient + 0.5).floor())


def _wide_quotient(dividend: ir.DecimalValue, divisor: ir.DecimalValue) -> ir.DecimalValue:
    """Exact ``trunc(dividend / divisor)`` in ``DECIMAL(38, 0)`` through a binary64 seed.

    Ibis division lowers to binary64, so the quotient is rebuilt from exact
    decimal residuals. With ``m = |dividend| - |dividend| % |divisor|``, an exact
    multiple of ``d = |divisor|`` with ``q = m / d``, the seed ``h`` is the
    integer cast of ``(1 - 16u) · fl(m) / fl(d)``. The binary64 quotient is
    within ``5u`` of ``q`` (two decimal-to-double casts of up to one ulp each
    and one division), so the scaled value lies below ``q`` by at least
    ``10u·q`` and at most ``22u·q``; after a text-based double-to-decimal cast
    (``u``) and integer rounding the seed satisfies ``q - 23u·q - 1 <= h <= q``.
    The residual ``e = m - h·d`` is therefore exact, non-negative and below
    ``(23u·q + 1)·d``; for ``q < 10**19`` its rounded binary64 quotient is the
    exact correction (``5u · 25300 < 1/2``), and no product exceeds ``m``. The
    certificate ``(h + c)·d == m`` is exact; an uncertified quotient is NULL,
    never wrong.
    """
    magnitude = dividend.abs()
    size = divisor.abs()
    multiple = magnitude - magnitude % size
    floating = cast(ir.FloatingValue, multiple.cast(dt.float64))
    width = cast(ir.FloatingValue, size.cast(dt.float64))
    seed = cast(ir.DecimalValue, (floating / width * _SEED_BIAS).cast(_WIDE))
    residual = cast(ir.DecimalValue, multiple - seed * size)
    ratio = cast(ir.FloatingValue, cast(ir.FloatingValue, residual.cast(dt.float64)) / width)
    correction = cast(ir.DecimalValue, _rounded(ratio).cast(_WIDE))
    quotient = cast(ir.DecimalValue, seed + correction)
    zero = ibis.literal(0, type=_WIDE)
    negative = (dividend < zero) != (divisor < zero)
    sign = negative.ifelse(ibis.literal(-1), ibis.literal(1)).cast(_WIDE)
    certified = quotient * size == multiple
    signed = cast(ir.DecimalValue, quotient * cast(ir.DecimalValue, sign))
    return cast(ir.DecimalValue, certified.ifelse(signed, _typed_null(_WIDE)))


@dataclass(frozen=True, slots=True)
class Dialect:
    """Immutable expression policy; the base is the portable unverified policy."""

    name: str
    operations: ClassVar[frozenset[str]] = frozenset(
        {
            "exact_state",
            "exact_float",
            "exact_ratio",
            "exact_is_nonfinite",
            "normalized",
            "integer_quotient",
            "timestamp_is_finite",
            "utc_wall_clock",
            "utc_instant",
            "timestamp_text",
            "asof_join",
            "least",
            "greatest",
            "integer_sequence",
        }
    )
    has_scaled_float_state: ClassVar[bool] = False
    has_native_asof: ClassVar[bool] = False
    # Fixed-precision state carries an accumulated binary64 magnitude bound so
    # that an accumulation leaving the domain refuses; native exact types do not.
    bounds_accumulations: ClassVar[bool] = True

    def exact_type(self) -> dt.DataType:
        """Float state: fixed scale 9, so ``|value| <= (10**38 - 1) / 10**9``."""
        return _FIXED

    def coefficient_type(self) -> dt.DataType:
        """The exact type of counts and source-decimal coefficients.

        The portable policy keeps them in the scale-0 coefficient domain,
        separate from fixed-scale float state; registered engines hold both in
        their native exact state.
        """
        return _WIDE

    def decimal_coefficient(self, value: ir.Value, scale: int) -> ir.Value:
        text = value.cast("string")
        return (text.replace(".", "") if scale else text).cast(self.coefficient_type())

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
        """Quantize a finite float's decimal text toward zero at scale 9.

        The text ``[-]D[.F][e±E]`` has value ``sign · digits · 10**(E - |F|)``,
        so its decimal point sits ``p = len(digits) + E - |F|`` digits into
        ``digits``. Padding the digits with zeros on both sides lets the integer
        part and the first nine fractional digits be cut out directly, which
        truncates toward zero. The integer part has more than 29 digits exactly
        when ``p > 29`` (``D`` carries no leading zeros beyond a lone ``0``), so
        ``|value| >= 10**29`` is unrepresentable; non-finite input is flagged
        separately. Nothing is multiplied in binary64.
        """
        from terrine.backends.numeric import _decimal_parts

        widened, text = self.finite_text(value)
        finite = self.float_is_finite(widened)
        sign, digits, fraction_digits, exponent = _decimal_parts(text)
        lead, trail = 40, 48
        padded = cast(ir.StringValue, ibis.literal("0" * lead)) + digits + "0" * trail
        point = cast(ir.IntegerValue, lead + digits.length() + exponent - fraction_digits)
        # Points left of the padding belong to magnitudes below 10**-40: zero at scale 9.
        position = cast(ir.IntegerValue, ibis.greatest(point, 1).cast(dt.int32))
        fits = point <= ibis.literal(lead + 38 - _FIXED_SCALE)
        scaled = sign + padded.substr(0, position) + "." + padded.substr(position, _FIXED_SCALE)
        exact = fits.ifelse(scaled, ibis.null(dt.string)).cast(_FIXED)
        false = ibis.literal(False)
        nonfinite = (~finite).fill_null(false)
        unrepresentable = (finite & ~fits).fill_null(false)
        return exact, nonfinite, unrepresentable

    def exact_ratio(self, numerator: ir.Value, denominator: ir.Value) -> ir.Value:
        """Divide once in binary64 from correctly rounded operands; NULL for zero.

        Exact operands convert through their decimal text, so the quotient is
        within ``(1 + u)**3 - 1 < 3.1u`` of the true ratio before any later
        rounding; a binary64 operand (a nested quotient) is taken as it is.
        """
        zero = denominator == ibis.literal(0)
        divisor = zero.ifelse(_typed_null(denominator.type()), denominator)
        return self._binary64_operand(numerator) / self._binary64_operand(divisor)

    def _binary64_operand(self, value: ir.Value) -> ir.FloatingValue:
        if value.type().is_floating():
            return cast(ir.FloatingValue, value)
        return self.binary64(value.cast("string"))

    def exact_is_nonfinite(self, value: ir.Value) -> ir.BooleanValue:
        """Decimal state never holds a non-finite value; a binary64 quotient might."""
        if value.type().is_floating():
            nonfinite = ~self.float_is_finite(value)
        else:
            nonfinite = cast(ir.BooleanValue, ibis.literal(False))
        return cast(ir.BooleanValue, value.isnull().ifelse(ibis.null(dt.boolean), nonfinite))

    def exact_to_float(
        self, value: ir.Value, scale: int | ir.IntegerValue = 0
    ) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
        """Round decimal state once to binary64 through its text.

        A fixed-precision decimal lies far inside binary64's range, so it never
        overflows; a binary64 quotient is already rounded and is flagged only
        when it is not finite.
        """
        if not isinstance(scale, int) or scale:
            raise ValueError(f"{self.name} exact state is unscaled; scale must be 0")
        false = cast(ir.BooleanValue, ibis.literal(False))
        if value.type().is_floating():
            floating = cast(ir.FloatingValue, value)
            nonfinite = (~self.float_is_finite(floating)).fill_null(ibis.literal(False))
            rounded = cast(ir.FloatingValue, nonfinite.ifelse(ibis.null(dt.float64), floating))
            return rounded, nonfinite, false
        return self.binary64(value.cast("string")), false, false

    def wide_type(self) -> dt.DataType:
        return _WIDE

    def wide_abs(self, value: ir.Value) -> ir.Value:
        return cast(ir.NumericValue, value).abs()

    def wide_quotient(self, value: ir.Value, divisor: ir.Value) -> ir.Value:
        wide = cast(ir.DecimalValue, value.cast(_WIDE))
        size = cast(ir.DecimalValue, divisor.cast(_WIDE))
        empty = size == ibis.literal(0)
        size = cast(ir.DecimalValue, empty.ifelse(_typed_null(_WIDE), size))
        return _wide_quotient(wide, size)

    def trim_leading_zeros(self, digits: ir.StringValue) -> ir.StringValue:
        return digits.re_replace("^0+", "")

    def exact_sum(self, value: ir.Value, *, admitted: bool = False) -> ir.Value:
        """Sum in the backend, keeping the operand's fixed type; NULL when it cannot fit.

        Ibis widens a scale-0 decimal sum to scale 2; the cast back keeps
        counts and coefficients in their domain. The binary64 sum of the
        operands' magnitudes bounds every partial sum, so a group that
        ``admits_accumulation`` rejects yields NULL instead of an overflowing
        cast. The backend still adds every operand, so callers whose operands
        may leave the type's domain withhold them first
        (``numeric.guard_accumulation``) and pass ``admitted``: the sum is then
        taken as it is, because re-deriving the admission from a differently
        ordered magnitude sum could withhold a sum the guard admitted.
        """
        column = cast(ir.NumericColumn, value)
        total = column.sum()
        dtype = value.type()
        if admitted or not isinstance(dtype, dt.Decimal) or dtype.precision is None:
            return total.cast(dtype)
        bound = cast(ir.FloatingValue, self.magnitude(value).sum())
        fits = self.admits_accumulation(bound, column.count(), dtype)
        return fits.ifelse(total, _typed_null(dtype)).cast(dtype)

    def magnitude(self, value: ir.Value) -> ir.FloatingColumn:
        """A NULL-free binary64 magnitude whose sum bounds any sum of ``value``.

        The conversion goes through the decimal text, which the verified
        backends round correctly (``|m - |x|| <= u·|x|``), unlike a direct
        decimal-to-binary64 cast.
        """
        floating = cast(ir.FloatingColumn, self.binary64(value.cast("string")))
        return cast(ir.FloatingColumn, floating.abs().fill_null(ibis.literal(0.0)))

    @staticmethod
    def magnitude_limit(dtype: dt.Decimal) -> float:
        """The largest binary64 not above the type's largest magnitude."""
        assert dtype.precision is not None and dtype.scale is not None
        exact = Fraction(10**dtype.precision - 1, 10**dtype.scale)
        rounded = float(exact)
        return rounded if Fraction(rounded) <= exact else math.nextafter(rounded, 0.0)

    def admits_accumulation(
        self, magnitude_sum: ir.Value, count: ir.Value, dtype: dt.Decimal
    ) -> ir.BooleanValue:
        """Whether a sum whose backend magnitude bound is ``magnitude_sum`` fits ``dtype``.

        Let ``S = Σ|x_i|`` over the ``n = count`` contributing values and ``L``
        the type's largest magnitude; every partial sum lies within ``S``, so
        ``S < L`` admits the exact sum. The backend computes ``B`` from the
        magnitudes ``m_i = |x_i|(1 + δ_i)``, ``|δ_i| <= u = 2**-53``, through at
        most ``n - 1`` binary64 additions in any order, so each term carries a
        factor within ``1 ± γ_{n-1}``, ``γ_k = k·u / (1 - k·u)``, and
        ``|B - S| <= e·S`` for ``e = (1 + u)(1 + γ_{n-1}) - 1``. The lower
        side, ``S <= B / (1 - e)``, makes ``B < L·(1 - e)`` sufficient;
        ``B·(1 + e) < L`` is not, since ``1/(1 - e) > 1 + e``. In closed form
        ``1 - e = (1 - (2n - 1)u) / (1 - (n - 1)u) >= 1 - 2n·u``.

        The guard compares against ``t = fl(fl(1 - (n + 2)·2u) · L̂)`` with
        ``L̂`` the largest binary64 not above ``L``: ``(n + 2)·2u`` is exact (an
        integer below ``2**53`` scaled by a power of two), the two remaining
        roundings contribute at most ``(1 + u)**2``, and
        ``(1 - (2n + 4)u)(1 + u)**2 < 1 - (2n + 2)u < 1 - 2n·u``, so
        ``t <= L·(1 - e)`` and ``B < t`` implies ``S < L``. From
        ``n >= 2**52 - 2`` the threshold is not positive and nothing is
        admitted; the model needs ``(n - 1)u < 1`` in any case. A NULL bound
        (no contributing rows) is admitted.
        """
        limit = cast(ir.FloatingScalar, ibis.literal(self.magnitude_limit(dtype), type=dt.float64))
        additions = cast(ir.FloatingValue, count.cast(dt.float64)) + 2.0
        slack = cast(ir.FloatingValue, additions * (2 * _UNIT_ROUNDOFF))
        threshold = cast(ir.FloatingValue, (1.0 - slack) * limit)
        bound = cast(ir.FloatingValue, magnitude_sum).fill_null(ibis.literal(0.0))
        return bound < threshold

    def exact_subtract(self, left: ir.Value, right: ir.Value) -> ir.Value:
        """Subtract exactly; a difference leaving the fixed domain is NULL.

        Only opposite signs can overflow, when ``|left| + |right|`` exceeds the
        type's limit; the check itself never overflows.
        """
        dtype = left.type()
        if not isinstance(dtype, dt.Decimal) or dtype.precision is None:
            return cast(ir.NumericValue, left) - cast(ir.NumericValue, right)
        limit = _decimal_limit(dtype)
        minuend, subtrahend = cast(ir.NumericValue, left), cast(ir.NumericValue, right)
        magnitude, other = minuend.abs(), subtrahend.abs()
        zero = ibis.literal(0)
        opposite = (minuend < zero) != (subtrahend < zero)
        overflow = opposite & (magnitude > limit - other)
        guarded = cast(ir.NumericValue, overflow.ifelse(_typed_null(dtype), subtrahend))
        return minuend - guarded

    def truncating_quotient(
        self, value: ir.IntegerValue, divisor: int | ir.IntegerValue
    ) -> ir.IntegerValue:
        """Signed integer division truncating toward zero; NULL beyond int64 or for zero.

        ``value % divisor`` keeps the dividend's sign, so ``value - remainder``
        is the exact multiple ``q·divisor``. For a literal divisor of at least
        ``2**12`` the quotient is below ``2**51``, so one rounded binary64
        division of that multiple (two roundings, ``|error| <= 2u·q < 1/2``)
        is exact; smaller or column divisors take the certified wide path.
        """
        if isinstance(divisor, int):
            if divisor == 0:
                return cast(ir.IntegerValue, _typed_null(dt.int64))
            if abs(divisor) >= _DIRECT_DIVISOR:
                multiple = cast(ir.FloatingValue, (value - value % divisor).cast(dt.float64))
                return _rounded(cast(ir.FloatingValue, multiple / divisor))
            size = cast(ir.DecimalValue, ibis.literal(divisor, type=_WIDE))
        else:
            size = cast(ir.DecimalValue, divisor.cast(_WIDE))
        quotient = self.wide_quotient(value.cast(_WIDE), size)
        fits = quotient.between(*(ibis.literal(bound, type=_WIDE) for bound in _INT64_BOUNDS))
        return cast(ir.IntegerValue, fits.ifelse(quotient, _typed_null(_WIDE)).cast(dt.int64))

    def float_is_finite(self, value: ir.Value) -> ir.BooleanValue:
        # PostgreSQL has no isfinite(double precision), and NaN equals NaN there.
        nonfinite = [
            ibis.literal(item, type=dt.float64) for item in (math.inf, -math.inf, math.nan)
        ]
        return cast(ir.FloatingValue, value.cast(dt.float64)).notin(nonfinite)

    def timestamp_is_finite(self, value: ir.TimestampValue) -> ir.BooleanValue:
        """The portable policy has no infinite instants."""
        return cast(ir.BooleanValue, ibis.literal(True))

    def timestamp_extremes(
        self, dtype: dt.Timestamp
    ) -> tuple[ir.TimestampScalar, ir.TimestampScalar]:
        digits = dtype.scale if dtype.scale is not None else 6
        high = "9999-12-31 23:59:59." + "9" * digits
        return (
            cast(ir.TimestampScalar, ibis.literal("0001-01-01 00:00:00").cast(dtype)),
            cast(ir.TimestampScalar, ibis.literal(high).cast(dtype)),
        )

    def _epoch_offset(
        self, value: ir.TimestampValue
    ) -> tuple[ir.IntegerValue, ir.IntegerValue, int, dt.Timestamp]:
        """``(seconds, remainder, divisor, dtype)`` since the typed UTC epoch.

        The delta against an epoch literal of the value's own type is an instant
        difference, independent of the session zone; a naive value denotes UTC.
        The remainder keeps the dividend's sign, so pre-epoch instants split as
        ``(0, -1)`` rather than ``(-1, 999999)``. A nanosecond delta is lowered
        as a bare ``a - b``, so it is held in an explicit cast node (``Value.cast``
        drops a same-type cast) and stays one operand of the quotient and the
        remainder.
        """
        from terrine.backends.timestamps import _UNSUPPORTED, _epoch

        dtype = value.type()
        if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6, 9):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        unit, divisor = ("nanosecond", 10**9) if dtype.scale == 9 else ("microsecond", 10**6)
        delta = value.delta(_epoch(dtype), unit=unit)
        # Ibis types the node constructor's operand loosely; the expression is an int64.
        atomic = ops.Cast(delta, to=dt.int64)  # ty: ignore[invalid-argument-type]
        coefficient = cast(ir.IntegerValue, atomic.to_expr())
        seconds = self.truncating_quotient(coefficient, divisor)
        remainder = cast(ir.IntegerValue, coefficient % divisor)
        return seconds, remainder, divisor, dtype

    def _rebuild(
        self,
        dtype: dt.Timestamp,
        seconds: ir.IntegerValue,
        remainder: ir.IntegerValue,
        divisor: int,
    ) -> ir.TimestampValue:
        """``epoch + seconds + remainder`` from whole seconds and a sub-second part.

        Whole seconds and the remainder are added as separate intervals so
        backends lowering intervals through binary64 seconds stay exact.
        """
        from terrine.backends.timestamps import _epoch

        unit = "ns" if divisor == 10**9 else "us"
        shifted = _epoch(dtype) + seconds.as_interval("s") + remainder.as_interval(unit)
        return shifted

    def utc_wall_clock(self, value: ir.Value) -> ir.TimestampValue:
        instant = cast(ir.TimestampValue, value)
        seconds, remainder, divisor, dtype = self._epoch_offset(instant)
        naive = dt.Timestamp(scale=dtype.scale, nullable=dtype.nullable)
        return self._rebuild(naive, seconds, remainder, divisor)

    def utc_instant(self, value: ir.Value) -> ir.TimestampValue:
        wall = cast(ir.TimestampValue, value)
        seconds, remainder, divisor, dtype = self._epoch_offset(wall)
        aware = dt.Timestamp(timezone="UTC", scale=dtype.scale, nullable=dtype.nullable)
        return self._rebuild(aware, seconds, remainder, divisor)

    def timestamp_text(self, value: ir.TimestampValue) -> ir.StringValue:
        """Render the UTC wall clock, so text is independent of the session zone."""
        if cast(dt.Timestamp, value.type()).timezone is not None:
            value = self.utc_wall_clock(value)
        return value.cast(dt.string)

    def timestamp_parts(
        self, value: ir.TimestampValue
    ) -> tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]:
        seconds, remainder, divisor, _ = self._epoch_offset(value)
        nanos = cast(ir.IntegerValue, remainder * (10**9 // divisor))
        rank = value.isnull().ifelse(ibis.null(dt.int64), ibis.literal(0, type=dt.int64))
        return cast(ir.IntegerValue, rank), seconds, nanos

    def timestamp_order_key(self, value: ir.TimestampValue) -> ir.Value:
        """Native timestamp order is the instant order on a lossless type."""
        from terrine.backends.timestamps import _UNSUPPORTED

        dtype = value.type()
        if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6, 9):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        return value

    def reverse_time_key(self, value: ir.TimestampValue) -> ir.Value:
        # One exact scalar: finite instants' nanoseconds stay far below 10**38.
        rank, seconds, nanos = self.timestamp_parts(value)
        billion = cast(ir.DecimalValue, ibis.literal(10**9, type=_WIDE))
        rank_weight = cast(ir.DecimalValue, ibis.literal(10**23, type=_WIDE))
        coordinate = cast(ir.DecimalValue, seconds.cast(_WIDE)) * billion + cast(
            ir.DecimalValue, nanos.cast(_WIDE)
        )
        return cast(ir.DecimalValue, ibis.literal(0, type=_WIDE)) - (
            cast(ir.DecimalValue, rank.cast(_WIDE)) * rank_weight + coordinate
        )

    def bounded_nanoseconds(self, scale: int | None) -> bool:
        return False

    def narrow_exact(
        self, coefficient: ir.Value, scale: int, target: str | dt.DataType
    ) -> ir.Value:
        from terrine.backends.numeric import _checked_narrow_exact

        return _checked_narrow_exact(coefficient, scale, target)

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
        """Union/window as-of: a timeline of distinct requests and states per unit.

        The eligible state for each request is the greatest right key at or
        before it in the timeline; that mapping joins back to every original
        left row, so duplicate requests keep their multiplicity and unmatched
        or NULL keys keep one row with NULL right columns.
        """
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
        """Whether this expression policy admits the backend's exact prefix state.

        The portable policy needs no native type or build; a backend lacking
        usable decimals is refused at execution admission.
        """
        return True


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
