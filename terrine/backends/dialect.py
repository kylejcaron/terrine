"""The expression policy a study expression compiles for.

The base ``Dialect`` is the portable policy for explicitly admitted,
unverified Ibis backends: every operation is an ordinary Ibis expression.
Float facts quantize once to ``DECIMAL(38, 9)`` and join the declared-decimal
coefficient domain ``DECIMAL(38, 0)``. Registered engines subclass it with
native exact types.
"""

from __future__ import annotations

import math
import sys
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
_INT64_BOUNDS = (-(2**63), 2**63 - 1)
# A literal divisor of this size keeps an int64 quotient below 2**51, where one
# rounded binary64 division of the exact multiple is already the exact quotient.
_DIRECT_DIVISOR = 2**12
# Binary64 unit roundoff; a wide quotient seed is scaled down by 16u so that
# its integer rounding never exceeds the true quotient.
_UNIT_ROUNDOFF = 2.0**-53
_SEED_BIAS = 1 - 16 * _UNIT_ROUNDOFF
# Binary64 magnitudes are bounded after scaling by 2**-64, so the bound of up to
# 2**53 terms at the largest magnitude stays finite. A magnitude below 2**-900
# counts as zero, so the scaling never underflows (the smallest scaled term is
# the normal 2**-964) and the bound understates the magnitude sum of n terms by
# less than n·2**-900.
_FLOAT_SCALE = 2.0**-64
_FLOAT_TINY = 2.0**-900
# Admitted binary64 sums stay within half the range, so a difference of two fits.
_FLOAT_HALF_RANGE = math.ldexp(sys.float_info.max, -1)
_FLOAT_LIMIT = math.ldexp(_FLOAT_HALF_RANGE, -64)
# A binary64 quotient is evaluated only where it cannot round to zero (below
# 2**-1020, checked when the divisor's magnitude is at least one) or overflow
# (above 2**1020, checked when it is below one); a product likewise against
# 2**-1020 for a factor below one and half the range for a factor of at least one.
_QUOTIENT_RANGE = 2.0**1020


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
            "quantize_float",
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
    has_native_asof: ClassVar[bool] = False
    # Fixed-precision state carries an accumulated binary64 magnitude bound so
    # that an accumulation leaving the domain refuses; native exact types do not.
    bounds_accumulations: ClassVar[bool] = True
    # Binary64 sums, quotients and products are withheld before leaving the
    # range where float arithmetic raises on overflow or underflow instead of
    # yielding an infinity or zero; an IEEE 754 engine publishes the non-finite
    # result for the finite check to refuse.
    bounds_float_arithmetic: ClassVar[bool] = True

    def exact_type(self) -> dt.DataType:
        """The exact coefficient domain shared by decimals and quantized floats."""
        return _WIDE

    def coefficient_type(self) -> dt.DataType:
        """The exact type of counts and fixed-scale coefficients."""
        return _WIDE

    def count_type(self) -> dt.DataType:
        """The exact type of a 0/1 count term and of a row-count literal.

        The portable policy counts in the coefficient domain; an engine whose
        exact sums accumulate native integers may count in one.
        """
        return self.coefficient_type()

    def exact_types(self) -> frozenset[dt.DataType]:
        """Every type an exact-state operand may carry under this policy."""
        return frozenset({self.exact_type(), self.coefficient_type(), self.count_type()})

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

    def binary64(self, text: ir.StringValue) -> ir.FloatingValue:
        return cast(ir.FloatingValue, text.cast(dt.float64))

    def quantize_float(self, value: ir.FloatingValue) -> ir.DecimalValue:
        """Round an exact measure's float once at scale 9 without overflowing a float-scaled cast.

        Cast the integral part separately so a binary64 multiplication by
        10**9 cannot push an admitted boundary value out of the decimal type.
        A scale-0 cast may round up; correct that toward zero before taking
        the floating remainder. Short float conversions can round admitted
        inputs to 10**29, so cap the integral part inside the scale-9 domain.
        """
        integral = cast(ir.DecimalValue, value.cast(_WIDE))
        rounded = self.binary64(integral.cast("string"))
        correction = cast(
            ir.DecimalValue,
            (value < ibis.literal(0.0)).ifelse(ibis.literal(-1), ibis.literal(1)).cast(_WIDE),
        )
        whole = cast(
            ir.DecimalValue,
            (rounded.abs() > value.abs()).ifelse(integral - correction, integral),
        )
        upper = cast(ir.DecimalValue, ibis.literal(str(10**29 - 1)).cast(_WIDE))
        lower = cast(ir.DecimalValue, ibis.literal(str(-(10**29 - 1))).cast(_WIDE))
        whole = cast(
            ir.DecimalValue,
            ibis.cases((whole > upper, upper), (whole < lower, lower), else_=whole),
        )
        remainder = value - self.binary64(whole.cast("string"))
        return cast(ir.DecimalValue, whole.cast(_FIXED) + remainder.cast(_FIXED))

    def exact_to_float(
        self, value: ir.Value, scale: int | ir.IntegerValue = 0
    ) -> tuple[ir.FloatingValue, ir.BooleanValue, ir.BooleanValue]:
        from terrine.backends.numeric import _scaled_exact_to_float

        if value.type().is_floating():
            floating = cast(ir.FloatingValue, value)
            nonfinite = (~self.float_is_finite(floating)).fill_null(ibis.literal(False))
            rounded = cast(ir.FloatingValue, nonfinite.ifelse(ibis.null(dt.float64), floating))
            return rounded, nonfinite, cast(ir.BooleanValue, ibis.literal(False))
        return _scaled_exact_to_float(value, scale, self)

    def wide_type(self) -> dt.DataType:
        return _WIDE

    def wide_quotient(self, value: ir.Value, divisor: ir.Value) -> ir.Value:
        wide = cast(ir.DecimalValue, value.cast(_WIDE))
        size = cast(ir.DecimalValue, divisor.cast(_WIDE))
        empty = size == ibis.literal(0)
        size = cast(ir.DecimalValue, empty.ifelse(_typed_null(_WIDE), size))
        return _wide_quotient(wide, size)

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
        fits = self.admits_accumulation(bound, column.count(), self.magnitude_limit(dtype))
        return fits.ifelse(total, _typed_null(dtype)).cast(dtype)

    def magnitude(self, value: ir.Value) -> ir.FloatingColumn:
        """A NULL-free binary64 magnitude whose sum bounds any sum of ``value``.

        The conversion goes through the decimal text, which the verified
        backends round correctly (``|m - |x|| <= u·|x|``), unlike a direct
        decimal-to-binary64 cast.
        """
        floating = cast(ir.FloatingColumn, self.binary64(value.cast("string")))
        return cast(ir.FloatingColumn, floating.abs().fill_null(ibis.literal(0.0)))

    def float_magnitude(self, value: ir.Value) -> ir.FloatingColumn:
        """A NULL-free scaled magnitude whose sum bounds any binary64 sum of ``value``.

        ``m = |x|·2**-64`` is exact for ``|x| >= 2**-900``, a power-of-two
        scaling with a normal result, and smaller magnitudes count as zero, so
        on an engine that raises on underflow the product is never evaluated
        where it would round to zero. The sum of ``n`` scaled magnitudes is
        below ``n·2**-64·MAX`` and cannot overflow; against the true magnitude
        sum it understates by less than ``n·2**-900``.
        """
        floating = cast(ir.FloatingColumn, value).abs()
        scaled = (floating < _FLOAT_TINY).ifelse(ibis.literal(0.0), floating * _FLOAT_SCALE)
        return cast(ir.FloatingColumn, scaled.fill_null(ibis.literal(0.0)))

    @staticmethod
    def magnitude_limit(dtype: dt.Decimal) -> float:
        """The largest binary64 not above the type's largest magnitude."""
        assert dtype.precision is not None and dtype.scale is not None
        exact = Fraction(10**dtype.precision - 1, 10**dtype.scale)
        rounded = float(exact)
        return rounded if Fraction(rounded) <= exact else math.nextafter(rounded, 0.0)

    @staticmethod
    def float_magnitude_limit() -> float:
        """Half the binary64 range in ``float_magnitude`` scale, ``(MAX / 2)·2**-64``, exactly.

        A sum whose scaled magnitude bound ``admits_accumulation`` admits at
        this limit has every computed binary64 partial sum within ``MAX / 2``,
        so the difference of two such sums stays within the range.
        """
        return _FLOAT_LIMIT

    def admits_accumulation(
        self, magnitude_sum: ir.Value, count: ir.Value, limit: float
    ) -> ir.BooleanValue:
        """Whether a sum whose backend magnitude bound is ``magnitude_sum`` stays below ``limit``.

        Let ``S = Σ|x_i|`` over the ``n = count`` contributing values and ``L``
        the domain's largest magnitude; every partial sum lies within ``S``, so
        ``S < L`` admits the exact sum. The backend computes ``B`` from the
        magnitudes ``m_i = |x_i|(1 + δ_i)``, ``|δ_i| <= u = 2**-53``, through at
        most ``n - 1`` binary64 additions in any order, so each term carries a
        factor within ``1 ± γ_{n-1}``, ``γ_k = k·u / (1 - k·u)``, and
        ``|B - S| <= e·S`` for ``e = (1 + u)(1 + γ_{n-1}) - 1``. The lower
        side, ``S <= B / (1 - e)``, makes ``B < L·(1 - e)`` sufficient;
        ``B·(1 + e) < L`` is not, since ``1/(1 - e) > 1 + e``. In closed form
        ``1 - e = (1 - (2n - 1)u) / (1 - (n - 1)u) >= 1 - 2n·u``.

        The guard compares against ``t = fl(fl(1 - (n + 2)·2u) · L̂)`` with
        ``L̂ = limit`` the largest binary64 not above ``L``: ``(n + 2)·2u`` is
        exact (an integer below ``2**53`` scaled by a power of two), the two
        remaining roundings contribute at most ``(1 + u)**2``, and
        ``(1 - (2n + 4)u)(1 + u)**2 < 1 - (2n + 2)u < 1 - 2n·u``, so
        ``t <= L·(1 - e)`` and ``B < t`` implies ``S < L``. From
        ``n >= 2**52 - 2`` the threshold is not positive and nothing is
        admitted; the model needs ``(n - 1)u < 1`` in any case. A NULL bound
        (no contributing rows) is admitted.

        For a binary64 sum (``float_magnitude`` bounds, ``float_magnitude_limit``)
        the admitted values' own sum rounds too: a computed partial sum ``V``
        of ``n`` terms satisfies ``|V| <= S·(1 + γ_{n-1}) = S / (1 - (n - 1)u)``.
        The scaled magnitudes are exact, so ``|B - S| <= γ_{n-1}·S`` and
        ``S <= B·(1 - (n - 1)u) / (1 - 2(n - 1)u)``; together
        ``|V| <= B / (1 - (2n - 2)u) < L̂·(1 - (2n + 2)u) / (1 - (2n - 2)u) < L̂``
        (in scale), so every partial sum of admitted terms, in any order, stays
        within the limit. The magnitudes below ``2**-900`` the bound omits add
        less than ``n·2**-899`` to ``|V|`` after their own rounding, far inside
        the ``4u·L`` the strict inequality leaves.
        """
        limit_literal = cast(ir.FloatingScalar, ibis.literal(limit, type=dt.float64))
        additions = cast(ir.FloatingValue, count.cast(dt.float64)) + 2.0
        slack = cast(ir.FloatingValue, additions * (2 * _UNIT_ROUNDOFF))
        threshold = cast(ir.FloatingValue, (1.0 - slack) * limit_literal)
        bound = cast(ir.FloatingValue, magnitude_sum).fill_null(ibis.literal(0.0))
        return bound < threshold

    def float_quotient(
        self, numerator: ir.FloatingValue, denominator: ir.FloatingValue
    ) -> ir.FloatingValue:
        """``numerator / denominator`` in binary64; NULL where the quotient may leave the range.

        The division is evaluated only when the quotient is known to lie
        within ``2**±1020``, decided without any product leaving the range.
        For ``|d| >= 1``, ``|n| >= |d|·2**-1020`` suffices: the product is
        normal and at most ``MAX·2**-1020``, so it is within ``u`` of exact and
        ``|n / d| >= 2**-1020·(1 - u)`` cannot round to zero, while
        ``|n / d| <= |n|`` cannot overflow. For ``0 < |d| < 1``,
        ``|n| <= |d|·2**1020`` suffices: the product lies in
        ``[2**-54, 2**1020)``, so ``|n / d| <= 2**1020·(1 + u)`` cannot
        overflow, while ``|n / d| > |n|`` cannot round to zero. A zero
        numerator divides to zero; a zero or NULL divisor yields NULL.
        """
        n, d = numerator.abs(), denominator.abs()
        one = ibis.literal(1.0, type=dt.float64)
        zero = ibis.literal(0.0, type=dt.float64)
        within = cast(
            ir.BooleanValue,
            (d >= one).ifelse(n >= d * (1.0 / _QUOTIENT_RANGE), n <= d * _QUOTIENT_RANGE),
        )
        safe = (d != zero) & ((numerator == zero) | within)
        return cast(ir.FloatingValue, safe.ifelse(numerator / denominator, ibis.null(dt.float64)))

    def float_product(self, left: ir.FloatingValue, right: ir.FloatingValue) -> ir.FloatingValue:
        """``left * right`` in binary64; NULL where the product may leave the range.

        The product is evaluated only when it is known to lie between
        ``2**-1020`` and half the range, decided without any intermediate
        leaving the range. For ``|right| >= 1`` the product cannot round to zero
        (``|left * right| >= |left|``) and ``|left| <= (MAX / 2) / |right|``
        suffices against overflow: the quotient is normal, within ``u`` of
        exact, so ``|left * right| <= (MAX / 2)(1 + u) < MAX``. For
        ``0 < |right| < 1`` the product cannot overflow (``|left * right| <
        |left|``) and ``|left| >= 2**-1020 / |right|`` suffices against zero:
        that quotient lies in ``(2**-1020, 2**54]``, so
        ``|left * right| >= 2**-1020 (1 - u)``. A zero operand multiplies to
        zero; a NULL operand yields NULL.
        """
        a, b = left.abs(), right.abs()
        one = ibis.literal(1.0, type=dt.float64)
        zero = ibis.literal(0.0, type=dt.float64)
        within = cast(
            ir.BooleanValue,
            (b >= one).ifelse(a <= _FLOAT_HALF_RANGE / b, a >= (1.0 / _QUOTIENT_RANGE) / b),
        )
        safe = (left == zero) | (right == zero) | within
        return cast(ir.FloatingValue, safe.ifelse(left * right, ibis.null(dt.float64)))

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

    def _epoch_coefficient(
        self, value: ir.TimestampValue
    ) -> tuple[ir.IntegerValue, int, dt.Timestamp]:
        """``(coefficient, divisor, dtype)``: the instant's offset from the typed UTC
        epoch in its own unit (microseconds, or nanoseconds at scale 9), and the
        unit's count per second.

        The delta against an epoch literal of the value's own type is an instant
        difference, independent of the session zone; a naive value denotes UTC.
        A nanosecond delta is lowered as a bare ``a - b``, so it is held in an
        explicit cast node (``Value.cast`` drops a same-type cast) and stays one
        operand of any arithmetic around it.
        """
        from terrine.backends.timestamps import _UNSUPPORTED, _epoch

        dtype = value.type()
        if not isinstance(dtype, dt.Timestamp) or dtype.scale not in (None, 6, 9):
            raise _UNSUPPORTED.error(dtype=str(dtype))
        unit, divisor = ("nanosecond", 10**9) if dtype.scale == 9 else ("microsecond", 10**6)
        delta = value.delta(_epoch(dtype), unit=unit)
        # Ibis types the node constructor's operand loosely; the expression is an int64.
        atomic = ops.Cast(delta, to=dt.int64)  # ty: ignore[invalid-argument-type]
        return cast(ir.IntegerValue, atomic.to_expr()), divisor, dtype

    def _epoch_offset(
        self, value: ir.TimestampValue
    ) -> tuple[ir.IntegerValue, ir.IntegerValue, int, dt.Timestamp]:
        """``(seconds, remainder, divisor, dtype)`` since the typed UTC epoch.

        The remainder keeps the dividend's sign, so pre-epoch instants split as
        ``(0, -1)`` rather than ``(-1, 999999)``.
        """
        coefficient, divisor, dtype = self._epoch_coefficient(value)
        seconds = self.truncating_quotient(coefficient, divisor)
        remainder = cast(ir.IntegerValue, coefficient % divisor)
        return seconds, remainder, divisor, dtype

    def local_day(
        self, timestamp: ir.TimestampValue, offset_minutes: int
    ) -> tuple[ir.DateValue, ir.TimestampValue]:
        """The fixed-offset local calendar day of an instant and the UTC instant
        starting it, from one epoch delta, without leaving the integer range.

        With ``c`` the instant's epoch offset in its unit, ``o`` the offset in the
        same unit and ``D`` one day, the local day number is
        ``n = floor((c + o) / D)``. ``c + o`` itself may overflow ``int64`` next to
        an endpoint, so ``c`` is split first: ``q = floor(c / D)`` is the
        truncating quotient less one where ``c % D`` is negative (a backend whose
        ``%`` follows the divisor's sign never yields a negative remainder, and its
        truncating quotient of the floor multiple is already the floor), and
        ``r = ((c % D) + D) % D`` lies in ``[0, D)`` under either sign convention;
        neither forms a value beyond ``c``. Then ``r + o`` lies within
        ``(-D, 2D)``, so ``n = q + carry`` with ``carry`` in ``{-1, 0, 1}`` by
        comparing ``r + o`` with ``0`` and ``D``; no division of the shifted
        value is needed. Local midnight is ``n·86400`` seconds after the naive
        epoch, whose date is the local day, and the day starts ``o`` earlier as a
        UTC instant; both are whole seconds lowered as one interval each, so
        neither depends on the session zone. A day start the timestamp type
        cannot hold (within a day of its minimum) is a backend range error, which
        execution reports by code. One delta and one quotient keep the expression
        shallow where chained conversions would nest.
        """
        from terrine.backends.timestamps import _epoch

        coefficient, divisor, _dtype = self._epoch_coefficient(timestamp)
        day_units = 86_400 * divisor
        offset_seconds = offset_minutes * 60
        signed_remainder = cast(ir.IntegerValue, coefficient % day_units)
        zero = cast(ir.IntegerScalar, ibis.literal(0, type=dt.int64))
        one = cast(ir.IntegerScalar, ibis.literal(1, type=dt.int64))
        negative = cast(ir.IntegerValue, (signed_remainder < zero).ifelse(one, zero))
        day_number = cast(
            ir.IntegerValue, self.truncating_quotient(coefficient, day_units) - negative
        )
        if offset_seconds:
            remainder = cast(ir.IntegerValue, (signed_remainder + day_units) % day_units)
            local = cast(ir.IntegerValue, remainder + offset_seconds * divisor)
            full_day = cast(ir.IntegerScalar, ibis.literal(day_units, type=dt.int64))
            overflow = cast(ir.IntegerValue, (local >= full_day).ifelse(one, zero))
            carry = cast(ir.IntegerValue, (local < zero).ifelse(-one, overflow))
            day_number = cast(ir.IntegerValue, day_number + carry)
        local_seconds = cast(ir.IntegerValue, day_number * 86_400)
        start_seconds = (
            cast(ir.IntegerValue, local_seconds - offset_seconds)
            if offset_seconds
            else local_seconds
        )
        wall = _epoch(dt.Timestamp()) + local_seconds.as_interval("s")
        # A whole-second instant needs no sub-second scale; keep the unscaled aware
        # type the registered engines' casts produce, so unions of day states and
        # window endpoints keep one schema.
        start = _epoch(dt.Timestamp(timezone="UTC")) + start_seconds.as_interval("s")
        return wall.date(), start

    def day_start(self, timestamp: ir.TimestampValue, offset_minutes: int) -> ir.TimestampValue:
        """The UTC instant starting the fixed-offset local day that contains
        ``timestamp``; endpoint alignment compares an instant with it."""
        _day, start = self.local_day(timestamp, offset_minutes)
        return start

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

    def _microsecond_instant(self, value: ir.Value, operation: str) -> ir.TimestampValue:
        """A UTC conversion operand; nanosecond values refuse before any query.

        The portable conversions rebuild the instant from whole-second and
        sub-second intervals, and Ibis lowers integer intervals at microsecond
        resolution at best, so a nanosecond value has no portable construction.
        """
        instant = cast(ir.TimestampValue, value)
        dtype = instant.type()
        if isinstance(dtype, dt.Timestamp) and dtype.scale == 9:
            raise unavailable(operation, self.name)
        return instant

    def utc_wall_clock(self, value: ir.Value) -> ir.TimestampValue:
        instant = self._microsecond_instant(value, "utc_wall_clock")
        seconds, remainder, divisor, dtype = self._epoch_offset(instant)
        naive = dt.Timestamp(scale=dtype.scale, nullable=dtype.nullable)
        return self._rebuild(naive, seconds, remainder, divisor)

    def utc_instant(self, value: ir.Value) -> ir.TimestampValue:
        wall = self._microsecond_instant(value, "utc_instant")
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

    def lookup_keys(
        self, point: ir.TimestampValue, state: ir.TimestampValue
    ) -> tuple[ir.Value, ir.Value]:
        """Keys comparing a point cutoff with a state instant: the order keys.

        The portable lookup unions both keys into one timeline, which needs
        one type: an instant whose scale is unstated (a fixed anchor's
        literal) is the engine's default microsecond precision, so it takes
        the other side's stated microsecond scale.
        """
        point_key, state_key = self.timestamp_order_key(point), self.timestamp_order_key(state)
        point_type, state_type = point_key.type(), state_key.type()
        if (
            isinstance(point_type, dt.Timestamp)
            and isinstance(state_type, dt.Timestamp)
            and point_type.timezone == state_type.timezone
            and {point_type.scale, state_type.scale} == {None, 6}
        ):
            target = dt.Timestamp(timezone=point_type.timezone, scale=6)
            return point_key.cast(target), state_key.cast(target)
        return point_key, state_key

    def comparison_keys(
        self, left: ir.TimestampValue, right: ir.TimestampValue
    ) -> tuple[ir.Value, ir.Value] | None:
        """Keys whose native comparison orders two instants exactly, or None.

        Without native keys, comparisons order (rank, seconds, nanos) parts.
        """
        return None

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

    def typed_null(self, dtype: dt.DataType) -> ir.Value:
        """A NULL of ``dtype``, such as a placeholder in one branch of a union."""
        return _typed_null(dtype)

    def null_safe_equal(self, left: ir.Value, right: ir.Value) -> ir.BooleanValue:
        """Compare nullable keys without changing their equality domain."""
        return left.identical_to(right)

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
        from terrine.backends.joins import union_unit_keys

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
        left_unit_key, right_unit_key = union_unit_keys(left[left_unit], right[right_unit])
        key_type = left[left_key].type().copy(nullable=True)
        requests = left.select(
            **{
                unit: left_unit_key,
                key: left[left_key].cast(key_type),
                kind: ibis.literal(left_kind),
            }
        ).distinct()
        states = right.select(
            **{
                unit: right_unit_key,
                key: right[right_key].cast(right[right_key].type().copy(nullable=True)),
                kind: ibis.literal(right_kind),
            }
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

    def boolean_not(self, value: ir.BooleanValue) -> ir.BooleanValue:
        """Three-valued NOT, NULL for NULL, that stays a negation inside a null test.

        Ibis renders a negation followed by IS [NOT] NULL without parentheses, and
        SQL reads ``NOT x IS NULL`` as ``NOT (x IS NULL)``. Comparing with FALSE
        negates every boolean exactly and keeps its own parentheses.
        """
        return value == ibis.literal(False)

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
