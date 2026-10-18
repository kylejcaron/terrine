"""Exact, in-memory order-statistic primitives shared by local references."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from fractions import Fraction

__all__ = ["exact_quantile"]


def _decimal_exact(value: Fraction) -> Decimal:
    denominator = value.denominator
    twos = fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator != 1:
        raise ArithmeticError(
            "exact decimal quantile interpolation produced a non-decimal rational"
        )
    scale = max(twos, fives)
    coefficient = value.numerator * 2 ** (scale - twos) * 5 ** (scale - fives)
    digits = tuple(int(digit) for digit in str(abs(coefficient)))
    return Decimal((int(coefficient < 0), digits, -scale))


def exact_quantile(values: Iterable[int | Decimal | None], probability: Decimal) -> int | Decimal:
    """Return Hyndman--Fan type 7, preserving exact numeric arithmetic.

    Null values are excluded, as in SQL aggregate quantiles. Empty and all-null
    inputs are unavailable and raise ``ValueError``; callers retain window
    completeness and reason metadata separately.
    """
    if not isinstance(probability, Decimal) or not probability.is_finite():
        raise TypeError("probability must be a finite Decimal")
    if probability < 0 or probability > 1:
        raise ValueError("probability must be in [0, 1]")
    ordered = sorted(value for value in values if value is not None)
    if not ordered:
        raise ValueError("quantile is undefined for an empty or all-null window")
    if any(isinstance(value, bool) or not isinstance(value, (int, Decimal)) for value in ordered):
        raise TypeError("exact quantile supports integer and Decimal values only")
    rank = Fraction(len(ordered) - 1) * Fraction(probability)
    lower = rank.numerator // rank.denominator
    remainder = rank - lower
    if remainder == 0:
        return ordered[lower]
    left, right = ordered[lower], ordered[lower + 1]
    return _decimal_exact(Fraction(left) + (Fraction(right) - Fraction(left)) * remainder)
