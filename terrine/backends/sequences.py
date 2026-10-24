"""Integer sequences on backends where Ibis cannot unnest a range."""

from __future__ import annotations

from functools import reduce

import ibis

__all__ = ["integer_sequence"]

_RADIX = 10


def integer_sequence(stop: int, name: str, *, dialect: str) -> ibis.Table:
    """A table of the integers ``0 <= name < stop`` as ``int64``.

    Ibis lowers an unnested range on Snowflake to ``TABLE(FLATTEN(...))``
    with an alias column list the warehouse rejects (``invalid identifier
    '_U.POS'``), so there the sequence is the cross product of one
    decimal-digit relation per place, filtered to ``stop``. Every other
    supported backend unnests the range natively.
    """
    if dialect.casefold() != "snowflake":
        return ibis.range(0, stop).unnest().cast("int64").name(name).as_table()
    digits = ibis.union(
        *(ibis.literal(digit, type="int64").name("digit").as_table() for digit in range(_RADIX))
    )
    places = len(str(max(stop - 1, 0)))
    table = reduce(
        lambda joined, place: joined.cross_join(digits.select(**{f"digit_{place}": digits.digit})),
        range(1, places),
        digits.select(digit_0=digits.digit),
    )
    ordinal = reduce(
        lambda total, place: total + table[f"digit_{place}"] * _RADIX**place,
        range(1, places),
        table.digit_0,
    )
    sequence = table.select(**{name: ordinal.cast("int64")})
    return sequence.filter(sequence[name] < stop)
