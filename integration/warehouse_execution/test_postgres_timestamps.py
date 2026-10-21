"""Exact PostgreSQL timestamp parts and comparisons against a Python reference."""

from __future__ import annotations

import itertools
from datetime import UTC, datetime, timedelta

import pytest

from integration.warehouse_execution.postgres_fixtures import (
    postgres_scratch as postgres_scratch,  # noqa: F401
)
from terrine.temporal._timestamps import compare_timestamps, timestamp_parts

pytestmark = pytest.mark.postgres

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_VALUES = {
    "before_epoch": "1969-12-31 23:59:59.999999+00",
    "epoch": "1970-01-01 00:00:00+00",
    "after_epoch": "1970-01-01 00:00:00.000001+00",
    "ordinary": "2025-06-01 12:34:56.789012+00",
    "next_second": "2025-06-01 12:34:57.789012+00",
    "positive_infinity": "infinity",
    "negative_infinity": "-infinity",
    "missing": None,
}


def _reference(text: str | None) -> tuple[int, int, int] | None:
    if text is None:
        return None
    if text == "infinity":
        return (1, 0, 0)
    if text == "-infinity":
        return (-1, 0, 0)
    instant = datetime.fromisoformat(text.replace("+00", "+00:00"))
    micros = (instant - _EPOCH) // timedelta(microseconds=1)
    seconds = int(micros / 10**6)  # truncate toward zero, like the SQL
    return (0, seconds, (micros - seconds * 10**6) * 1000)


def _compare(left, right, op: str, offset: int) -> bool | None:
    if left is None or right is None:
        return None
    if right[0] == 0 and offset:
        nanos = right[1] * 10**9 + right[2] + offset * 10**9
        right = (0, nanos, 0)
        left = (left[0], left[1] * 10**9 + left[2], 0) if left[0] == 0 else left
    return {
        "lt": left < right,
        "le": left <= right,
        "gt": left > right,
        "ge": left >= right,
        "eq": left == right,
    }[op]


def test_parts_and_comparisons_match_the_reference(postgres_scratch):  # noqa: F811
    con = postgres_scratch.con
    con.raw_sql("CREATE TABLE instants(name text, at timestamptz)").close()
    for name, text in _VALUES.items():
        con.raw_sql(
            "INSERT INTO instants VALUES "
            f"('{name}', {'NULL' if text is None else repr(text)}::timestamptz)"
        ).close()
    table = con.table("instants", database=postgres_scratch.schema)
    rank, seconds, nanos = timestamp_parts(table.at)
    parts = {
        row["name"]: None if row["rank"] is None else (row["rank"], row["seconds"], row["nanos"])
        for row in table.select("name", rank=rank, seconds=seconds, nanos=nanos)
        .to_pyarrow()
        .to_pylist()
    }
    assert parts == {name: _reference(text) for name, text in _VALUES.items()}

    left = table.select(left_name=table.name, left_at=table.at)
    right = table.select(right_name=table.name, right_at=table.at)
    pairs = left.cross_join(right)
    for op, offset in itertools.product(("lt", "le", "gt", "ge", "eq"), (0, 1, -86_400)):
        result = pairs.select(
            "left_name",
            "right_name",
            value=compare_timestamps(
                pairs.left_at, pairs.right_at, op, right_offset_seconds=offset
            ),
        ).to_pyarrow()
        for row in result.to_pylist():
            expected = _compare(
                _reference(_VALUES[row["left_name"]]),
                _reference(_VALUES[row["right_name"]]),
                op,
                offset,
            )
            assert row["value"] == expected, (op, offset, row)
