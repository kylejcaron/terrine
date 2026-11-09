"""The independent benchmark oracle must reject missing or duplicated origins."""

from collections import Counter
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from benchmarks.query_strategy_spike import assert_support


@pytest.mark.parametrize("identities", [("first", "second"), (), ("first", "first")])
def test_support_requires_every_origin_once_while_allowing_tied_timestamps(identities):
    instant = datetime(2025, 1, 1, tzinfo=UTC)
    expected = Counter({("a", instant): 2})
    actual = pa.table(
        {
            "unit": pa.array(["a"] * len(identities), pa.string()),
            "t0": pa.array([instant] * len(identities), pa.timestamp("us", tz="UTC")),
            "anchor_id": pa.array(identities, pa.string()),
        }
    )
    if identities == ("first", "second"):
        assert assert_support(actual, expected) == 2
    else:
        with pytest.raises(AssertionError):
            assert_support(actual, expected)
