from datetime import UTC, datetime
from importlib import import_module
from importlib.util import find_spec

import pyarrow as pa
import pytest

from terrine.data import ColumnMeta, ExecutionSnapshot, MaterializedFrame, Provenance, Roles


@pytest.fixture
def library():
    def load(name):
        if find_spec(name) is None:
            pytest.skip(f"optional {name} extra is absent")
        return import_module(name)

    return load


@pytest.fixture
def handoff_frame():
    n = 12
    table = pa.table(
        {
            "unit": list(range(n)),
            "anchor_id": list(range(n)),
            "t0": pa.array([datetime(2025, 1, 1, tzinfo=UTC)] * n),
            "treatment": [i % 2 for i in range(n)],
            "x": [(i // 2) % 2 for i in range(n)],
            "y": [float(i + (i % 3)) for i in range(n)],
            "complete": [True] * n,
            "reason": pa.array([None] * n, type=pa.string()),
        }
    )
    return MaterializedFrame(
        table,
        Roles("unit", "anchor_id", "t0", "treatment", ("x",), ("y",)),
        {
            "x": ColumnMeta(None, "feature", None, None, None, None),
            "y": ColumnMeta(None, "outcome", None, None, "complete", "reason"),
        },
        Provenance(
            {},
            {},
            "example",
            "example",
            {},
            ExecutionSnapshot(None, "arrow", {}, None, "unverified"),
            "cross_section",
            "baseline_binary",
        ),
    )
