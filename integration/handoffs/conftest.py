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
            pytest.skip(f"optional {name} extra is absent")  # ty: ignore[too-many-positional-arguments]
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
        table=table,
        roles=Roles(
            unit="unit",
            anchor="anchor_id",
            t0="t0",
            treatment="treatment",
            features=("x",),
            outcomes=("y",),
        ),
        columns={
            "x": ColumnMeta(
                window=None,
                role="feature",
                observed_from_column=None,
                observed_until_column=None,
                complete_column=None,
                reason_column=None,
            ),
            "y": ColumnMeta(
                window=None,
                role="outcome",
                observed_from_column=None,
                observed_until_column=None,
                complete_column="complete",
                reason_column="reason",
            ),
        },
        provenance=Provenance(
            feature_cutoffs={},
            eligibility_cutoffs={},
            design_hash="example",
            model_hash="example",
            source_generations={},
            execution_snapshot=ExecutionSnapshot(
                snapshot_id=None,
                backend="arrow",
                source_generations={},
                input_binding_id=None,
                replay_equivalence="unverified",
            ),
            shape="cross_section",
            treatment_design="baseline_binary",
            engine_name="arrow",
            dialect="generic",
            engine_status="unverified",
        ),
    )
