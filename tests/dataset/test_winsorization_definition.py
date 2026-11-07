from datetime import UTC, datetime
from decimal import Decimal, localcontext

import pyarrow as pa

from terrine.data import (
    DatasetViewMeta,
    ExecutionSnapshot,
    MaterializedFrame,
    Provenance,
    Roles,
)
from terrine.dataset.transforms import fit_winsorization


def test_fitted_limits_are_exact_under_small_decimal_context():
    table = pa.table(
        {
            "unit": ["a", "b", "c"],
            "anchor": [0, 1, 2],
            "t0": [datetime(2025, 1, 5, tzinfo=UTC)] * 3,
            "value": pa.array(
                [
                    Decimal("1000000000000000000000000000000.01"),
                    Decimal("1000000000000000000000000000000.03"),
                    None,
                ],
                type=pa.decimal128(38, 2),
            ),
            "__terrine_split": ["train"] * 3,
        }
    )
    data = MaterializedFrame(
        table=table,
        roles=Roles(
            unit="unit", anchor="anchor", t0="t0", treatment=None, features=("value",), outcomes=()
        ),
        columns={},
        provenance=Provenance(
            feature_cutoffs={},
            eligibility_cutoffs={},
            design_hash="c" * 64,
            model_hash="d" * 64,
            source_generations={},
            execution_snapshot=ExecutionSnapshot(
                snapshot_id=None,
                backend="arrow",
                source_generations={},
                input_binding_id=None,
                replay_equivalence="unverified",
            ),
            shape="cross_section",
            treatment_design=None,
            engine_name="arrow",
            dialect="generic",
            engine_status="unverified",
            dataset_view=DatasetViewMeta(
                kind="training",
                selected_features=("value",),
                selected_labels=(),
                view_hash="e" * 64,
                feature_definition_hash="b" * 64,
                split_policy={"kind": "group"},
            ),
        ),
    )
    with localcontext() as context:
        context.prec = 6
        fitted = fit_winsorization(
            data,
            column="value",
            lower="0.25",
            upper="0.75",
            definition_hash="a" * 64,
            feature_definition_hash="b" * 64,
        )
    assert fitted.lower == Decimal("1000000000000000000000000000000.015")
    assert fitted.upper == Decimal("1000000000000000000000000000000.025")
    assert fitted.fit is not None
    assert fitted.fit.non_null_count == 2
