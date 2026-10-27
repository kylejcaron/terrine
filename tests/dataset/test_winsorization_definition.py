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
        table,
        Roles("unit", "anchor", "t0", None, ("value",), ()),
        {},
        Provenance(
            {},
            {},
            "c" * 64,
            "d" * 64,
            {},
            ExecutionSnapshot(None, "arrow", {}, None, "unverified"),
            "cross_section",
            None,
            engine_name="arrow",
            dialect="generic",
            engine_status="unverified",
            dataset_view=DatasetViewMeta(
                "training",
                ("value",),
                (),
                "e" * 64,
                "b" * 64,
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
