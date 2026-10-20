from decimal import Decimal

import ibis
import pyarrow as pa
import pytest

from terrine.errors import TerrineError
from tests.fixtures.winsorization import create_model, design


def test_fixed_clamping_precedes_decimal_narrowing_and_preserves_nulls():
    from terrine.dataset.transforms import apply_winsorization, bind_winsorization
    from terrine.spec.transforms import Winsorization

    transform = bind_winsorization(
        Winsorization("value", lower="1.25", upper="8.75"), pa.decimal128(38, 0)
    )
    table = pa.table(
        {
            "value": pa.array(
                [
                    Decimal("99999999999999999999999999999999999999"),
                    Decimal("-99999999999999999999999999999999999999"),
                    Decimal(4),
                    None,
                ],
                type=pa.decimal128(38, 0),
            ),
            "reason": [None, None, None, "incomplete_coverage"],
        }
    )
    actual = apply_winsorization(table, (transform,))
    assert actual["value"].to_pylist() == [Decimal("8.75"), Decimal("1.25"), Decimal(4), None]
    assert actual.schema.field("value").type == pa.decimal128(3, 2)
    assert actual["reason"].equals(table["reason"])
    batch = apply_winsorization(table.to_batches()[0], (transform,))
    assert pa.Table.from_batches([batch]).equals(actual)


@pytest.mark.parametrize("dtype", [pa.float64(), pa.string(), pa.bool_()])
def test_unsupported_winsorization_types_refuse_before_application(dtype):
    from terrine.dataset.transforms import bind_winsorization
    from terrine.spec.transforms import Winsorization

    with pytest.raises(TerrineError) as error:
        bind_winsorization(Winsorization("value", lower="0", upper="1"), dtype)
    assert error.value.code == "terrine.transform.winsorization_type_unsupported"
    assert error.value.context["route"]


def test_winsorization_refuses_unrepresentable_output_and_unknown_stage():
    from terrine.dataset.transforms import bind_winsorization
    from terrine.spec.transforms import Winsorization

    with pytest.raises(TerrineError) as precision:
        bind_winsorization(Winsorization("value", lower="1e38", upper="1e38"), pa.int64())
    assert precision.value.code == "terrine.transform.winsorization_precision_unavailable"
    with pytest.raises(TerrineError) as stage:
        Winsorization("value", lower="0", upper="1", stage="before_aggregation")
    assert stage.value.code == "terrine.transform.invalid_winsorization"


def test_canonical_decimal_limits_reopen_with_identical_output_type():
    from terrine.dataset.transforms import apply_winsorization, bind_winsorization
    from terrine.spec.transforms import Winsorization

    first = bind_winsorization(Winsorization("value", lower="1.2500", upper="8.7500"), pa.int64())
    second = bind_winsorization(Winsorization("value", lower="1.25", upper="8.75"), pa.int64())
    assert first.digest == second.digest
    restored = Winsorization.from_json(first.to_json())
    actual = apply_winsorization(pa.table({"value": [0, 4, 10]}), (restored,))
    assert actual["value"].to_pylist() == [Decimal("1.25"), Decimal(4), Decimal("8.75")]


def test_unsupported_adapter_transform_is_coded_refusal():
    from terrine import Coverage, Model, UnitSource
    from terrine.semantic.bind import bind_metric
    from terrine.semantic.models import Catalog

    con = ibis.duckdb.connect()
    try:
        definitions = create_model(con).catalog.model_dump(mode="python")
        declaration = {"method": "fixed", "limit": 5}
        definitions["metrics"]["total"]["winsorization"] = declaration
        catalog = Catalog.model_validate(definitions)
        assert bind_metric(catalog, "total").transforms == (declaration,)
        model = Model.from_catalog(
            catalog,
            con=con,
            units={"user": UnitSource("users", "user_id")},
            readiness={name: Coverage("2025-01-01", "2025-01-12") for name in ("events", "labels")},
        )
        with pytest.raises(TerrineError) as error:
            design().validate(model)
        assert error.value.code == "terrine.metric.unsupported_transform"
        assert "Frame.winsorize" in error.value.context["route"]
    finally:
        con.disconnect()
