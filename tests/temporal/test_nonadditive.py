from decimal import Decimal

import pytest

from terrine.errors import TerrineError
from terrine.spec.expressions import MetricQuantile, quantile
from terrine.temporal.nonadditive import exact_quantile


def test_quantile_declares_type7_rank_and_exact_interpolation():
    assert exact_quantile(
        [Decimal("1.00"), Decimal("2.00"), Decimal("8.00")], Decimal("0.25")
    ) == Decimal("1.50")
    assert exact_quantile([1, 2, 8], Decimal("0.25")) == Decimal("1.5")

    probability = Decimal("0." + "1" * 40)
    assert exact_quantile([0, 1], probability) == Decimal(probability)


@pytest.mark.parametrize(
    ("value_type", "probability"),
    [
        ("decimal(30, 0)", Decimal("0.125")),
        ("decimal(29, 25)", Decimal("0.12345678")),
    ],
)
def test_quantile_refuses_when_interpolation_intermediate_exceeds_decimal_budget(
    value_type, probability
):
    from terrine.temporal.nonadditive import _exact_numeric_dtype

    with pytest.raises(TerrineError) as error:
        _exact_numeric_dtype(value_type, "quantile", probability)

    assert error.value.code == "terrine.metric.exact_precision_unavailable"


def test_quantile_probability_is_canonical_decimal_and_in_range():
    node = quantile("revenue", Decimal("0.2500"), window=("-7d", "0d"))
    assert isinstance(node, MetricQuantile)
    assert node.probability == Decimal("0.25")
    for probability in (Decimal("-0.01"), Decimal("1.01")):
        with pytest.raises(ValueError):
            quantile("revenue", probability, window=("-7d", "0d"))
    with pytest.raises(TypeError):
        quantile("revenue", 0.25, window=("-7d", "0d"))  # ty: ignore[invalid-argument-type]


def test_empty_and_all_null_quantiles_are_unavailable():
    with pytest.raises(ValueError):
        exact_quantile([], Decimal("0.5"))
    with pytest.raises(ValueError):
        exact_quantile([None, None], Decimal("0.5"))


def test_approximate_quantile_expression_is_explicit_and_constructor_is_data_only():
    from terrine.spec.expressions import ApproxMetricQuantile, SketchRequest, approx_quantile

    node = approx_quantile(
        "revenue",
        Decimal("0.5"),
        window=("-7d", "0d"),
        sketch=SketchRequest("tdigest", "5.2.0", Decimal("0.01")),
    )
    assert isinstance(node, ApproxMetricQuantile)
    assert node.sketch.algorithm == "tdigest"
