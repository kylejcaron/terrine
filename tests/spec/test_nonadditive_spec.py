import json
from decimal import Decimal, localcontext

import pytest

from terrine import quantile as public_quantile
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design
from terrine.spec.expressions import ApproxMetricQuantile, MetricQuantile, SketchRequest, quantile
from terrine.spec.io import from_json, from_yaml, to_json, to_yaml
from terrine.windows import Window


def _design(probability: Decimal | str) -> Design:
    return Design(
        unit="user",
        anchor=FixedDate("2025-03-01"),
        outcomes={"p50": quantile("revenue", probability, window=("0d", "7d"))},
    )


def test_quantile_spec_round_trips_through_design():
    design = _design(Decimal("0.5"))
    assert public_quantile("revenue", "0.5", window=("0d", "7d")) == design.outcomes["p50"]
    assert from_json(to_json(design)) == design
    assert from_yaml(to_yaml(design)) == design
    assert from_json(to_json(design)).spec_hash == design.spec_hash


def test_high_precision_probability_is_context_independent_in_design_round_trip():
    probability = Decimal("0." + "1" * 40)
    with localcontext() as context:
        context.prec = 8
        design = _design(probability)
        restored = from_json(to_json(design))
    expression = restored.outcomes["p50"]
    assert isinstance(expression, MetricQuantile)
    assert expression.probability == probability
    assert restored.spec_hash == design.spec_hash


@pytest.mark.parametrize("malformed", [0.1, True])
def test_serialized_probability_rejects_numeric_and_boolean_json(malformed):
    payload = json.loads(to_json(_design("0.5")))
    payload["design"]["outcomes"]["p50"]["probability"] = malformed
    with pytest.raises((TypeError, ValueError)):
        from_json(json.dumps(payload))


def test_approximate_quantile_spec_serialization_keeps_sketch_contract():
    expression = ApproxMetricQuantile(
        "revenue",
        Decimal("0.5"),
        Window("0d", "7d"),
        SketchRequest("tdigest", "1", Decimal("0.01")),
    )
    design = Design(
        unit="user",
        anchor=FixedDate("2025-03-01"),
        outcomes={"p50": expression},
    )
    assert from_json(to_json(design)) == design


@pytest.mark.parametrize("malformed", [0.01, True])
def test_serialized_sketch_rank_error_rejects_numeric_and_boolean_json(malformed):
    expression = ApproxMetricQuantile(
        "revenue",
        Decimal("0.5"),
        Window("0d", "7d"),
        SketchRequest("tdigest", "1", Decimal("0.01")),
    )
    design = Design(
        unit="user",
        anchor=FixedDate("2025-03-01"),
        outcomes={"p50": expression},
    )
    payload = json.loads(to_json(design))
    payload["design"]["outcomes"]["p50"]["sketch"]["rank_error"] = malformed
    with pytest.raises((TypeError, ValueError)):
        from_json(json.dumps(payload))
