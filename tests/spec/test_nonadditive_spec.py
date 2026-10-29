import json
from decimal import Decimal, localcontext

import pytest

from terrine import quantile as public_quantile
from terrine.errors import TerrineError
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design
from terrine.spec.expressions import (
    ApproxMetricQuantile,
    MetricQuantile,
    SketchRequest,
    approx_distinct,
    quantile,
)
from terrine.spec.io import from_json, from_yaml, to_json, to_yaml
from terrine.windows import Window
from tests.spec.test_validate import model


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
    with pytest.raises(TerrineError) as error:
        from_json(json.dumps(payload))
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "malformed_document"


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
    with pytest.raises(TerrineError) as error:
        from_json(json.dumps(payload))
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "malformed_document"


def test_approximate_distinct_design_round_trips():
    from terrine.spec.expressions import ApproxMetricDistinct, approx_distinct

    design = Design(
        unit="user",
        anchor=FixedDate("2025-03-01"),
        outcomes={"distinct": approx_distinct("users", window=("0d", "7d"))},
    )
    expression = from_json(to_json(design)).outcomes["distinct"]
    assert isinstance(expression, ApproxMetricDistinct)
    assert expression == design.outcomes["distinct"]


@pytest.mark.parametrize("dialect", ["postgres", "bigquery", "snowflake", "unregistered"])
@pytest.mark.parametrize(
    ("expression", "code"),
    [
        (
            quantile("sessions", "0.5", window=("0d", "7d")),
            "terrine.metric.exact_quantile_backend_unsupported",
        ),
        (
            approx_distinct("sessions", window=("0d", "7d")),
            "terrine.metric.approx_backend_unavailable",
        ),
    ],
)
def test_nonadditive_backend_refusals_precede_queries(dialect, expression, code):
    design = Design(unit="user", anchor=FixedDate("2025-01-02"), outcomes={"value": expression})
    with pytest.raises(TerrineError) as caught:
        design.validate(model(dialect=dialect))
    assert caught.value.code == code
    assert dict(caught.value.context) == {"dialect": dialect}
