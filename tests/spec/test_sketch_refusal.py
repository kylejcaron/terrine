from decimal import Decimal

import pytest

from terrine.errors import TerrineError
from terrine.spec.anchors import EventAnchor
from terrine.spec.design import Design
from terrine.spec.expressions import ApproxMetricQuantile, SketchRequest
from terrine.spec.io import from_json, to_json
from terrine.spec.validate import validate_design
from terrine.windows import Window
from tests.spec.test_validate import model


@pytest.mark.parametrize("round_trip", [False, True])
def test_unavailable_explicit_sketch_uses_same_refusal_for_all_design_paths(round_trip):
    design = Design(
        "user",
        EventAnchor("session"),
        features={
            "p50": ApproxMetricQuantile(
                "sessions",
                Decimal("0.5"),
                Window("-7d", "0d"),
                SketchRequest("tdigest", "1", Decimal("0.01")),
            )
        },
    )
    if round_trip:
        design = from_json(to_json(design))
    with pytest.raises(TerrineError) as error:
        validate_design(design, model())
    assert error.value.code == "terrine.metric.sketch_unavailable"
    assert error.value.context == {"algorithm": "tdigest", "version": "1"}


def test_missing_optional_extra_refuses_approx_quantile_before_source_query(monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "datasketches", None)
    design = Design(
        "user",
        EventAnchor("session"),
        features={
            "p50": ApproxMetricQuantile(
                "sessions",
                Decimal("0.5"),
                Window("-7d", "0d"),
                SketchRequest("tdigest", "5.2.0", Decimal("0.01")),
            )
        },
    )
    with pytest.raises(TerrineError) as error:
        validate_design(design, model())
    assert error.value.code == "terrine.metric.sketch_unavailable"
    assert "install terrine[sketches]" in str(error.value)
