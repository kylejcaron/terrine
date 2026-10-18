from decimal import Decimal

from terrine import quantile as public_quantile
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design
from terrine.spec.expressions import quantile
from terrine.spec.io import from_json, from_yaml, to_json, to_yaml


def test_quantile_spec_round_trips_through_design():
    design = Design(
        unit="user",
        anchor=FixedDate("2025-03-01"),
        outcomes={"p50": quantile("revenue", Decimal("0.5"), window=("0d", "7d"))},
    )
    assert public_quantile("revenue", "0.5", window=("0d", "7d")) == design.outcomes["p50"]
    assert from_json(to_json(design)) == design
    assert from_yaml(to_yaml(design)) == design
    assert from_json(to_json(design)).spec_hash == design.spec_hash
