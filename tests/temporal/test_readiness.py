from datetime import UTC, datetime

import pytest

from terrine.data import BoundCoverage
from terrine.errors import TerrineError
from terrine.temporal.readiness import Readiness


def test_readiness_looks_up_coverage_by_fact_source():
    coverage = BoundCoverage(datetime(2025, 3, 1, tzinfo=UTC), datetime(2025, 4, 1, tzinfo=UTC))
    readiness = Readiness({"events": coverage})
    assert readiness.for_fact("events") is coverage
    with pytest.raises(TerrineError) as error:
        readiness.for_fact("missing")
    assert error.value.code == "terrine.readiness.missing_source"
