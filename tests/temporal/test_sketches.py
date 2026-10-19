import random
import subprocess
import sys
from decimal import Decimal

import pytest

pytest.importorskip("datasketches")
from terrine.spec.expressions import SketchRequest
from terrine.temporal.nonadditive import exact_quantile
from terrine.temporal.sketches import (
    approximate_count_distinct,
    approximate_quantile,
    merge_count_distinct_partitions,
    merge_quantile_partitions,
)


def test_tdigest_quantile_accuracy_and_empirical_metadata():
    values = random.Random(20261019).sample(range(100_000), 10_000)
    request = SketchRequest("tdigest", "5.2.0", Decimal("0.02"))
    estimate = approximate_quantile(values, Decimal("0.5"), request)
    exact = exact_quantile(values, Decimal("0.5"))
    assert abs(estimate.value - float(exact)) <= 1000
    assert estimate.metadata.algorithm == "tdigest"
    assert estimate.metadata.error_bound is None
    assert estimate.metadata.error_basis == "empirical_no_guaranteed_bound"
    assert dict(estimate.metadata.parameters)["k"] >= 20


def test_tdigest_caps_extremely_strict_empirical_tolerance_without_overflow():
    request = SketchRequest("tdigest", "5.2.0", Decimal("1e-400"))
    estimate = approximate_quantile([0, 1, 2], Decimal("0.5"), request)
    assert dict(estimate.metadata.parameters)["k"] == 1000


def test_tdigest_partition_merge_is_canonical_and_fresh_process_reproducible():
    request = SketchRequest("tdigest", "5.2.0", Decimal("0.02"))
    partitions = {
        "part-c": list(range(600, 900)),
        "part-a": list(range(0, 300)),
        "part-b": list(range(300, 600)),
    }
    estimate = merge_quantile_partitions(partitions, Decimal("0.5"), request)
    assert estimate.metadata.merge_operation == "tdigest_merge_sorted_partition_id"
    code = """from decimal import Decimal
from terrine.spec.expressions import SketchRequest
from terrine.temporal.sketches import merge_quantile_partitions
parts = {'part-c': list(range(600, 900)), 'part-a': list(range(0, 300)),
         'part-b': list(range(300, 600))}
request = SketchRequest('tdigest', '5.2.0', Decimal('0.02'))
print(merge_quantile_partitions(parts, Decimal('0.5'), request).value)
"""
    fresh = float(subprocess.check_output([sys.executable, "-c", code], text=True))
    assert fresh == estimate.value
    assert abs(estimate.value - 449.5) <= 100


def test_hll_distinct_estimate_has_published_relative_error_bound():
    values = [f"item-{index}" for index in range(5000)]
    request = SketchRequest("hll", "5.2.0", Decimal("0.03"))
    estimate = approximate_count_distinct(values, request)
    assert estimate.metadata.error_bound is not None
    assert abs(estimate.value - len(set(values))) / len(values) <= float(
        estimate.metadata.error_bound
    )
    assert estimate.metadata.exactness == "approximate"
    assert estimate.metadata.error_basis == "datasketches_99_7_percent_relative_bound"


def test_hll_partition_union_is_independent_of_mapping_insertion_order():
    request = SketchRequest("hll", "5.2.0", Decimal("0.03"))
    partitions = {"b": range(300, 600), "a": range(0, 300)}
    first = merge_count_distinct_partitions(partitions, request)
    second = merge_count_distinct_partitions(dict(reversed(list(partitions.items()))), request)
    assert first == second


def test_unattainable_hll_bound_refuses_instead_of_weakening_request():
    request = SketchRequest("hll", "5.2.0", Decimal("0.000001"))
    try:
        approximate_count_distinct(["a", "b"], request)
    except Exception as error:
        assert getattr(error, "code", None) == "terrine.metric.sketch_unavailable"
    else:
        raise AssertionError("unattainable HLL bound was accepted")
