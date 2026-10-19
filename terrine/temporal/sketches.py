"""Optional Apache DataSketches adapters with explicit accuracy metadata.

DataSketches t-digest quantile accuracy is empirical and has no guaranteed error
bound. HLL estimates expose the library's published three-standard-deviation bound.
"""

from __future__ import annotations

import importlib.metadata
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal, Protocol

import pyarrow as pa

from terrine.errors import RefusalSpec


class SketchRequestLike(Protocol):
    @property
    def algorithm(self) -> str: ...

    @property
    def version(self) -> str: ...

    @property
    def rank_error(self) -> Decimal: ...


__all__ = [
    "SketchMetadata",
    "SketchEstimate",
    "approximate_count_distinct",
    "approximate_quantile",
    "merge_count_distinct_partitions",
    "approximate_quantiles_by_key",
    "validate_sketch_request",
    "quantile_request_metadata",
    "merge_quantile_partitions",
]

_UNAVAILABLE = RefusalSpec(
    "terrine.metric.sketch_unavailable",
    "Sketch {algorithm!r} version {version!r} is unavailable or cannot meet its declared "
    "error bound; install terrine[sketches] or use the exact form.",
)


def validate_sketch_request(request: SketchRequestLike) -> str:
    """Verify the requested optional t-digest package/version before source queries."""
    _, version = _package(request, "tdigest")
    return version


def quantile_request_metadata(request: SketchRequestLike) -> SketchMetadata:
    _, version = _package(request, "tdigest")
    return _tdigest_metadata(version, _tdigest_k(request), "tdigest_stream_update")


@dataclass(frozen=True, slots=True)
class SketchMetadata:
    algorithm: str
    version: str
    parameters: tuple[tuple[str, int], ...]
    error_bound: Decimal | None
    error_kind: Literal["empirical_value_tolerance", "relative_count"]
    error_basis: Literal[
        "empirical_no_guaranteed_bound", "datasketches_99_7_percent_relative_bound"
    ]
    merge_operation: Literal[
        "tdigest_stream_update",
        "tdigest_merge_sorted_partition_id",
        "hll_union_sorted_partition_id",
    ]
    exactness: Literal["approximate"] = "approximate"


@dataclass(frozen=True, slots=True)
class SketchEstimate:
    value: float
    metadata: SketchMetadata


def _package(request: SketchRequestLike, algorithm: str):
    if request.algorithm != algorithm:
        raise _UNAVAILABLE.error(algorithm=request.algorithm, version=request.version)
    try:
        import datasketches
    except ImportError as exc:
        raise _UNAVAILABLE.error(algorithm=request.algorithm, version=request.version) from exc
    version = importlib.metadata.version("datasketches")
    if request.version != version:
        raise _UNAVAILABLE.error(algorithm=request.algorithm, version=request.version)
    return datasketches, version


def _hll_lg_k(datasketches, error: Decimal, request: SketchRequestLike, *, unioned: bool) -> int:
    for lg_k in range(7, 22):
        lower = abs(datasketches.hll_sketch.get_rel_err(False, unioned, lg_k, 3))
        upper = abs(datasketches.hll_sketch.get_rel_err(True, unioned, lg_k, 3))
        if Decimal(str(max(lower, upper))) <= error:
            return lg_k
    raise _UNAVAILABLE.error(algorithm=request.algorithm, version=request.version)


def _numeric_batches(values: Iterable[int | float | Decimal | None], *, batch_size: int = 4096):
    batch: list[float] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise TypeError("t-digest sketches support integer, float, and Decimal values")
        converted = float(value)
        if not math.isfinite(converted):
            raise ValueError("t-digest sketch values must be finite")
        batch.append(converted)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _distinct_values(values: Iterable[str | int | float | None]):
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise TypeError("HLL sketches support string and numeric identifiers")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("HLL sketch values must be finite")
        yield value


def _tdigest_k(request: SketchRequestLike) -> int:
    # rank_error is a requested empirical tolerance, not a guaranteed bound.
    # Use it only to choose compression; metadata makes the absence of a bound explicit.
    if request.rank_error <= Decimal("0.001"):
        return 1000
    requested = math.ceil(Decimal(1) / request.rank_error)
    return max(200, min(1000, requested))


def _tdigest(values: Iterable[int | float | Decimal | None], request: SketchRequestLike):
    datasketches, version = _package(request, "tdigest")
    k = _tdigest_k(request)
    sketch = datasketches.tdigest_double(k)
    import numpy

    for batch in _numeric_batches(values):
        sketch.update(numpy.asarray(batch, dtype=numpy.float64))
    return datasketches, sketch, version, k


def _tdigest_metadata(
    version: str,
    k: int,
    operation: Literal["tdigest_stream_update", "tdigest_merge_sorted_partition_id"],
) -> SketchMetadata:
    return SketchMetadata(
        "tdigest",
        version,
        (("k", k),),
        None,
        "empirical_value_tolerance",
        "empirical_no_guaranteed_bound",
        operation,
    )


def approximate_quantile(
    values: Iterable[int | float | Decimal | None],
    probability: Decimal,
    request: SketchRequestLike,
) -> SketchEstimate:
    if (
        not isinstance(probability, Decimal)
        or not probability.is_finite()
        or not 0 <= probability <= 1
    ):
        raise ValueError("probability must be a Decimal in [0, 1]")
    _, sketch, version, k = _tdigest(values, request)
    if sketch.is_empty():
        raise ValueError("approximate quantile is undefined for an empty or all-null window")
    return SketchEstimate(
        sketch.get_quantile(float(probability)),
        _tdigest_metadata(version, k, "tdigest_stream_update"),
    )


def merge_quantile_partitions(
    partitions: Mapping[str, Iterable[int | float | Decimal | None]],
    probability: Decimal,
    request: SketchRequestLike,
) -> SketchEstimate:
    if not partitions or any(not isinstance(key, str) or not key for key in partitions):
        raise ValueError("quantile partitions require nonempty string identifiers")
    if (
        not isinstance(probability, Decimal)
        or not probability.is_finite()
        or not 0 <= probability <= 1
    ):
        raise ValueError("probability must be a Decimal in [0, 1]")
    datasketches, version = _package(request, "tdigest")
    k = _tdigest_k(request)
    merged = datasketches.tdigest_double(k)
    for partition_id in sorted(partitions):
        part = datasketches.tdigest_double(k)
        import numpy

        for batch in _numeric_batches(partitions[partition_id]):
            part.update(numpy.asarray(batch, dtype=numpy.float64))
        if not part.is_empty():
            merged.merge(part)
    if merged.is_empty():
        raise ValueError("approximate quantile is undefined for empty partitions")
    return SketchEstimate(
        merged.get_quantile(float(probability)),
        _tdigest_metadata(version, k, "tdigest_merge_sorted_partition_id"),
    )


def approximate_quantiles_by_key(
    batches: Iterable[pa.RecordBatch],
    probability: Decimal,
    request: SketchRequestLike,
    *,
    key_columns: tuple[str, str] = ("unit", "anchor_id"),
    value_column: str = "value",
) -> tuple[dict[tuple[object, object], float], SketchMetadata]:
    """Update one t-digest at a time from canonical-order Arrow batches."""
    if (
        not isinstance(probability, Decimal)
        or not probability.is_finite()
        or not 0 <= probability <= 1
    ):
        raise ValueError("probability must be a Decimal in [0, 1]")
    datasketches, version = _package(request, "tdigest")
    k = _tdigest_k(request)
    import numpy

    results = {}
    current_key = None
    sketch: Any = None
    pending = []

    def finish():
        if sketch is not None and current_key is not None and not sketch.is_empty():
            results[current_key] = sketch.get_quantile(float(probability))

    for batch in batches:
        for row in batch.to_pylist():
            key = (row[key_columns[0]], row[key_columns[1]])
            if current_key is not None and key != current_key:
                if pending:
                    sketch.update(numpy.asarray(pending, dtype=numpy.float64))
                    pending.clear()
                finish()
                sketch = None
            if sketch is None:
                current_key = key
                sketch = datasketches.tdigest_double(k)
            value = row[value_column]
            if value is not None:
                pending.append(float(value))
                if len(pending) == 4096:
                    sketch.update(numpy.asarray(pending, dtype=numpy.float64))
                    pending.clear()
    if pending and sketch is not None:
        sketch.update(numpy.asarray(pending, dtype=numpy.float64))
    finish()
    return results, _tdigest_metadata(version, k, "tdigest_stream_update")


def _hll_sketch(values, request: SketchRequestLike, *, unioned: bool):
    datasketches, version = _package(request, "hll")
    lg_k = _hll_lg_k(datasketches, request.rank_error, request, unioned=unioned)
    sketch = datasketches.hll_sketch(lg_k)
    for value in _distinct_values(values):
        sketch.update(value)
    return sketch, version, lg_k


def _hll_metadata(sketch, version: str, lg_k: int, *, unioned: bool) -> SketchMetadata:
    bound = Decimal(
        str(
            max(
                abs(sketch.get_rel_err(False, unioned, lg_k, 3)),
                abs(sketch.get_rel_err(True, unioned, lg_k, 3)),
            )
        )
    )
    return SketchMetadata(
        "hll",
        version,
        (("lg_k", lg_k), ("num_std_devs", 3)),
        bound,
        "relative_count",
        "datasketches_99_7_percent_relative_bound",
        "hll_union_sorted_partition_id",
    )


def approximate_count_distinct(
    values: Iterable[str | int | float | None], request: SketchRequestLike
) -> SketchEstimate:
    sketch, version, lg_k = _hll_sketch(values, request, unioned=False)
    return SketchEstimate(
        sketch.get_estimate(), _hll_metadata(sketch, version, lg_k, unioned=False)
    )


def merge_count_distinct_partitions(
    partitions: Mapping[str, Iterable[str | int | float | None]], request: SketchRequestLike
) -> SketchEstimate:
    if not partitions or any(not isinstance(key, str) or not key for key in partitions):
        raise ValueError("distinct-count partitions require nonempty string identifiers")
    datasketches, version = _package(request, "hll")
    lg_k = _hll_lg_k(datasketches, request.rank_error, request, unioned=True)
    union = datasketches.hll_union(lg_k)
    for partition_id in sorted(partitions):
        part = datasketches.hll_sketch(lg_k)
        for value in _distinct_values(partitions[partition_id]):
            part.update(value)
        union.update(part)
    sketch = union.get_result(datasketches.tgt_hll_type.HLL_8)
    return SketchEstimate(sketch.get_estimate(), _hll_metadata(sketch, version, lg_k, unioned=True))
