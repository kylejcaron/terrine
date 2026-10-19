"""Independent NumPy reference for exact nonadditive window tests."""

from collections.abc import Iterable

import numpy as np


def numpy_type7(values: Iterable[int | float | None], probability: float) -> float:
    """Compute the linear type-7 quantile independently of the Terrine compiler."""
    observed = np.asarray([value for value in values if value is not None], dtype=np.float64)
    if observed.size == 0:
        raise ValueError("quantile is undefined for an empty or all-null window")
    return float(np.quantile(observed, probability, method="linear"))
