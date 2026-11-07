"""Accuracy bounds against the exact sum of original binary64 inputs."""

import math
from collections.abc import Sequence


def window_sum_bound(values: Sequence[float]) -> float:
    return 1e-9 * len(values) + 1e-12 * math.fsum(abs(value) for value in values)


def native_window_bound(prefix_values: Sequence[float]) -> float:
    """The native binary64 window-sum bound, over the history prefix to the window end."""
    return 2 * len(prefix_values) * 2**-53 * math.fsum(abs(value) for value in prefix_values)


def ratio_bound(n_hat: float, d_hat: float, e_n: float, e_d: float) -> float | None:
    if abs(d_hat) <= e_d:
        return None
    return (abs(n_hat) * e_d + abs(d_hat) * e_n) / (abs(d_hat) * (abs(d_hat) - e_d)) + 2**-51 * abs(
        n_hat / d_hat
    )
