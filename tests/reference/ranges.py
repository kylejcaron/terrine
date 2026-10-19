"""Independent row-loop references for half-open event windows."""

from datetime import timedelta
from decimal import Decimal
from fractions import Fraction

import pandas as pd


def window_values(events: pd.DataFrame, anchors: pd.DataFrame, start: int, end: int):
    """Return exact summaries without importing temporal or compiler code."""
    rows = []
    for anchor in anchors.to_dict("records"):
        selected = events.loc[
            (events["unit"] == anchor["unit"])
            & (events["timestamp"] >= anchor["t0"] + timedelta(seconds=start))
            & (events["timestamp"] < anchor["t0"] + timedelta(seconds=end))
        ]
        values = sorted(int(value) for value in selected["value"].dropna())
        if values:
            rank = Fraction(len(values) - 1, 4)
            lower = rank.numerator // rank.denominator
            fraction = rank - lower
            quantile = Fraction(values[lower])
            if fraction:
                quantile += (values[lower + 1] - values[lower]) * fraction
            q25 = Decimal(quantile.numerator) / Decimal(quantile.denominator)
        else:
            q25 = None
        rows.append(
            {
                "unit": anchor["unit"],
                "anchor_id": anchor["anchor_id"],
                "minimum": min(values) if values else None,
                "maximum": max(values) if values else None,
                "distinct": len(set(values)),
                "q25": q25,
                "event_count": len(selected),
                "nonnull_count": len(values),
            }
        )
    return rows
