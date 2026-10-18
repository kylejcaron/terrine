"""Independent pandas row-loop references for repeated panel identities."""

from datetime import timedelta

import pandas as pd


def event_period_rows(events: pd.DataFrame, periods: tuple[int, ...], period_seconds: int):
    """Expand one selected event per unit into declared exact-relative-time rows."""
    rows = []
    for event in events.to_dict(orient="records"):
        for period in periods:
            rows.append(
                {
                    "unit": event["unit"],
                    "parent_event_id": event["event_id"],
                    "parent_t0": event["t0"],
                    "event_period": period,
                    "t0": event["t0"] + timedelta(seconds=period * period_seconds),
                }
            )
    return pd.DataFrame(rows)
