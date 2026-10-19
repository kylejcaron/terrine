"""Independent pandas retention counts over explicit event identities."""

from datetime import timedelta

import pandas as pd


def retention_rows(
    events,
    anchors,
    *,
    thresholds,
    horizon=None,
    coverage_start=None,
    coverage_end=None,
    cutoff=None,
):
    rows = []
    for anchor in anchors.to_dict("records"):
        origin = anchor["t0"]
        end = origin + timedelta(seconds=horizon) if horizon is not None else cutoff
        observed = anchor["observed_until"]
        if pd.isna(origin) or origin >= end:
            reason = "horizon_unavailable"
        elif (coverage_start is not None and origin < coverage_start) or (
            coverage_end is not None and end > coverage_end
        ):
            reason = "incomplete_coverage"
        elif (
            "observed_from" in anchor
            and not pd.isna(anchor["observed_from"])
            and origin < anchor["observed_from"]
        ):
            reason = "incomplete_observation"
        elif (horizon is not None and pd.isna(observed)) or (
            not pd.isna(observed) and observed < end
        ):
            reason = "insufficient_followup"
        else:
            reason = None
        selected = events.loc[
            (events["unit"] == anchor["unit"])
            & (events["name"] == "target")
            & (events["timestamp"] >= origin)
            & (events["timestamp"] < end)
        ]
        unknown = events.loc[
            (events["unit"] == anchor["unit"])
            & events["name"].isna()
            & events["event_id"].notna()
            & (events["timestamp"] >= origin)
            & (events["timestamp"] < end)
        ]
        if reason is None and not unknown.empty:
            reason = "incomplete_label_history"
        count = selected["event_id"].nunique() if reason is None else None
        rows.append(
            {
                "unit": anchor["unit"],
                "anchor_id": anchor["anchor_id"],
                "occurrence_count": count,
                "complete": reason is None,
                "retention_band": sum(count >= threshold for threshold in thresholds)
                if count is not None
                else None,
                "reason": reason,
            }
        )
    return rows
