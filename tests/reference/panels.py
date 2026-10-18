"""Independent pandas row-loop references for repeated panel identities."""

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta

import pandas as pd


def _translated(
    origin: datetime, windows: Mapping[str, tuple[int, int]] | None
) -> dict[str, tuple[datetime, datetime]]:
    return {
        name: (origin + timedelta(seconds=start), origin + timedelta(seconds=end))
        for name, (start, end) in (windows or {}).items()
    }


def sequential_rows(
    *,
    units: Sequence[str],
    trial_times: Sequence[datetime],
    treatment_times: Mapping[str, datetime],
    trial_weights: Sequence[float] | None = None,
    windows: Mapping[str, tuple[int, int]] | None = None,
    cutoffs: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """Build schedule rows and exact per-trial windows from pandas/Python inputs."""
    weights = trial_weights or tuple(1 / len(trial_times) for _ in trial_times)
    if len(weights) != len(trial_times):
        raise ValueError("trial_weights must align with trial_times")
    rows = []
    for index, origin in enumerate(trial_times):
        for unit in units:
            treatment_time = treatment_times.get(unit)
            rows.append(
                {
                    "unit": unit,
                    "trial_id": f"schedule:{index}",
                    "anchor_id": f"schedule:{index}",
                    "t0": origin,
                    "treatment": treatment_time is not None and treatment_time <= origin,
                    "trial_weight": float(weights[index]),
                    "windows": _translated(origin, windows),
                    "cutoffs": {
                        name: origin + timedelta(seconds=offset)
                        for name, offset in (cutoffs or {}).items()
                    },
                }
            )
    return pd.DataFrame(rows)


def calendar_panel_rows(
    *,
    units: Sequence[str],
    trial_times: Sequence[datetime],
    treatment_times: Mapping[str, datetime],
    trial_weights: Sequence[float] | None = None,
    windows: Mapping[str, tuple[int, int]] | None = None,
    cutoffs: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """Return the canonical sequential rows for CalendarPanel lowering."""
    return sequential_rows(
        units=units,
        trial_times=trial_times,
        treatment_times=treatment_times,
        trial_weights=trial_weights,
        windows=windows,
        cutoffs=cutoffs,
    )


def event_period_rows(
    events: pd.DataFrame,
    periods: tuple[int, ...],
    period_seconds: int,
    *,
    windows: Mapping[str, tuple[int, int]] | None = None,
    cutoffs: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """Expand parent events into exact-relative-time rows and translated bounds."""
    rows = []
    for event in events.to_dict(orient="records"):
        for period in periods:
            origin = event["t0"] + timedelta(seconds=period * period_seconds)
            rows.append(
                {
                    "unit": event["unit"],
                    "parent_event_id": event["event_id"],
                    "parent_t0": event["t0"],
                    "event_period": period,
                    "anchor_id": (event["event_id"], period),
                    "t0": origin,
                    "windows": _translated(origin, windows),
                    "cutoffs": {
                        name: origin + timedelta(seconds=offset)
                        for name, offset in (cutoffs or {}).items()
                    },
                }
            )
    return pd.DataFrame(rows)


def matched_t0_probabilities(
    risk_set: pd.DataFrame,
    retained: pd.DataFrame,
    *,
    treatment: str,
    strata: tuple[str, ...],
) -> pd.DataFrame:
    """Attach exact treated/control inclusion probabilities within each risk set."""
    eligible_controls: dict[tuple[object, ...], int] = {}
    sampled_controls: dict[tuple[object, ...], int] = {}
    for row in risk_set.to_dict(orient="records"):
        if row[treatment]:
            continue
        key = tuple(row[name] for name in strata)
        eligible_controls[key] = eligible_controls.get(key, 0) + 1
    rows = retained.to_dict(orient="records")
    for row in rows:
        if not row[treatment]:
            key = tuple(row[name] for name in strata)
            sampled_controls[key] = sampled_controls.get(key, 0) + 1
    probabilities = []
    for row in rows:
        if row[treatment]:
            probabilities.append(1.0)
        else:
            key = tuple(row[name] for name in strata)
            probabilities.append(sampled_controls[key] / eligible_controls[key])
    result = retained.copy()
    result["assignment_probability"] = probabilities
    return result
