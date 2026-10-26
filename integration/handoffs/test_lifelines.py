from dataclasses import replace
from datetime import timedelta

import pyarrow as pa
import pytest

from terrine.compile.shapes.life_table import aggregate_life_table
from terrine.data import ColumnMeta, Roles
from terrine.export import to_lifelines_cox, to_lifelines_km

pytestmark = pytest.mark.handoffs


def test_lifelines_fits_record_km_cox_and_compressed_risk_counts(handoff_frame, library):
    lifelines = library("lifelines")
    t0 = handoff_frame.table["t0"].to_pylist()
    durations = [3, 7, 2, 9, 6, 4, 10, 8, 5, 11, 12, 13]
    table = handoff_frame.table.append_column(
        "duration", pa.array([timedelta(days=days) for days in durations], type=pa.duration("us"))
    )
    table = table.append_column(
        "entry_time", pa.array([origin + timedelta(days=i % 2) for i, origin in enumerate(t0)])
    )
    table = table.append_column("event", pa.array([i % 3 != 1 for i in range(12)]))
    table = table.append_column(
        "censor_reason",
        pa.array([None if i % 3 != 1 else "administrative_cutoff" for i in range(12)]),
    )
    table = table.append_column("w", pa.array([1 + i % 2 for i in range(12)]))
    data = replace(
        handoff_frame,
        table=table,
        roles=replace(
            handoff_frame.roles, outcomes=("duration", "event"), weight="w", cluster_unit="unit"
        ),
        columns={
            **handoff_frame.columns,
            **{
                name: ColumnMeta(None, "outcome", None, None, "complete", "reason")
                for name in ("duration", "event")
            },
        },
        provenance=replace(handoff_frame.provenance, shape="time_to_event"),
    )
    km = to_lifelines_km(data)
    fitted = lifelines.KaplanMeierFitter().fit(
        km.durations, km.event_observed, entry=km.entry, weights=km.weights
    )
    assert len(fitted.durations) == 12
    assert fitted.weights.sum() == 18
    cox = to_lifelines_cox(data)
    fitter = lifelines.CoxPHFitter().fit(
        cox.df,
        duration_col=cox.duration_col,
        event_col=cox.event_col,
        entry_col=cox.entry_col,
        weights_col=cox.weights_col,
        cluster_col=cox.cluster_col,
    )
    assert fitter.params_.index.tolist() == ["x"]
    assert len(fitter.durations) == 12

    # A separate no-delayed-entry risk table deliberately coarsens times to bins.
    records = table.set_column(
        table.schema.get_field_index("entry_time"), "entry_time", table["t0"]
    )
    risk_table = aggregate_life_table(records, interval_seconds=86400, group_by=[])
    risk = replace(
        data,
        table=risk_table,
        roles=Roles("unit", "anchor_id", "t0", None, ("time_bin",), ("events", "censored")),
        columns={
            name: ColumnMeta(None, "outcome", None, None, "complete", "reason")
            for name in ("events", "censored")
        },
        provenance=replace(data.provenance, shape="life_table"),
    )
    arrays = to_lifelines_km(risk)
    fitted_counts = lifelines.KaplanMeierFitter().fit(
        arrays.durations, arrays.event_observed, weights=arrays.weights
    )
    assert fitted_counts.weights.sum() == 12
    assert fitted_counts.event_observed.sum() > 0
