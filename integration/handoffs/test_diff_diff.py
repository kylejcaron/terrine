from dataclasses import replace

import pyarrow as pa
import pytest

from terrine.export import to_diff_diff

pytestmark = pytest.mark.handoffs


def test_diff_diff_accepts_cohorts_and_event_study_inputs(handoff_frame, library):
    diff_diff = library("diff_diff")
    rows = []
    for unit in range(18):
        adoption = 2 if unit < 6 else 4 if unit < 12 else None
        for period in range(6):
            rows.append(
                {
                    "unit": unit,
                    "anchor_id": unit * 6 + period,
                    "t0": period,
                    "treatment": int(adoption is not None and period >= adoption),
                    "x": float(unit % 2),
                    "y": unit + period + ((unit * period + 3 * unit) % 7) / 10,
                    "complete": True,
                    "reason": None,
                }
            )
    table = pa.Table.from_pylist(rows)
    data = replace(
        handoff_frame,
        table=table,
        roles=replace(handoff_frame.roles, cluster_unit="unit"),
        provenance=replace(handoff_frame.provenance, shape="sequential"),
    )
    handoff = to_diff_diff(data, period_col="t0")
    kwargs = dict(
        outcome="y",
        unit=handoff.unit_col,
        time=handoff.time_col,
        first_treat=handoff.first_treat_col,
    )
    cs = diff_diff.CallawaySantAnna(cluster=data.roles.cluster_unit).fit(handoff.df, **kwargs)
    event_study = cs.aggregate("event_study")
    assert not event_study.to_dataframe().empty
    sa = diff_diff.SunAbraham(cluster=data.roles.cluster_unit).fit(handoff.df, **kwargs)
    for result in (cs, sa):
        assert result.groups == [3, 5]
        assert result.time_periods == [1, 2, 3, 4, 5, 6]
        assert result.n_treated_units == 12
        assert result.n_control_units == 6
        assert result.n_obs == 108
    assert 0 in sa.event_study_effects
