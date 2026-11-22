from dataclasses import replace

import ibis
import pyarrow as pa
import pytest

from terrine.errors import TerrineError
from terrine.export import preflight


@pytest.mark.parametrize("drop", [False, True])
@pytest.mark.parametrize(
    "reasons",
    [
        ["incomplete_coverage", None],
        [["incomplete_coverage", "incomplete_observation", "incomplete_coverage"], []],
    ],
)
def test_warehouse_preflight_matches_arrow_exclusions(frame, drop, reasons):
    table = frame.table.set_column(6, "complete", pa.array([False, True])).set_column(
        7, "reason", pa.array(reasons)
    )
    frame = replace(
        frame,
        table=table,
        roles=replace(frame.roles, outcomes=("x", "y")),
        columns={**frame.columns, "x": frame.columns["y"]},
    )
    con = ibis.duckdb.connect()
    try:
        relation = con.create_table("dataset", frame.table)
        if not drop:
            with pytest.raises(TerrineError) as error:
                with preflight.preflight_warehouse(
                    relation, roles=frame.roles, columns=frame.columns, provenance=frame.provenance
                ):
                    raise AssertionError("incomplete export was accepted")
            assert error.value.code == "terrine.export.incomplete_outcome"
            return
        expected = preflight.preflight_export(frame, drop_incomplete=True)
        with preflight.preflight_warehouse(
            relation,
            roles=frame.roles,
            columns=frame.columns,
            provenance=frame.provenance,
            drop_incomplete=True,
        ) as checked:
            assert checked.excluded_counts == expected.excluded_counts
            assert checked.table.to_pyarrow().equals(expected.table)
    finally:
        con.disconnect()


def test_warehouse_preflight_keeps_scoring_rows_without_outcomes(frame):
    frame = replace(
        frame,
        table=frame.table.select(["id", "anchor", "t0", "x"]),
        roles=replace(frame.roles, treatment=None, outcomes=()),
        columns={"x": frame.columns["x"]},
    )
    con = ibis.duckdb.connect()
    try:
        relation = con.create_table("dataset", frame.table)
        with preflight.preflight_warehouse(
            relation, roles=frame.roles, columns=frame.columns, provenance=frame.provenance
        ) as checked:
            assert checked.excluded_counts == {}
            assert checked.table.to_pyarrow().equals(frame.table)
    finally:
        con.disconnect()


def test_warehouse_snapshot_spans_evidence_and_consumer(frame, tmp_path):
    path = str(tmp_path / "dataset.duckdb")
    con = ibis.duckdb.connect(path)
    writer = ibis.duckdb.connect(path)
    try:
        relation = con.create_table("dataset", frame.table)
        with preflight.preflight_warehouse(
            relation, roles=frame.roles, columns=frame.columns, provenance=frame.provenance
        ) as checked:
            writer.raw_sql("UPDATE dataset SET complete = false, y = NULL")
            assert checked.table.aggregate(total=checked.table.y.sum()).execute().total[0] == 3.0
        assert relation.filter(~relation.complete).count().execute() == 2
    finally:
        writer.disconnect()
        con.disconnect()


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ("cluster", "terrine.export.cluster_required"),
        ("grace", "terrine.export.grace_baseline_treatment"),
        ("eligibility", "terrine.export.unknown_eligibility"),
    ],
)
def test_warehouse_uses_shared_hazard_codes(frame, change, code):
    from terrine.data import EligibilitySummary

    if change == "cluster":
        frame = replace(frame, table=frame.table.take([0, 0]))
    elif change == "grace":
        frame = replace(
            frame, provenance=replace(frame.provenance, treatment_design="grace_initiation")
        )
    else:
        frame = replace(
            frame,
            provenance=replace(
                frame.provenance, eligibility_summary=EligibilitySummary(2, 0, 1, 1)
            ),
        )
    con = ibis.duckdb.connect()
    try:
        relation = con.create_table("dataset", frame.table)
        with pytest.raises(TerrineError) as error:
            with preflight.preflight_warehouse(
                relation, roles=frame.roles, columns=frame.columns, provenance=frame.provenance
            ):
                raise AssertionError("unsafe export was accepted")
        assert error.value.code == code
    finally:
        con.disconnect()


@pytest.mark.parametrize("backend", ["postgres", "bigquery", "snowflake", "unregistered"])
def test_warehouse_snapshot_refuses_other_backends_before_execution(frame, monkeypatch, backend):
    class NoQueries:
        name = backend

        def raw_sql(self, *args, **kwargs):
            raise pytest.fail.Exception("warehouse export queried an unsupported backend")

    monkeypatch.setattr(ibis, "get_backend", lambda table: NoQueries())
    with pytest.raises(TerrineError) as caught:
        with preflight.preflight_warehouse(
            ibis.table(frame.table.schema),
            roles=frame.roles,
            columns=frame.columns,
            provenance=frame.provenance,
        ):
            raise AssertionError("warehouse export accepted an unsupported backend")
    assert caught.value.code == "terrine.export.snapshot_required"
    assert dict(caught.value.context) == {}


def _calendar_preflight(frame, con, *, paired, drop_incomplete=False):
    from terrine.export import CalendarContrast

    contrast = CalendarContrast(
        outcome="y",
        cohort=frame.table["cohort"][0].as_py(),
        reference_period=-1,
        comparison_periods=(0,),
        paired=paired,
    )
    if con is None:
        return preflight.preflight_export(
            frame, calendar_contrast=contrast, drop_incomplete=drop_incomplete
        ).calendar_support
    relation = con.create_table("calendar_export", frame.table, temp=True, overwrite=True)
    with preflight.preflight_warehouse(
        relation,
        roles=frame.roles,
        columns=frame.columns,
        provenance=frame.provenance,
        calendar_contrast=contrast,
        drop_incomplete=drop_incomplete,
    ) as checked:
        return checked.calendar_support


@pytest.fixture(params=["arrow", "warehouse"])
def contrast_connection(request):
    if request.param == "arrow":
        yield None
    else:
        con = ibis.duckdb.connect()
        try:
            yield con
        finally:
            con.disconnect()


def _with_treated_followup(table, value, reason=None):
    (index,) = (
        i
        for i, (treated, period) in enumerate(
            zip(table["treatment"].to_pylist(), table["event_period"].to_pylist(), strict=True)
        )
        if treated and period == 0
    )
    y, reasons = table["y"].to_pylist(), table["reason"].to_pylist()
    y[index], reasons[index] = value, reason
    return table.set_column(table.schema.get_field_index("y"), "y", pa.array(y)).set_column(
        table.schema.get_field_index("reason"), "reason", pa.array(reasons, pa.string())
    )


def test_calendar_contrast_counts_outcome_null_and_below_resolution_differently(
    calendar_frame, contrast_connection
):
    from copy import deepcopy
    from pickle import dumps, loads

    frame = calendar_frame
    comparison = frame.provenance.calendar_comparison
    assert comparison is not None and comparison.support is not None
    assert [(cell.period, cell.treated, cell.control) for cell in comparison.support] == [
        (-1, 1, 1),
        (0, 1, 1),
    ]
    updated = _with_treated_followup(frame.table, 0.0, "below_resolution")
    for paired in (False, True):
        with pytest.raises(TerrineError) as caught:
            _calendar_preflight(frame, contrast_connection, paired=paired)
        refusal = caught.value
        assert refusal.code == "terrine.export.calendar_contrast_unsupported"
        expected = {
            "outcome": "y",
            "cohort": frame.table["cohort"][0].as_py().isoformat(),
            "reference_period": -1,
            "comparison_period": 0,
            "period": 0,
            "arm": "treated",
            "reason": "empty_margin",
            "treated": (1, 0),
            "control": (1, 1),
            "treated_pairs": 0,
            "control_pairs": 1,
        }
        for error in (refusal, deepcopy(refusal), loads(dumps(refusal))):
            assert error.code == refusal.code
            assert {name: error.context[name] for name in expected} == expected
            assert error.context["route"]
        support = _calendar_preflight(
            replace(frame, table=updated), contrast_connection, paired=paired
        )
        assert support is not None
        (cell,) = support
        assert cell.comparison_period == 0
        assert cell.treated == cell.control == (1, 1)
        assert cell.treated_pairs == cell.control_pairs == 1


def test_calendar_contrast_disjoint_units_pass_marginal_but_not_paired(
    calendar_frame, contrast_connection
):
    # A different treatment unit follows up, and duplicate rows don't add support.
    frame = calendar_frame
    table = _with_treated_followup(frame.table, 4.0)
    table = table.set_column(0, "id", pa.array(["C", "C", "T", "T2"])).take([0, 0, 1, 2, 3])
    frame = replace(frame, table=table)
    support = _calendar_preflight(frame, contrast_connection, paired=False)
    assert support is not None
    (cell,) = support
    assert cell.treated == cell.control == (1, 1)
    assert (cell.treated_pairs, cell.control_pairs) == (0, 1)
    with pytest.raises(TerrineError) as caught:
        _calendar_preflight(frame, contrast_connection, paired=True)
    assert caught.value.code == "terrine.export.calendar_contrast_unsupported"
    assert caught.value.context["reason"] == "empty_pair"
    assert caught.value.context["arm"] == "treated"


def test_calendar_contrast_does_not_pool_controls_or_count_ineligible_rows(
    calendar_frame, contrast_connection
):
    from datetime import timedelta

    frame = calendar_frame
    cohort = frame.table["cohort"][0].as_py()
    other = cohort + timedelta(days=1)
    table = _with_treated_followup(frame.table, 4.0).take([0, 1, 2, 3, 1])
    table = (
        table.set_column(
            table.schema.get_field_index("cohort"), "cohort", pa.array([cohort] * 4 + [other])
        )
        .set_column(
            table.schema.get_field_index("control_eligible"),
            "control_eligible",
            pa.array([True, False, False, False, True]),
        )
    )
    with pytest.raises(TerrineError) as caught:
        _calendar_preflight(replace(frame, table=table), contrast_connection, paired=False)
    assert caught.value.code == "terrine.export.calendar_contrast_unsupported"
    assert caught.value.context["control"] == (1, 0)
    assert caught.value.context["arm"] == "control"


def test_calendar_contrast_uses_rows_after_all_outcome_exclusions(
    calendar_frame, contrast_connection
):
    frame = calendar_frame
    table = _with_treated_followup(frame.table, 4.0).append_column(
        "x_complete", pa.array([True, True, True, False])
    )
    frame = replace(
        frame,
        table=table,
        columns={**frame.columns, "x": replace(frame.columns["x"], complete_column="x_complete")},
    )
    with pytest.raises(TerrineError) as caught:
        _calendar_preflight(frame, contrast_connection, paired=False, drop_incomplete=True)
    assert caught.value.code == "terrine.export.calendar_contrast_unsupported"
    assert caught.value.context["arm"] == "treated"
    assert caught.value.context["treated"] == (1, 0)
