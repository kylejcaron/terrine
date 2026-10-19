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
        relation = con.create_table("dataset", table)
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
