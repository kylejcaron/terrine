import sys
from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pytest

import terrine as tr
from terrine.data import Budget, Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, EventAnchor, EventSequence
from terrine.windows import Window


def _design(**kwargs):
    return Design("user", EventAnchor("session"), **kwargs)


def _model(native_inputs, *, invalid_timestamp=False):
    catalog, con = native_inputs
    if invalid_timestamp:
        definitions = catalog.model_dump(mode="json")
        definitions["sources"]["events"]["relation"] = {
            "kind": "sql",
            "sql": "SELECT user_id, CAST(event_id AS TIMESTAMPTZ) AS ts FROM events",
        }
        catalog = Catalog.model_validate(definitions)
    return Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-01-31")}
    )


def test_non_utc_execution_preserves_instants_and_exports_utc_batches(native_inputs):
    _, con = native_inputs
    con.raw_sql("SET TimeZone = 'America/New_York'")
    result = _design().build(_model(native_inputs)).materialize()
    assert result.table.schema.field("t0").type == pa.timestamp("us", tz="UTC")
    assert result.table.column("t0").to_pylist() == [datetime(2025, 1, 3, tzinfo=UTC)] * 2
    assert con.raw_sql("SELECT current_setting('TimeZone')").fetchall() == [("America/New_York",)]


def test_default_preview_does_not_evaluate_invalid_source_rows(native_inputs):
    model = _model(native_inputs, invalid_timestamp=True)
    design = _design()
    frame = design.build(model)
    report = tr.preview(design, model)
    assert report.candidate_rows is None
    assert report.counted_rows is None
    assert report.scan_bytes is None
    with pytest.raises(duckdb.ConversionException):
        frame.materialize()


def test_bounded_preview_only_reports_totals_when_complete(native_inputs):
    model = _model(native_inputs)
    capped = tr.preview(_design(), model, bounded_count=1)
    assert capped.count_capped is True
    assert capped.counted_rows == 1
    assert (
        capped.candidate_rows,
        capped.known_false_rows,
        capped.unknown_rows,
        capped.eligible_rows,
    ) == (None, None, None, None)
    exact = tr.preview(_design(), model, bounded_count=2)
    assert exact.count_capped is False
    assert exact.counted_rows == 2
    assert (
        exact.candidate_rows,
        exact.known_false_rows,
        exact.unknown_rows,
        exact.eligible_rows,
    ) == (2, 0, 0, 2)


def test_bounded_preview_caps_the_summary_read_but_not_the_checks(native_inputs, monkeypatch):
    """The eligibility summary reads at most cap + 1 candidates; the integrity
    checks still cover every candidate."""
    import ibis

    from terrine.execution.inputs import ExecutionContext

    executed = []
    original = ExecutionContext.execute

    def execute(self, query):
        executed.append(ibis.to_sql(query, dialect="duckdb"))
        return original(self, query)

    monkeypatch.setattr(ExecutionContext, "execute", execute)
    model = _model(native_inputs)
    design = _design(features={"sessions": tr.agg("sessions", window=("-1d", "0s"))})
    report = tr.preview(design, model, bounded_count=1)
    assert report.count_capped is True and report.counted_rows == 1
    summaries = [sql for sql in executed if "__terrine_gate_candidate" in sql]
    checks = [sql for sql in executed if "__terrine_gate_check_" in sql]
    assert len(summaries) == 1 and "LIMIT 2" in summaries[0]
    assert checks and all("LIMIT" not in sql for sql in checks)


@pytest.mark.parametrize(
    ("budget", "code"),
    [
        (Budget(max_rows=0), "terrine.budget.rows_exceeded"),
        (Budget(max_rows=1), "terrine.budget.rows_exceeded"),
        (Budget(max_output_bytes=1), "terrine.budget.output_bytes_exceeded"),
    ],
)
def test_materialization_aborts_instead_of_returning_partial_data(native_inputs, budget, code):
    frame = _design().build(_model(native_inputs))
    with pytest.raises(TerrineError) as error:
        frame.materialize(budget)
    assert error.value.code == code
    complete = frame.materialize(Budget(max_rows=2))
    assert sorted(complete.table.column("unit").to_pylist()) == ["a", "b"]
    assert complete.provenance.eligibility_summary.retained == 2


def test_sequence_provenance_round_trips_on_bounded_materialization(native_inputs):
    frame = _design(shape=EventSequence("session", Window("0s", "1d"), max_events=10)).build(
        _model(native_inputs)
    )
    materialized = frame.materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
    metadata = materialized.provenance.sequence_meta
    assert metadata.event == "session"
    assert metadata.representation == "positions"
    assert metadata.event_fields == ()
    stamp = (
        materialized.provenance.engine_name,
        materialized.provenance.dialect,
        materialized.provenance.engine_status,
    )
    assert stamp == ("duckdb", "duckdb", "verified")
    restored = type(materialized).from_arrow(materialized.to_arrow())
    assert restored.provenance.sequence_meta == metadata
    assert restored.provenance == materialized.provenance


def test_unresolved_eligibility_refuses_export_with_only_aggregate_counts():
    import ibis

    from terrine.compile.anchors import AnchorTable
    from terrine.compile.columns import CompiledStudy
    from terrine.data import Roles
    from terrine.execution.gates import validate_compiled_output

    con = ibis.duckdb.connect()
    try:
        table = con.create_table(
            "candidates",
            pa.table(
                {
                    "unit": ["a", "b"],
                    "anchor_id": [0, 1],
                    "t0": [datetime(2025, 1, 3, tzinfo=UTC)] * 2,
                    "__terrine_eligible": pa.array([None, None], type=pa.bool_()),
                }
            ),
        )
        compiled = CompiledStudy(
            table,
            table,
            Roles("unit", "anchor_id", "t0", None, (), ()),
            {},
            {},
            {},
            {},
            AnchorTable(table, ()),
            (),
        )
        with pytest.raises(TerrineError) as error:
            validate_compiled_output(compiled, con.to_pyarrow)
    finally:
        con.disconnect()
    assert error.value.code == "terrine.study.eligibility_unknown"
    assert dict(error.value.context) == {
        "candidate": 2,
        "excluded_false": 0,
        "unknown": 2,
        "retained": 0,
    }


def test_strict_scan_policy_precedes_integrity_reads(native_inputs):
    catalog, con = native_inputs
    con.raw_sql("INSERT INTO users VALUES ('a', 'duplicate')")
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": tr.UnitSource("users", "user_id")},
    )
    frame = _design().build(model)
    with pytest.raises(TerrineError) as error:
        frame.materialize(Budget(max_scan_bytes=1))
    assert error.value.code == "terrine.budget.scan_unknown"
    with pytest.raises(TerrineError) as error:
        frame.materialize()
    assert error.value.code == "terrine.compile.anchor_integrity"


def test_missing_optional_conversion_dependency_refuses_before_data(native_inputs, monkeypatch):
    frame = _design().build(_model(native_inputs, invalid_timestamp=True))
    monkeypatch.setitem(sys.modules, "pandas", None)
    with pytest.raises(TerrineError) as error:
        frame.to_pandas()
    assert error.value.code == "terrine.convert.missing_dependency"


@pytest.mark.parametrize("library", ["pandas", "polars"])
def test_dataframe_conversion_selects_compact_or_lossless_columns(
    native_inputs, monkeypatch, library
):
    from terrine.data import ColumnMeta, ExecutionSnapshot, MaterializedFrame, Provenance, Roles
    from terrine.frame import Frame

    materialized = MaterializedFrame(
        pa.table(
            {
                "unit": ["a"],
                "anchor_id": ["a1"],
                "t0": [datetime(2025, 1, 1, tzinfo=UTC)],
                "value": [None],
                "value__complete": [False],
                "value__reason": ["incomplete_observation"],
                "label": [True],
                "label__complete": [True],
                "label__reason": [None],
                "_observed_from": [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        ),
        Roles("unit", "anchor_id", "t0", None, ("value",), ("label",)),
        {
            "value": ColumnMeta(
                None, "feature", "_observed_from", None, "value__complete", "value__reason"
            ),
            "label": ColumnMeta(None, "outcome", None, None, "label__complete", "label__reason"),
        },
        Provenance(
            {},
            {},
            "design",
            "model",
            {},
            ExecutionSnapshot(None, "duckdb", {}, None, "unverified"),
            "cross_section",
            None,
            engine_name="duckdb",
            dialect="duckdb",
            engine_status="verified",
        ),
    )
    frame = _design().build(_model(native_inputs))
    monkeypatch.setattr(Frame, "materialize", lambda self, budget=None: materialized)
    convert = getattr(frame, f"to_{library}")
    compact = convert()
    lossless = convert(include_observation_metadata=True)
    assert list(compact.columns) == [
        "unit",
        "anchor_id",
        "t0",
        "value",
        "value__complete",
        "value__reason",
        "label",
        "label__complete",
        "label__reason",
    ]
    assert list(lossless.columns) == materialized.table.column_names
    if library == "pandas":
        assert compact["value"].isna().all()
        assert compact["value__reason"].tolist() == ["incomplete_observation"]
    else:
        assert compact["value"].null_count() == 1
        assert compact["value__reason"].to_list() == ["incomplete_observation"]


def test_empty_cohort_has_zero_eligibility_totals(native_inputs):
    _, con = native_inputs
    con.raw_sql("DELETE FROM events")
    model = _model(native_inputs)
    frame = _design().build(model)

    report = tr.preview(_design(), model, bounded_count=1)
    assert (
        report.candidate_rows,
        report.known_false_rows,
        report.unknown_rows,
        report.eligible_rows,
    ) == (0, 0, 0, 0)

    result = frame.materialize()
    assert result.table.num_rows == 0
    summary = result.provenance.eligibility_summary
    assert summary is not None
    assert (
        summary.candidate,
        summary.excluded_false,
        summary.unknown,
        summary.retained,
    ) == (0, 0, 0, 0)
    assert result.table.schema.equals(frame.expr.schema().to_pyarrow())


def test_shadowed_native_function_in_compiled_query_is_refused_before_it_runs(
    native_inputs, tmp_path
):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": tr.UnitSource("users", "user_id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    path = tmp_path / "ordinals.csv"
    path.write_text("ordinal\n0\n1\n")
    calls = []
    con.con.create_function(
        "probe_path",
        lambda: calls.append(True) or str(path),
        return_type=duckdb.sqltype("VARCHAR"),
    )
    con.raw_sql("CREATE MACRO range(lo, hi) AS (SELECT list(ordinal) FROM read_csv(probe_path()))")
    calls.clear()
    design = Design("user", tr.Schedule("1d", "2025-01-03", "2025-01-04"))
    frame = design.build(model)  # compiling sends no SQL
    with pytest.raises(TerrineError) as error:
        frame.materialize()
    assert error.value.code == "terrine.execution.snapshot_unavailable"
    assert calls == []
