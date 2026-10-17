import sys
from _thread import LockType, RLock
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Event

import duckdb
import pyarrow as pa
import pytest

import terrine as tr
from terrine.data import Budget, Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, EventAnchor, agg, gt


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


@pytest.mark.slow
def test_many_window_columns_compile_and_export_together(native_inputs):
    outcomes = {
        f"sessions_{days}d": agg("sessions", window=("0d", f"{days}d")) for days in range(1, 7)
    }
    result = _design(outcomes=outcomes).build(_model(native_inputs)).materialize()
    rows = result.table.sort_by("unit").to_pylist()
    assert [row["unit"] for row in rows] == ["a", "b"]
    assert all(row[name] == 1 for row in rows for name in outcomes)
    assert all(row[result.columns[name].complete_column] for row in rows for name in outcomes)


def test_non_utc_execution_preserves_instants_and_exports_utc_batches(native_inputs):
    _, con = native_inputs
    con.raw_sql("SET TimeZone = 'America/New_York'")
    result = _design().build(_model(native_inputs)).materialize()
    assert result.table.schema.field("t0").type == pa.timestamp("us", tz="UTC")
    assert result.table.column("t0").to_pylist() == [datetime(2025, 1, 3, tzinfo=UTC)] * 2
    assert con.raw_sql("SELECT current_setting('TimeZone')").fetchall() == [("America/New_York",)]


def test_shared_model_concurrent_exports_keep_independent_results(native_inputs):
    frame = _design().build(_model(native_inputs))
    origin = native_inputs[1].con
    execute, fetchall = origin.execute.__func__, origin.fetchall.__func__
    executed, release, fetched = Event(), Event(), Event()

    def export(first):
        paused = False

        def interleave(_frame, event, operation):
            nonlocal paused
            if first:
                if event == "c_return" and operation is execute and not paused:
                    paused = True
                    executed.set()
                    assert release.wait(timeout=30)
                elif event == "c_return" and operation is fetchall:
                    fetched.set()
            elif not release.is_set() and (
                (event == "c_return" and operation is fetchall)
                or (event == "return" and isinstance(operation, (LockType, RLock)))
            ):
                # Allow serialization, or force the competing result to be consumed
                # before the first export fetches. No replacement database is used.
                release.set()
                assert fetched.wait(timeout=30)

        sys.setprofile(interleave)
        try:
            return frame.materialize().table.select(["unit", "t0"]).sort_by("unit")
        finally:
            sys.setprofile(None)
            release.set()
            fetched.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(export, True)
        assert executed.wait(timeout=30)
        second = pool.submit(export, False)
        results = [future.result(timeout=120) for future in (first, second)]
    expected = [{"unit": unit, "t0": datetime(2025, 1, 3, tzinfo=UTC)} for unit in ("a", "b")]
    assert all(result.to_pylist() == expected for result in results)


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


def test_unresolved_eligibility_refuses_export_with_only_aggregate_counts(native_inputs):
    frame = _design(eligibility={"history": gt(agg("sessions", window=("-7d", "0d")), 0)}).build(
        _model(native_inputs)
    )
    with pytest.raises(TerrineError) as error:
        frame.materialize()
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


@pytest.mark.slow
def test_declared_boolean_baseline_reaches_estimator_admission(native_inputs):
    from terrine.estimate._data import prepare

    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE events ADD COLUMN enabled BOOLEAN")
    con.raw_sql("UPDATE events SET enabled = (user_id = 'a')")
    definitions = catalog.model_dump(mode="json")
    definitions["sources"]["events"]["properties"] = ["enabled"]
    definitions["properties"]["enabled"] = {
        "source": "events",
        "column": "enabled",
        "dtype": "bool",
        "as_of": "event_time",
    }
    model = Model.from_catalog(
        Catalog.model_validate(definitions),
        con=con,
        units={"user": tr.UnitSource("users", "user_id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )

    def build(known_at_t0):
        return (
            Design(
                "user",
                tr.FixedDate("2025-01-04"),
                treatment=tr.Treatment("enabled", known_at_t0=known_at_t0),
                outcomes={"future": agg("sessions", window=("0s", "1d"))},
            )
            .build(model)
            .materialize()
        )

    declared = prepare(build(True))
    assert dict(zip(declared.units, declared.a, strict=True)) == {"a": 1.0, "b": 0.0}
    assert declared.y.tolist() == [0.0, 0.0]
    with pytest.raises(TerrineError) as error:
        prepare(build(False))
    assert error.value.code == "terrine.estimate.unsupported_design"


def test_compiler_sql_refuses_shadowed_functions_before_schema_binding(native_inputs, tmp_path):
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
    design = Design("user", tr.Schedule("1d", "2025-01-03", "2025-01-05"))
    for operation in (lambda: design.build(model), lambda: tr.preview(design, model)):
        with pytest.raises(TerrineError) as error:
            operation()
        assert error.value.code == "terrine.execution.snapshot_unavailable"
        assert calls == []
