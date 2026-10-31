from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import ibis
import pyarrow as pa
import pytest

from terrine.data import Coverage, RowCheckGroup, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import (
    Design,
    FixedDate,
    Schedule,
    agg,
    asof,
    eq,
    event_time,
    is_null,
)
from terrine.spec.expressions import since


@pytest.fixture
def column_model():
    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE events AS SELECT user_id, ts::TIMESTAMPTZ AS ts,
            amount::BIGINT AS amount, tier::VARCHAR AS tier
        FROM (VALUES
            ('a', '2025-01-02 11:59:59+00', 3, 'old'),
            ('a', '2025-01-02 12:00:00+00', 7, 'new'),
            ('b', '2025-01-02 11:59:59+00', NULL, NULL)
        ) AS data(user_id, ts, amount, tier)
    """)
    con.raw_sql("CREATE TABLE users AS SELECT * FROM (VALUES ('a'), ('b')) AS u(id)")
    catalog = Catalog.model_validate(
        {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["amount"],
                    "properties": ["tier"],
                    "dimensions": [],
                }
            },
            "events": {"amount": {"source": "events", "column": "amount"}},
            "properties": {
                "tier": {
                    "source": "events",
                    "column": "tier",
                    "dtype": "string",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {"total": {"event": "amount", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "unit": "user", "measure": "total"}},
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-02")},
    )
    try:
        yield model
    finally:
        con.disconnect()


def _compile(model, **kwargs):
    from terrine.compile.columns import compile_columns

    return compile_columns(
        Design("user", FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)), **kwargs), model
    )


@pytest.mark.parametrize("bound", ["source", "unit"])
@pytest.mark.parametrize("value", ["old", None])
def test_carried_inline_state_before_observation_start_is_unknown(column_model, bound, value):
    con = column_model.con
    con.raw_sql("DELETE FROM events")
    con.raw_sql(
        "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2024-12-31', 1, "
        + ("NULL" if value is None else "'old'")
        + ")"
    )
    if bound == "unit":
        con.raw_sql("ALTER TABLE users ADD COLUMN observed_from TIMESTAMPTZ")
        con.raw_sql("UPDATE users SET observed_from = TIMESTAMPTZ '2025-01-01'")
        column_model = Model.from_catalog(
            column_model.catalog,
            con=con,
            readiness={"events": Coverage("2024-12-01", "2025-01-02")},
            units={"user": UnitSource("users", "id", observed_from="observed_from")},
        )
    compiled = _compile(
        column_model,
        features={"state": asof("tier", before="0s")},
        eligibility={"is_missing": is_null(asof("tier", before="0s"))},
    )
    row = con.to_pyarrow(compiled.audit_table.filter(compiled.audit_table.unit == "a")).to_pylist()[
        0
    ]
    meta = compiled.columns["state"]
    assert row["state"] is None
    assert row[meta.complete_column] is False
    assert row[meta.reason_column] == (
        "missing_history" if bound == "source" else "incomplete_observation"
    )
    assert row["is_missing"] is None


@pytest.mark.parametrize("names", [("same", "SAME"), ("é", "e\u0301")])
def test_output_namespace_collisions_refuse_before_compilation(column_model, names):
    with pytest.raises(TerrineError) as error:
        _compile(
            column_model,
            features={names[0]: asof("tier", before="0s")},
            eligibility={names[1]: is_null(asof("tier", before="0s"))},
        )
    assert error.value.code == "terrine.spec.output_name_conflict"


@pytest.mark.parametrize("name", ["UNIT", "treatment", "__Terrine_private"])
def test_reserved_output_names_refuse(column_model, name):
    with pytest.raises(TerrineError) as error:
        _compile(column_model, features={name: asof("tier", before="0s")})
    assert error.value.code == "terrine.spec.reserved_output_name"


def test_time_series_columns_are_values_with_named_support(column_model):
    frame = (
        Design(
            "user",
            Schedule("1d", "2025-01-02", "2025-01-03"),
            features={"rev_14d": agg("total", window=("-14d", "0d"))},
        )
        .build(column_model)
        .materialize()
    )
    full = frame.to_pyarrow()
    compact = full.drop(frame._bookkeeping_columns())
    assert compact.column_names == [
        "unit",
        "anchor_id",
        "t0",
        "rev_14d",
        "rev_14d__complete",
        "rev_14d__reason",
    ]
    assert "_observed_from" in full.column_names
    assert set(compact.column_names) < set(full.column_names)
    assert frame.columns["rev_14d"].complete_column == "rev_14d__complete"


@pytest.mark.parametrize("field", ["features", "outcomes", "eligibility"])
@pytest.mark.parametrize(
    "name",
    [
        "revenue__complete",
        "revenue__reason",
        "x__REASON",
        "anchor_provenance",
        "_observed_from",
        "_observed_until",
    ],
)
def test_reserved_support_suffix_refuses(field, name):
    import copy
    import pickle

    with pytest.raises(TerrineError) as error:
        nodes = {name: is_null(asof("tier", before="0s"))}
        Design(
            "user",
            FixedDate("2025-01-01"),
            features=nodes if field == "features" else None,
            outcomes=nodes if field == "outcomes" else None,
            eligibility=nodes if field == "eligibility" else None,
        )
    for restored in (
        error.value,
        copy.deepcopy(error.value),
        pickle.loads(pickle.dumps(error.value)),
    ):
        assert restored.code == "terrine.spec.reserved_output_name"
        assert restored.context["name"] == name


def test_reobserved_null_is_known_and_duplicate_inline_observations_dedupe(column_model):
    column_model.con.raw_sql("INSERT INTO events SELECT * FROM events WHERE user_id = 'b'")
    column_model.con.raw_sql("""
        INSERT INTO events VALUES ('b', TIMESTAMPTZ '2024-12-31', 1, 'stale')
    """)
    compiled = _compile(
        column_model, eligibility={"known_null": is_null(asof("tier", before="0s"))}
    )
    compiled.validate(execute=column_model.con.to_pyarrow)
    rows = column_model.con.to_pyarrow(compiled.audit_table.order_by("unit")).to_pylist()
    assert [r["known_null"] for r in rows] == [False, True]
    assert column_model.con.to_pyarrow(compiled.table).num_rows == 1


def test_conflicting_inline_history_is_refused_even_for_false_eligibility(column_model):
    column_model.con.raw_sql("""
        INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-02 11:59:59+00', 3, 'conflict')
    """)
    compiled = _compile(
        column_model, eligibility={"eligible": eq(asof("tier", before="0s"), "absent")}
    )
    with pytest.raises(TerrineError) as error:
        compiled.validate(execute=column_model.con.to_pyarrow)
    assert error.value.code == "terrine.temporal.duplicate_history"


def test_row_checks_are_lifted_into_the_audit_at_their_schedule_position(column_model):
    from terrine.temporal.aggregate import TemporalResult

    compiled = _compile(
        column_model,
        features={"state": asof("tier", before="0s"), "total": agg("total", window=("-1d", "0s"))},
    )
    kinds = [type(check).__name__ for check in compiled._checks]
    assert kinds.index("AsOfResult") < kinds.index("RowCheckGroup")
    group = next(check for check in compiled._checks if isinstance(check, RowCheckGroup))
    assert group.checks and group.table.equals(compiled.audit_table)
    assert all(
        check.column.startswith("__terrine_check_") and check.column in compiled.audit_table.columns
        for check in group.checks
    )
    assert not any(check.column in compiled.table.columns for check in group.checks)
    assert not any(isinstance(check, TemporalResult) for check in compiled._checks)


def test_compiled_study_rejects_a_row_check_outside_its_relation(column_model):
    compiled = _compile(column_model, features={"total": agg("total", window=("-1d", "0s"))})
    group = next(check for check in compiled._checks if isinstance(check, RowCheckGroup))
    bad = RowCheckGroup(compiled.audit_table.drop(group.checks[0].column), group.checks)
    with pytest.raises(ValueError):
        replace(compiled, _checks=(bad,))


def test_row_check_counts_survive_a_later_unvalidated_join(column_model):
    """A failing window registered before an as-of lookup whose static history
    holds two values for a unit reports its own failed count: the lookup's
    duplicate rows belong to its own, later, refusal."""
    con = column_model.con
    con.raw_sql("ALTER TABLE events ADD COLUMN weight DOUBLE")
    con.raw_sql("UPDATE events SET weight = CASE WHEN tier = 'old' THEN 'NaN'::DOUBLE ELSE 1 END")
    definition = column_model.catalog.model_dump(mode="python")
    definition["properties"]["tier"]["as_of"] = "static"
    source_events = definition["sources"]["events"]["events"]
    definition["sources"]["events"]["events"] = (*source_events, "weight")
    definition["events"]["weight"] = {"source": "events", "column": "weight"}
    definition["measures"]["weight_total"] = {"event": "weight", "aggregation": "sum"}
    definition["metrics"]["weight_total"] = {
        "kind": "mean",
        "unit": "user",
        "measure": "weight_total",
    }
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    # Outputs register in name order: the window precedes the lookup.
    compiled = _compile(
        model,
        features={
            "a_weight": agg("weight_total", window=("-1d", "0s")),
            "b_tier": asof("tier", before="0s"),
        },
    )
    assert type(compiled._checks[-1]).__name__ == "AsOfResult"
    with pytest.raises(TerrineError) as error:
        compiled.validate(execute=con.to_pyarrow)
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failed_count"] == 1


@pytest.mark.parametrize("authority", ["range", "changes"])
def test_explicit_validity_can_cover_gap_but_derived_change_interval_cannot(
    column_model, authority
):
    con = column_model.con
    con.raw_sql("""
        CREATE TABLE tier_history AS SELECT 'a' AS id, 'gold' AS tier,
            TIMESTAMPTZ '2024-12-01' AS starts, TIMESTAMPTZ '2025-02-01' AS ends
    """)
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["dimensions"] = ("history",)
    definition["properties"]["tier"]["dimension"] = "history"
    definition["dimensions"]["history"] = {
        "relation": {"kind": "table", "name": "tier_history"},
        "unit": "user",
        "key": "id",
        "properties": ("tier",),
        "validity": {"valid_from": "starts", "valid_to": "ends"}
        if authority == "range"
        else {"changed_at": "starts"},
    }
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    compiled = _compile(model, features={"state": asof("tier", before="0s")})
    compiled.validate(execute=con.to_pyarrow)
    row = con.to_pyarrow(compiled.table.filter(compiled.table.unit == "a")).to_pylist()[0]
    meta = compiled.columns["state"]
    assert row["state"] == ("gold" if authority == "range" else None)
    assert row[meta.complete_column] is (authority == "range")
    assert row[meta.reason_column] == (None if authority == "range" else "missing_history")


@pytest.mark.parametrize("validity", ["static", "range"])
def test_dimension_admission_reads_only_the_columns_the_lookup_uses(column_model, validity):
    """The relations a dimension lookup submits for admission are the ones that
    execute, the lookup and its validation queries projected to their columns,
    so an unused decimal column of the dimension table does not demand a decimal
    witness; the validation relations are exactly the queries ``validate`` runs."""
    from terrine.compile.columns import _HistoryValidation
    from terrine.execution._generic import _needs_decimal

    con = column_model.con
    con.raw_sql("""
        CREATE TABLE tier_history AS SELECT 'a' AS id, 'gold' AS tier,
            TIMESTAMPTZ '2024-12-01' AS starts, TIMESTAMPTZ '2025-02-01' AS ends,
            12.50::DECIMAL(12, 2) AS balance
    """)
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["dimensions"] = ("history",)
    definition["properties"]["tier"]["dimension"] = "history"
    definition["properties"]["tier"]["as_of"] = "static" if validity == "static" else "event_time"
    definition["dimensions"]["history"] = {
        "relation": {"kind": "table", "name": "tier_history"},
        "unit": "user",
        "key": "id",
        "properties": ("tier",),
        **(
            {}
            if validity == "static"
            else {"validity": {"valid_from": "starts", "valid_to": "ends"}}
        ),
    }
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    compiled = _compile(model, features={"state": asof("tier", before="0s")})
    assert all(not _needs_decimal(relation) for relation in compiled.reads())
    executed = []

    def recording(query):
        executed.append(ibis.to_sql(query, dialect="duckdb"))
        return con.to_pyarrow(query)

    from terrine.compile._asof import AsOfResult

    for check in compiled._checks:
        if isinstance(check, (_HistoryValidation, AsOfResult)):
            executed.clear()
            check.validate(recording)
            # An as-of result also admits its lookup, which validation does not run.
            validations = check.reads()[1:] if isinstance(check, AsOfResult) else check.reads()
            assert executed == [ibis.to_sql(relation, dialect="duckdb") for relation in validations]
            assert len(executed) >= 2
    row = con.to_pyarrow(compiled.table.filter(compiled.table.unit == "a")).to_pylist()[0]
    assert row["state"] == "gold"


def test_static_inline_conflicts_refuse_instead_of_choosing_one_value(column_model):
    definition = column_model.catalog.model_dump(mode="python")
    definition["properties"]["tier"]["as_of"] = "static"
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=column_model.con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    compiled = _compile(model, features={"state": asof("tier", before="0s")})
    with pytest.raises(TerrineError) as error:
        compiled.validate(execute=column_model.con.to_pyarrow)
    assert error.value.code == "terrine.temporal.static_key_multiplicity"


def test_event_time_selector_and_relative_metric_execute(column_model):
    column_model.con.raw_sql("""
        INSERT INTO events VALUES
            ('a', TIMESTAMPTZ '2025-01-02 12:00:01+00', NULL, NULL)
    """)
    selector = event_time("amount", window=("0s", "1s"))
    ordinary = agg("total", window=("1s", "2s"))
    relative = agg("total", window=("1s", "2s"), relative_to=selector)
    compiled = _compile(
        column_model,
        outcomes={
            "selected_at": selector,
            "ordinary_amount": ordinary,
            "selected_amount": relative,
        },
    )
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["selected_at"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[0]["ordinary_amount"] is None
    assert rows[0]["selected_amount"] is None
    amount_meta = compiled.columns["ordinary_amount"]
    relative_meta = compiled.columns["selected_amount"]
    ordinary_state = (
        rows[0]["ordinary_amount"],
        rows[0][amount_meta.complete_column],
        rows[0][amount_meta.reason_column],
    )
    relative_state = (
        rows[0]["selected_amount"],
        rows[0][relative_meta.complete_column],
        rows[0][relative_meta.reason_column],
    )
    assert ordinary_state == relative_state
    assert ordinary_state == (None, True, "all_values_null")
    timing = compiled.expression_timings["selected_amount"]
    assert timing.kind == "event_time"
    assert timing.selector_ref is not None
    assert timing.relative_offsets == (1, 2)
    assert timing.support_seconds == (1, 3)
    assert timing.selector_support_seconds[timing.selector_ref] == (0, 1)
    assert timing.point_time_column in rows[0]
    assert timing.start_bound_column in rows[0]
    assert timing.end_bound_column in rows[0]
    assert rows[0][timing.point_time_column] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[0][timing.start_bound_column] == datetime(2025, 1, 2, 12, 0, 1, tzinfo=UTC)
    assert rows[0][timing.end_bound_column] == datetime(2025, 1, 2, 12, 0, 2, tzinfo=UTC)
    assert compiled.feature_cutoffs == {}


def test_relative_feature_cutoff_uses_absolute_support_and_selector_refs(column_model):
    first = event_time("amount", window=("-2d", "-1d"))
    second = event_time("amount", window=("0s", "1h"), relative_to=first)
    compiled = _compile(
        column_model,
        features={"relative_metric": agg("total", window=("0s", "1h"), relative_to=second)},
    )
    timing = compiled.expression_timings["relative_metric"]
    cutoff = compiled.feature_cutoffs["relative_metric"]
    assert timing.support_seconds == (-172800, -79200)
    assert len(timing.selector_dependencies) == 2
    assert len(timing.selector_support_seconds) == 2
    assert timing.selector_ref == repr(second)
    assert cutoff.latest_seconds == -79200
    row = column_model.con.to_pyarrow(compiled.table).to_pylist()[0]
    assert timing.point_time_column in row
    assert timing.start_bound_column in row
    assert timing.end_bound_column in row


def test_reused_event_time_chain_allocates_unique_support_columns(column_model):
    first = event_time("amount", window=("0s", "1s"))
    second = event_time("amount", window=("0s", "1s"), relative_to=first)
    compiled = _compile(
        column_model,
        outcomes={
            "first_time": first,
            "second_time": second,
            "relative_amount": agg("total", window=("0s", "1s"), relative_to=second),
        },
    )
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["first_time"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[0]["second_time"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[0]["relative_amount"] == Decimal(7)


def test_event_time_selector_requires_source_readiness_for_its_full_search_window(column_model):
    selector = event_time("amount", window=("2d", "3d"))
    compiled = _compile(column_model, outcomes={"selected_at": selector})
    compiled.validate(execute=column_model.con.to_pyarrow)
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    meta = compiled.columns["selected_at"]
    assert meta.complete_column is not None
    assert meta.reason_column is not None
    assert all(row[meta.complete_column] is False for row in rows)
    assert all(row[meta.reason_column] == "incomplete_coverage" for row in rows)


def test_event_time_selector_resolves_dimension_backed_label_property(column_model):
    con = column_model.con
    con.raw_sql("""
        INSERT INTO events VALUES
            ('b', TIMESTAMPTZ '2025-01-02 12:00:01+00', 9, NULL)
    """)
    con.raw_sql("""
        CREATE TABLE tier_history AS SELECT 'a' AS id, 'gold' AS tier,
            TIMESTAMPTZ '2024-12-01' AS starts, TIMESTAMPTZ '2025-02-01' AS ends
    """)
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["dimensions"] = ("history",)
    definition["properties"]["tier"]["dimension"] = "history"
    definition["dimensions"]["history"] = {
        "relation": {"kind": "table", "name": "tier_history"},
        "unit": "user",
        "key": "id",
        "properties": ("tier",),
        "validity": {"valid_from": "starts", "valid_to": "ends"},
    }
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    selector = event_time("amount", window=("0s", "2s"), label_property="tier", equals="gold")
    nth_two = event_time(
        "amount",
        window=("0s", "2s"),
        label_property="tier",
        equals="gold",
        occurrence="nth",
        n=2,
    )
    compiled = _compile(model, outcomes={"selected_at": selector, "nth_two": nth_two})
    compiled.validate(execute=con.to_pyarrow)
    rows = con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["selected_at"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[1]["selected_at"] is None
    label_meta = compiled.columns["selected_at"]
    assert label_meta.complete_column is not None and label_meta.reason_column is not None
    assert rows[1][label_meta.complete_column] is False
    assert rows[1][label_meta.reason_column] == "incomplete_label_history"
    nth_meta = compiled.columns["nth_two"]
    assert rows[1]["nth_two"] is None
    assert nth_meta.complete_column is not None and nth_meta.reason_column is not None
    assert rows[1][nth_meta.complete_column] is True
    assert rows[1][nth_meta.reason_column] is None


def test_event_time_selector_metadata_distinguishes_known_absence_and_missing_origin(
    column_model,
):
    known_absent = event_time("amount", window=("-2s", "-1s"))
    selected = event_time("amount", window=("0s", "1s"))
    compiled = _compile(
        column_model,
        eligibility={"known_absent": is_null(known_absent)},
        outcomes={
            "selected_at": selected,
            "selected_amount": agg("total", window=("0s", "1s"), relative_to=selected),
        },
    )
    compiled.validate(execute=column_model.con.to_pyarrow)
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert [row["known_absent"] for row in rows] == [True, True]
    selector_meta = compiled.columns["selected_at"]
    assert selector_meta.complete_column is not None
    assert selector_meta.reason_column is not None
    assert rows[1]["selected_at"] is None
    assert rows[1][selector_meta.complete_column] is True
    amount_meta = compiled.columns["selected_amount"]
    assert amount_meta.complete_column is not None
    assert amount_meta.reason_column is not None
    assert rows[1]["selected_amount"] is None
    assert rows[1][amount_meta.complete_column] is False
    assert rows[1][amount_meta.reason_column] == "missing_selector_origin"


def test_event_time_selector_admits_tied_timestamps(column_model):
    column_model.con.raw_sql("""
        INSERT INTO events VALUES
            ('a', TIMESTAMPTZ '2025-01-02 12:00:00+00', 8, 'duplicate')
    """)
    compiled = _compile(
        column_model,
        outcomes={
            "last_at": event_time("amount", window=("0s", "1s"), occurrence="last"),
            "second_at": event_time("amount", window=("0s", "1s"), occurrence="nth", n=2),
        },
    )
    compiled.validate(execute=column_model.con.to_pyarrow)
    row = next(
        row for row in column_model.con.to_pyarrow(compiled.table).to_pylist() if row["unit"] == "a"
    )
    instant = datetime(2025, 1, 2, 12, tzinfo=UTC)
    for name in ("last_at", "second_at"):
        assert row[name] == instant
        assert row[f"{name}__complete"] is True


def test_event_time_selector_respects_unit_observation_interval(column_model):
    con = column_model.con
    con.create_table(
        "observed_users",
        ibis.memtable(
            {
                "id": ["a", "b"],
                "observed_from": [
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, tzinfo=UTC),
                ],
                "observed_until": [
                    datetime(2025, 1, 3, tzinfo=UTC),
                    datetime(2025, 1, 2, 11, 59, 59, tzinfo=UTC),
                ],
            }
        ),
    )
    model = Model.from_catalog(
        column_model.catalog,
        con=con,
        units={
            "user": UnitSource(
                "observed_users",
                "id",
                observed_from="observed_from",
                observed_until="observed_until",
            )
        },
        readiness=column_model.readiness,
    )
    compiled = _compile(model, outcomes={"selected_at": event_time("amount", window=("0s", "1s"))})
    compiled.validate(execute=con.to_pyarrow)
    rows = con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    meta = compiled.columns["selected_at"]
    assert meta.complete_column is not None and meta.reason_column is not None
    assert rows[0][meta.complete_column] is True
    assert rows[1][meta.complete_column] is False
    assert rows[1][meta.reason_column] == "incomplete_observation"


def test_event_time_selector_normalizes_naive_source_timestamps_to_utc(column_model):
    con = column_model.con
    con.raw_sql("""
        CREATE TABLE naive_events AS
        SELECT user_id, TIMESTAMP '2025-01-02 12:00:00' AS ts, amount, tier
        FROM events
        WHERE user_id = 'a' AND ts = TIMESTAMPTZ '2025-01-02 12:00:00+00'
    """)
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["relation"] = {"kind": "table", "name": "naive_events"}
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    compiled = _compile(model, outcomes={"selected_at": event_time("amount", window=("0s", "1s"))})
    compiled.validate(execute=con.to_pyarrow)
    row = con.to_pyarrow(compiled.table).to_pylist()[0]
    assert row["selected_at"] == datetime(2025, 1, 2, 12, tzinfo=UTC)


def test_event_time_selector_treats_value_less_fact_as_row_occurrence(column_model):
    con = column_model.con
    con.raw_sql("""
        CREATE TABLE row_events AS
        SELECT user_id, ts, amount, tier,
            row_number() OVER (PARTITION BY user_id ORDER BY ts) AS event_id
        FROM events
    """)
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["relation"] = {"kind": "table", "name": "row_events"}
    definition["sources"]["events"]["events"] = ("amount", "presence")
    definition["events"]["presence"] = {"source": "events", "column": None}
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
        event_ids={"events": "event_id"},
    )
    compiled = _compile(
        model, outcomes={"selected_at": event_time("presence", window=("0s", "1s"))}
    )
    compiled.validate(execute=con.to_pyarrow)
    rows = con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["selected_at"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert rows[1]["selected_at"] is None


def test_last_occurrence_selects_latest_event_before_t0(column_model):
    con = column_model.con
    con.raw_sql("DROP TABLE events")
    con.raw_sql("""
        CREATE TABLE events (
            user_id VARCHAR, ts TIMESTAMPTZ, amount BIGINT, tier VARCHAR
        )
    """)
    con.raw_sql("""
        INSERT INTO events VALUES
            ('a', TIMESTAMPTZ '2024-12-28 12:00:00+00', 1, 'old'),
            ('a', TIMESTAMPTZ '2025-01-01 12:00:00+00', 2, 'new')
    """)
    model = Model.from_catalog(
        column_model.catalog,
        con=con,
        readiness={"events": Coverage("2024-12-20", "2025-01-03")},
        units=column_model.units,
    )
    selected = event_time("amount", window=("-5d", "0s"), occurrence="last")
    compiled = _compile(model, features={"selected_at": selected})
    rows = con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    assert rows[0]["selected_at"] == datetime(2025, 1, 1, 12, tzinfo=UTC)


def test_last_occurrence_empty_complete_lookback_is_null_complete(column_model):
    selected = event_time("amount", window=("-1d", "0s"), occurrence="last")
    compiled = _compile(column_model, features={"selected_at": selected})
    rows = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()
    meta = compiled.columns["selected_at"]
    assert rows[1]["selected_at"] is None
    assert rows[1][meta.complete_column] is True
    assert rows[1][meta.reason_column] is None


def _dimension_label_model(
    column_model,
    events,
    *,
    starts,
    ends,
    workspace_by_event_id=None,
    known_workspace="a",
):
    con = column_model.con
    con.raw_sql("DROP TABLE events")
    con.raw_sql("""
        CREATE TABLE events (
            user_id VARCHAR, ts TIMESTAMPTZ, amount BIGINT, tier VARCHAR,
            event_id VARCHAR, workspace_id VARCHAR
        )
    """)
    for timestamp, event_id in events:
        workspace = workspace_by_event_id[event_id] if workspace_by_event_id is not None else "a"
        con.raw_sql(
            "INSERT INTO events VALUES "
            f"('a', TIMESTAMPTZ '{timestamp}', 1, NULL, '{event_id}', '{workspace}')"
        )
    con.raw_sql("""
        CREATE TABLE tier_history (
            id VARCHAR, tier VARCHAR, starts TIMESTAMPTZ, ends TIMESTAMPTZ
        )
    """)
    con.raw_sql(
        "INSERT INTO tier_history VALUES "
        f"('{known_workspace}', 'gold', TIMESTAMPTZ '{starts}', TIMESTAMPTZ '{ends}')"
    )
    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["dimensions"] = ("history",)
    definition["sources"]["events"]["relation"] = {"kind": "table", "name": "events"}
    if workspace_by_event_id is not None:
        definition["sources"]["events"]["units"]["workspace"] = "workspace_id"
    definition["properties"]["tier"]["dimension"] = "history"
    definition["dimensions"]["history"] = {
        "relation": {"kind": "table", "name": "tier_history"},
        "unit": "workspace" if workspace_by_event_id is not None else "user",
        "key": "id",
        "properties": ("tier",),
        "validity": {"valid_from": "starts", "valid_to": "ends"},
    }
    return Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        readiness={"events": Coverage("2024-12-20", "2025-01-03")},
        units=column_model.units,
        event_ids={"events": "event_id"},
    )


def test_last_occurrence_unknown_label_after_latest_match_is_incomplete(column_model):
    model = _dimension_label_model(
        column_model,
        [("2024-12-30 12:00:00+00", "a"), ("2025-01-01 12:00:00+00", "b")],
        starts="2024-12-29 00:00:00+00",
        ends="2024-12-31 00:00:00+00",
    )
    selector = event_time(
        "amount", window=("-6d", "0s"), label_property="tier", equals="gold", occurrence="last"
    )
    compiled = _compile(model, features={"selected_at": selector})
    row = model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()[0]
    meta = compiled.columns["selected_at"]
    assert row["selected_at"] is None
    assert row[meta.complete_column] is False
    assert row[meta.reason_column] == "incomplete_label_history"


def test_last_occurrence_unknown_label_before_latest_match_is_complete(column_model):
    model = _dimension_label_model(
        column_model,
        [("2024-12-28 12:00:00+00", "a"), ("2025-01-01 12:00:00+00", "b")],
        starts="2024-12-31 00:00:00+00",
        ends="2025-01-03 00:00:00+00",
    )
    selector = event_time(
        "amount", window=("-6d", "0s"), label_property="tier", equals="gold", occurrence="last"
    )
    compiled = _compile(model, features={"selected_at": selector})
    row = model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()[0]
    meta = compiled.columns["selected_at"]
    assert row["selected_at"] == datetime(2025, 1, 1, 12, tzinfo=UTC)
    assert row[meta.complete_column] is True
    assert row[meta.reason_column] is None


def test_last_occurrence_incomplete_lookback_is_null_incomplete(column_model):
    selector = event_time("amount", window=("-6d", "0s"), occurrence="last")
    compiled = _compile(column_model, features={"selected_at": selector})
    row = column_model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()[0]
    meta = compiled.columns["selected_at"]
    assert row["selected_at"] is None
    assert row[meta.complete_column] is False
    assert row[meta.reason_column] == "incomplete_coverage"


def test_last_occurrence_tied_unknown_label_after_match_is_incomplete(column_model):
    model = _dimension_label_model(
        column_model,
        [("2025-01-01 12:00:00+00", "a"), ("2025-01-01 12:00:00+00", "b")],
        starts="2024-12-31 00:00:00+00",
        ends="2025-01-03 00:00:00+00",
        workspace_by_event_id={"a": "known", "b": "unknown"},
        known_workspace="known",
    )
    selector = event_time(
        "amount", window=("-6d", "0s"), label_property="tier", equals="gold", occurrence="last"
    )
    compiled = _compile(model, features={"selected_at": selector})
    row = model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()[0]
    meta = compiled.columns["selected_at"]
    assert row["selected_at"] is None
    assert row[meta.complete_column] is False
    assert row[meta.reason_column] == "incomplete_label_history"


def test_last_occurrence_tied_unknown_label_before_match_is_complete(column_model):
    model = _dimension_label_model(
        column_model,
        [("2025-01-01 12:00:00+00", "a"), ("2025-01-01 12:00:00+00", "b")],
        starts="2024-12-31 00:00:00+00",
        ends="2025-01-03 00:00:00+00",
        workspace_by_event_id={"a": "unknown", "b": "known"},
        known_workspace="known",
    )
    selector = event_time(
        "amount", window=("-6d", "0s"), label_property="tier", equals="gold", occurrence="last"
    )
    compiled = _compile(model, features={"selected_at": selector})
    row = model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()[0]
    meta = compiled.columns["selected_at"]
    assert row["selected_at"] == datetime(2025, 1, 1, 12, tzinfo=UTC)
    assert row[meta.complete_column] is True
    assert row[meta.reason_column] is None


def test_last_occurrence_tied_known_events_use_latest_event_id(column_model):
    model = _dimension_label_model(
        column_model,
        [
            ("2025-01-01 12:00:00+00", "a"),
            ("2025-01-01 12:00:00+00", "b"),
            ("2025-01-01 12:00:00+00", "c"),
        ],
        starts="2024-12-31 00:00:00+00",
        ends="2025-01-03 00:00:00+00",
        workspace_by_event_id={"a": "known", "b": "unknown", "c": "known"},
        known_workspace="known",
    )
    selector = event_time(
        "amount", window=("-6d", "0s"), label_property="tier", equals="gold", occurrence="last"
    )
    compiled = _compile(model, features={"selected_at": selector})
    row = model.con.to_pyarrow(compiled.table.order_by("unit")).to_pylist()[0]
    meta = compiled.columns["selected_at"]
    assert row["selected_at"] == datetime(2025, 1, 1, 12, tzinfo=UTC)
    assert row[meta.complete_column] is True
    assert row[meta.reason_column] is None


def test_future_looking_since_feature_refuses(column_model):
    with pytest.raises(TerrineError) as error:
        _compile(column_model, features={"recency": since("amount", window=("-7d", "1d"))})
    assert error.value.code == "terrine.design.gap_violation"


@pytest.mark.parametrize("consumer", ["event_time", "agg", "asof"])
def test_elapsed_selector_cannot_be_an_origin(consumer):
    elapsed = since("amount", window=("-7d", 0))
    with pytest.raises(TerrineError) as error:
        if consumer == "event_time":
            event_time("amount", window=("0s", "1d"), relative_to=elapsed)
        elif consumer == "agg":
            agg("total", window=("-1d", 0), relative_to=elapsed)
        else:
            asof("tier", before="0s", relative_to=elapsed)
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "elapsed_selector_as_origin"


@pytest.mark.parametrize("predicate", [eq, is_null, None])
def test_elapsed_selector_in_eligibility_refuses(predicate):
    with pytest.raises(TerrineError) as error:
        elapsed = since("amount", window=("-7d", 0))
        condition = (
            elapsed
            if predicate is None
            else predicate(elapsed, 0)
            if predicate is eq
            else predicate(elapsed)
        )
        Design(
            "user",
            FixedDate("2025-01-02"),
            eligibility={"recent": condition},  # ty: ignore[invalid-argument-type]
        )
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "elapsed_selector_in_predicate"


@pytest.mark.parametrize("shape", ["StatePanel.baseline_features", "CohortSpec", "LifeTable"])
def test_since_refuses_in_state_panel_baseline_and_ltv(column_model, shape):
    from terrine import recipes
    from terrine.spec import StateObservations, StatePanel

    elapsed = since("amount", window=("-7d", 0))
    with pytest.raises(TerrineError) as error:
        if shape == "StatePanel.baseline_features":
            panel = StatePanel(
                StateObservations("tier", ("old", "new")),
                max_periods=2,
                baseline_features={"recency": elapsed},
            )
            design = Design("user", Schedule("1d", "2025-01-01", "2025-01-02"), shape=panel)
        elif shape == "CohortSpec":
            design = recipes.ltv(
                start_event="amount",
                metric="total",
                period="1d",
                periods=2,
                features={"recency": elapsed},
            )
        else:
            design = recipes.life_table(
                start_event="amount",
                end_event="amount",
                bin="1d",
                group_by={"recency": elapsed},
            )
        design.validate(column_model)
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "elapsed_selector_unsupported_shape"
    assert error.value.context["shape"] == shape


def test_frame_expr_refuses_elapsed_selectors(column_model):
    frame = Design(
        "user",
        FixedDate("2025-01-02"),
        features={"recency": since("amount", window=("-1d", 0))},
    ).build(column_model)
    with pytest.raises(TerrineError) as error:
        _ = frame.expr
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "elapsed_selector_in_expr"


def test_elapsed_finalization_is_idempotent(column_model):
    t0 = datetime(2025, 1, 2, 12, tzinfo=UTC)
    raw = pa.table(
        {
            "unit": ["a"],
            "t0": [t0],
            "recency": [t0 - timedelta(seconds=1)],
            "next": [t0 + timedelta(seconds=1)],
        }
    )
    frame = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features={"recency": since("amount", window=("-1d", 0))},
        outcomes={"next": since("amount", window=("1s", "2s"), occurrence="first")},
    ).build(column_model)
    result = SimpleNamespace(table=frame._normalize_output(raw))
    assert frame._normalize_output(result.table).equals(result.table, check_metadata=True)
    assert pa.types.is_duration(result.table["recency"].type)
    assert next(row for row in result.table.to_pylist() if row["unit"] == "a")["next"] == (
        -timedelta(seconds=1)
    )


def test_elapsed_finalization_preserves_nanoseconds_and_refuses_tick_overflow(column_model):
    frame = Design(
        "user",
        FixedDate("2025-01-02"),
        features={"recency": since("amount", window=("-1d", 0))},
    ).build(column_model)
    dtype = pa.timestamp("ns", tz="UTC")
    raw = pa.table(
        {
            "t0": pa.array([200, 200], type=dtype),
            "recency": pa.array([199, None], type=dtype),
        }
    )
    result = frame._normalize_output(raw)
    assert result["recency"].type == pa.duration("ns")
    assert result["recency"].cast(pa.int64()).to_pylist() == [1, None]
    assert result["__terrine_selected_at_recency"].cast(pa.int64()).to_pylist() == [199, None]
    overflow = pa.table(
        {
            "t0": pa.array([2**63 - 1], type=dtype),
            "recency": pa.array([-1], type=dtype),
        }
    )
    with pytest.raises(pa.ArrowInvalid):
        frame._normalize_output(overflow)


def test_filter_lowering_parity(column_model):
    from terrine.compile._predicates import CompiledValue, compile_predicate
    from terrine.data import ColumnMeta, FeatureCutoff
    from terrine.semantic.bind import source_filter_predicate
    from terrine.semantic.filters import lower_filter
    from terrine.semantic.models import BoundFilter, Filter, PropertyBinding
    from terrine.temporal.daily import _predicate

    table = column_model.con.sql("""
        SELECT i, money::DECIMAL(10,2) AS money, score::DOUBLE AS score,
            true AS complete, NULL::VARCHAR AS reason,
            TIMESTAMPTZ '2025-01-01' AS observed_from,
            TIMESTAMPTZ '2025-01-03' AS observed_until
        FROM (VALUES (1, 10.49, 9), (2, 10.50, 11), (3, NULL, 'NaN'::DOUBLE))
            t(i, money, score)
    """)
    source = replace(
        column_model.sources["events"],
        properties={
            name: PropertyBinding(name, name, "float", "event_time", None)
            for name in ("money", "score")
        },
    )
    measure = column_model.metrics["total"].measures[0]
    meta = ColumnMeta(None, "feature", "observed_from", "observed_until", "complete", "reason")
    for column, threshold, expected in (
        ("money", "10.50", [False, True, None]),
        ("score", 11.0, [False, True, None]),
    ):
        filt = Filter(property=column, op="equals", values=(threshold,))
        lowered = lower_filter(table, filt, table[column])
        membership = source_filter_predicate(table, replace(source, where=(filt,)))
        metric = _predicate(
            table, replace(measure, filters=(BoundFilter(column, "equals", (threshold,)),)), source
        )[0]
        expression = asof(column)
        predicate = compile_predicate(
            table,
            eq(expression, threshold),
            {expression: CompiledValue(column, meta, FeatureCutoff(0, True, "recorded_at"))},
            output="eligible",
            role="eligibility",
        )
        rows = column_model.con.to_pyarrow(
            table.select("i", lowered=lowered, metric=metric, source=membership).order_by("i")
        ).to_pylist()
        assert [r["lowered"] for r in rows] == [r["metric"] for r in rows] == expected
        assert [r["source"] for r in rows] == expected
        assert (
            column_model.con.to_pyarrow(predicate.table.order_by("i"))["eligible"].to_pylist()
            == expected
        )


def test_property_state_identity_includes_source_membership(column_model):
    from terrine.frame import _feature_definition_hash
    from terrine.spec import StateObservations, StatePanel

    definition = column_model.catalog.model_dump(mode="python")
    definition["sources"]["events"]["where"] = (
        {"property": "user_id", "op": "equals", "values": ("a",)},
    )
    filtered = Model.from_catalog(
        definition,
        con=column_model.con,
        units=column_model.units,
        readiness=column_model.readiness,
    )
    design = Design(
        "user",
        Schedule("1d", "2025-01-01", "2025-01-02"),
        shape=StatePanel(StateObservations("tier", ("old", "new")), max_periods=2),
    )
    assert _feature_definition_hash(design, ("from_state",), column_model) != (
        _feature_definition_hash(design, ("from_state",), filtered)
    )
