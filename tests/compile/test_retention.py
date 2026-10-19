from datetime import UTC, datetime

import ibis
import pyarrow as pa
import pytest

from terrine import Coverage, Design, FixedDate, RetentionMetric, UnitSource, retention
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import EventAnchor, EventPanel, Schedule
from terrine.spec.io import from_json, from_yaml, to_json, to_yaml
from tests.reference.retention import retention_rows


@pytest.fixture
def retention_model():
    con = ibis.duckdb.connect()
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["a", "a", "a", "a", "b", "unknown"],
                "id": ["x", "x", "y", "z", "other", "uncertain"],
                "name": ["target", "target", "target", "target", "other", None],
                "ts": [
                    datetime(2025, 1, day, hour, tzinfo=UTC)
                    for day, hour in [(2, 0), (3, 0), (3, 1), (4, 12), (3, 0), (3, 0)]
                ],
            }
        ),
    )
    units = ["a", "b", "empty", "short", "late", "unobserved", "unknown"]
    con.create_table(
        "users",
        pa.table(
            {
                "id": units,
                "observed_from": [
                    datetime(2025, 1, 3 if unit == "late" else 1, tzinfo=UTC) for unit in units
                ],
                "observed_until": pa.array(
                    [
                        None
                        if unit == "unobserved"
                        else datetime(2025, 1, 3 if unit == "short" else 6, tzinfo=UTC)
                        for unit in units
                    ],
                    type=pa.timestamp("us", tz="UTC"),
                ),
            }
        ),
    )
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["occurrence"],
                    "properties": ["event_name"],
                    "dimensions": [],
                }
            },
            "facts": {"occurrence": {"source": "events", "column": "id"}},
            "properties": {
                "event_name": {
                    "source": "events",
                    "column": "name",
                    "dtype": "string",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {},
            "metrics": {},
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        units={
            "user": UnitSource(
                "users", "id", observed_from="observed_from", observed_until="observed_until"
            )
        },
        readiness={"events": Coverage("2025-01-01", "2025-01-04")},
    )
    try:
        yield model
    finally:
        con.disconnect()


@pytest.mark.parametrize("horizon", ["2d", "open_ended"])
def test_named_retention_roundtrip_frame_matches_independent_occurrences(retention_model, horizon):
    expression = retention(
        "occurrence",
        label_property="event_name",
        target_event="target",
        thresholds=(1, 3),
        horizon=horizon,
    )
    assert isinstance(expression, RetentionMetric)
    design = Design("user", FixedDate("2025-01-02"), outcomes={"retained": expression})
    assert from_json(to_json(design)) == from_yaml(to_yaml(design)) == design
    assert from_json(to_json(design)).spec_hash == design.spec_hash
    result = design.build(retention_model).materialize()
    table = result.to_arrow()
    meta = result.columns["retained"]
    rows = {row["unit"]: row for row in table.to_pylist()}
    events = (
        retention_model.con.table("events")
        .select(unit="user_id", event_id="id", name="name", timestamp="ts")
        .to_pandas()
    )
    anchors = (
        table.select(["unit", "anchor_id", "t0", "_observed_from", "_observed_until"])
        .to_pandas()
        .rename(columns={"_observed_from": "observed_from", "_observed_until": "observed_until"})
    )
    expected = retention_rows(
        events,
        anchors,
        thresholds=(1, 3),
        horizon=2 * 86400 if horizon == "2d" else None,
        coverage_start=datetime(2025, 1, 1, tzinfo=UTC),
        coverage_end=datetime(2025, 1, 5, tzinfo=UTC),
        cutoff=datetime(2025, 1, 5, tzinfo=UTC),
    )
    for oracle in expected:
        row = rows[oracle["unit"]]
        assert row["retained"] == oracle["retention_band"]
        assert row[meta.complete_column] == oracle["complete"]
        assert row[meta.reason_column] == oracle["reason"]
    assert rows["a"]["retained"] == (1 if horizon == "2d" else 2)
    assert rows["empty"]["retained"] == 0
    assert rows["unknown"][meta.reason_column] == "incomplete_label_history"
    assert meta.numeric_policy == "exact"
    restored = type(result).from_arrow(table)
    assert restored.columns == result.columns
    assert restored.provenance.expression_timings == result.provenance.expression_timings


@pytest.mark.parametrize("shape", ["schedule", "event_panel"])
def test_retention_composes_with_bounded_repeated_shapes(retention_model, shape):
    expression = retention(
        "occurrence",
        label_property="event_name",
        target_event="target",
        thresholds=(1, 3),
        horizon="1d",
    )
    if shape == "schedule":
        design = Design(
            "user", Schedule("1d", "2025-01-02", "2025-01-04"), outcomes={"retained": expression}
        )
    else:
        design = EventPanel(
            Design("user", EventAnchor("occurrence", "first"), outcomes={"retained": expression}),
            periods=(0, 1),
            period_seconds=86400,
            reference_period=0,
        )
    result = design.build(retention_model).materialize()
    rows = result.to_arrow().to_pylist()
    assert rows
    assert all(row["retained"] in (None, 0, 1) for row in rows)


def test_retention_feature_and_unknown_fact_refuse_without_source_execution(
    retention_model, monkeypatch
):
    def fail(*args, **kwargs):
        raise AssertionError("retention admission executed a query")

    monkeypatch.setattr(retention_model.con, "to_pyarrow", fail)
    expression = retention(
        "occurrence",
        label_property="event_name",
        target_event="target",
        thresholds=(1,),
        horizon="1d",
    )
    with pytest.raises(TerrineError) as feature:
        Design("user", FixedDate("2025-01-02"), features={"retained": expression}).validate(
            retention_model
        )
    assert feature.value.code == "terrine.design.gap_violation"
    with pytest.raises(TerrineError) as missing:
        Design(
            "user",
            FixedDate("2025-01-02"),
            outcomes={
                "retained": retention(
                    "absent",
                    label_property="event_name",
                    target_event="target",
                    thresholds=(1,),
                    horizon="1d",
                )
            },
        ).validate(retention_model)
    assert missing.value.code == "terrine.design.missing_reference"


def test_open_retention_sets_independent_treatment_followup_bound(retention_model):
    from terrine.spec import PersistentAdoption, Sequential

    original = retention_model
    record = original.catalog.model_dump(mode="json")
    record["sources"]["treatments"] = {
        **record["sources"]["events"],
        "facts": [],
        "properties": ["adopted_name"],
    }
    record["properties"]["adopted_name"] = {
        "source": "treatments",
        "column": "name",
        "dtype": "string",
        "as_of": "event_time",
    }
    model = Model.from_catalog(
        Catalog.model_validate(record),
        con=original.con,
        units={"user": UnitSource("users", "id")},
        readiness={
            "events": Coverage("2025-01-01", "2025-01-04"),
            "treatments": Coverage("2025-01-01", "2025-01-03"),
        },
    )
    expression = retention(
        "occurrence",
        label_property="event_name",
        target_event="target",
        thresholds=(1,),
        horizon="open_ended",
    )
    design = Sequential(
        Design(
            "user", Schedule("1d", "2025-01-02", "2025-01-04"), outcomes={"retained": expression}
        ),
        treatment=PersistentAdoption(
            "treatments", "adopted_name", "never", datetime(2025, 1, 1, tzinfo=UTC), "absorbing"
        ),
    )
    with pytest.raises(TerrineError) as error:
        design.validate(model)
    assert error.value.code == "terrine.design.incomplete_treatment_history"
