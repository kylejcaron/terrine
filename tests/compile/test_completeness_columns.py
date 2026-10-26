from datetime import UTC, datetime, timedelta

import pyarrow as pa

from terrine.compile.shapes.life_table import aggregate_life_table
from terrine.data import ColumnMeta, MaterializedFrame, Roles
from terrine.export.preflight import preflight_export
from tests.test_metadata import _provenance


def test_life_table_keeps_list_valued_reasons_under_the_suffix():
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    records = pa.table(
        {
            "t0": [origin, origin],
            "entry_time": [origin, origin],
            "duration": pa.array([timedelta(hours=1)] * 2, type=pa.duration("us")),
            "event": [False, False],
            "duration__complete": [False, False],
            "duration__reason": ["incomplete_observation", "missing_history"],
            "censor_reason": ["incomplete_observation", "missing_history"],
        }
    )
    table = aggregate_life_table(records, interval_seconds=86400, group_by=())
    expected = ["incomplete_observation", "missing_history"]
    assert table["events__reason"].to_pylist() == [expected]
    assert pa.types.is_list(table.schema.field("events__reason").type)
    assert table["events__complete"].to_pylist() == [False]
    frame = MaterializedFrame(
        table,
        Roles("unit", "anchor_id", "t0", None, (), ("events",)),
        {"events": ColumnMeta(None, "outcome", None, None, "events__complete", "events__reason")},
        _provenance(),
    )
    result = preflight_export(frame, drop_incomplete=True)
    assert result.table.num_rows == 0
    assert result.excluded_counts == {reason: 1 for reason in expected}


def test_null_reason_round_trips_arrow_and_manifest_under_new_names():
    from terrine.dataset.manifest import DatasetManifest, ManifestColumn, ManifestGeneration
    from terrine.spec import Design, FixedDate
    from terrine.spec.dataset import DatasetDefinition

    origin = datetime(2025, 1, 1, tzinfo=UTC)
    table = pa.table(
        {
            "unit": ["u"],
            "anchor_id": ["a"],
            "t0": [origin],
            "revenue": pa.array([None], type=pa.int64()),
            "revenue__complete": [False],
            "revenue__reason": ["incomplete_observation"],
            "revenue_observed_from": [origin],
            "revenue_observed_until": [origin],
            "_observed_from": [origin],
            "_observed_until": [origin],
            "anchor_provenance": ["schedule"],
        }
    )
    frame = MaterializedFrame(
        table,
        Roles("unit", "anchor_id", "t0", None, (), ("revenue",)),
        {
            "revenue": ColumnMeta(
                None,
                "outcome",
                "revenue_observed_from",
                "revenue_observed_until",
                "revenue__complete",
                "revenue__reason",
            )
        },
        _provenance(),
    )
    arrow = frame.to_arrow()
    assert arrow.column_names == [
        "unit",
        "anchor_id",
        "t0",
        "revenue",
        "revenue__complete",
        "revenue__reason",
    ]
    restored = MaterializedFrame.from_arrow(arrow)
    assert restored.table.equals(table)
    assert restored.columns == frame.columns
    assert MaterializedFrame.from_json(frame.to_json()).table.equals(table)
    definition = DatasetDefinition(
        schema_version=1, base_design=Design("user", FixedDate("2025-01-01"))
    )
    manifest = DatasetManifest(
        definition,
        ManifestGeneration("ds", "g1", ("table:g1",), ("sha256:x",), 1),
        tuple(
            ManifestColumn(
                field.name,
                str(field.type),
                "outcome" if field.name == "revenue" else "support",
                frame.to_metadata()["columns"].get(field.name, {}),
            )
            for field in table.schema
        ),
        "design",
        "model",
        {},
        {},
    )
    reopened = DatasetManifest.from_json(manifest.to_json())
    revenue = next(column for column in reopened.columns if column.name == "revenue")
    assert revenue.metadata["reason_column"] == "revenue__reason"
