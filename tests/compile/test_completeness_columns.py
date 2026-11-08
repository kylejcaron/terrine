from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

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
        table=table,
        roles=Roles(
            unit="unit",
            anchor="anchor_id",
            t0="t0",
            treatment=None,
            features=(),
            outcomes=("events",),
        ),
        columns={
            "events": ColumnMeta(
                window=None,
                role="outcome",
                observed_from_column=None,
                observed_until_column=None,
                complete_column="events__complete",
                reason_column="events__reason",
            )
        },
        provenance=_provenance(),
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
        table=table,
        roles=Roles(
            unit="unit",
            anchor="anchor_id",
            t0="t0",
            treatment=None,
            features=(),
            outcomes=("revenue",),
        ),
        columns={
            "revenue": ColumnMeta(
                window=None,
                role="outcome",
                observed_from_column="revenue_observed_from",
                observed_until_column="revenue_observed_until",
                complete_column="revenue__complete",
                reason_column="revenue__reason",
            )
        },
        provenance=_provenance(),
    )
    arrow = frame.to_pyarrow()
    assert arrow.column_names == table.column_names
    restored = MaterializedFrame.from_arrow(arrow)
    assert restored.table.equals(table)
    assert restored.columns == frame.columns
    assert MaterializedFrame.from_json(frame.to_json()).table.equals(table)
    definition = DatasetDefinition(base_design=Design("user", FixedDate("2025-01-01")))
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


def test_lossless_arrow_keeps_observation_evidence_with_sorted_filtered_rows():
    from dataclasses import replace

    origin = datetime(2025, 1, 1, tzinfo=UTC)
    table = pa.table(
        {
            "unit": ["b", "a", "c"],
            "anchor_id": ["fixed"] * 3,
            "t0": [origin] * 3,
            "value": [None, None, None],
            "value__complete": [False] * 3,
            "value__reason": ["source_coverage", "unit_observation", "missing_history"],
            "value__observed_from": [origin + timedelta(days=day) for day in (2, 1, 3)],
            "value__observed_until": [origin + timedelta(days=day) for day in (5, 4, 6)],
            "resolved_time": [origin + timedelta(hours=hour) for hour in (2, 1, 3)],
            "anchor_provenance": ["event:b", "event:a", "event:c"],
        }
    )
    from terrine.data import ExpressionTiming

    frame = MaterializedFrame(
        table=table,
        roles=Roles(
            unit="unit",
            anchor="anchor_id",
            t0="t0",
            treatment=None,
            features=(),
            outcomes=("value",),
        ),
        columns={
            "value": ColumnMeta(
                window=None,
                role="outcome",
                observed_from_column="value__observed_from",
                observed_until_column="value__observed_until",
                complete_column="value__complete",
                reason_column="value__reason",
            )
        },
        provenance=replace(
            _provenance(),
            expression_timings={
                "value": ExpressionTiming(
                    kind="fixed_t0",
                    selector_ref=None,
                    relative_offsets=None,
                    support_seconds=(0, 0),
                    selector_dependencies=(),
                    selector_support_seconds={},
                    point_time_column="resolved_time",
                    start_bound_column=None,
                    end_bound_column=None,
                ),
            },
        ),
    )
    arrow = frame.to_pyarrow()
    sorted_frame = MaterializedFrame.from_arrow(arrow.sort_by("unit"))
    assert sorted_frame.table.to_pylist() == table.sort_by("unit").to_pylist()
    changed = arrow.filter(pa.array([True, True, False])).sort_by("unit")
    restored = MaterializedFrame.from_arrow(changed)
    assert (
        restored.table.to_pylist()
        == table.filter(pa.array([True, True, False])).sort_by("unit").to_pylist()
    )
    assert restored.table["value__observed_from"].to_pylist() == [
        origin + timedelta(days=1),
        origin + timedelta(days=2),
    ]


def test_compact_arrow_refuses_missing_bookkeeping_with_pickle_safe_context():
    import copy
    import pickle

    from terrine.errors import TerrineError

    origin = datetime(2025, 1, 1, tzinfo=UTC)
    frame = MaterializedFrame(
        table=pa.table({"unit": ["u"], "value": [None], "value__observed_from": [origin]}),
        roles=Roles(
            unit="unit",
            anchor="anchor_id",
            t0="t0",
            treatment=None,
            features=(),
            outcomes=("value",),
        ),
        columns={
            "value": ColumnMeta(
                window=None,
                role="outcome",
                observed_from_column="value__observed_from",
                observed_until_column=None,
                complete_column=None,
                reason_column=None,
            )
        },
        provenance=_provenance(),
    )
    arrow = frame.to_pyarrow().select(["unit", "value"])
    with pytest.raises(TerrineError) as error:
        MaterializedFrame.from_arrow(arrow)
    for refusal in (
        error.value,
        copy.deepcopy(error.value),
        pickle.loads(pickle.dumps(error.value)),
    ):
        assert refusal.code == "terrine.metadata.missing_columns"
        assert refusal.context["columns"] == ("value__observed_from",)


def test_grouped_life_table_keeps_identity_first_and_group_support_adjacent():
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    records = pa.table(
        {
            "t0": [origin],
            "entry_time": [origin],
            "duration": pa.array([timedelta(hours=1)], type=pa.duration("us")),
            "event": [False],
            "censor_reason": ["source_coverage"],
            "region": ["west"],
            "tier": ["gold"],
            "region__complete": [True],
            "region__reason": pa.array([None], type=pa.string()),
            "tier__complete": [True],
            "tier__reason": pa.array([None], type=pa.string()),
        }
    )
    table = aggregate_life_table(
        records,
        interval_seconds=86400,
        group_by=("region", "tier"),
        group_supports={
            "region__complete": "all",
            "region__reason": "reasons",
            "tier__complete": "all",
            "tier__reason": "reasons",
        },
    )
    assert table.column_names[:9] == [
        "unit",
        "anchor_id",
        "t0",
        "region",
        "region__complete",
        "region__reason",
        "tier",
        "tier__complete",
        "tier__reason",
    ]
