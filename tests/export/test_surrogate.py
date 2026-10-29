from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pyarrow as pa
import pytest

from terrine.data import ExpressionTiming, SamplingMeta
from terrine.errors import TerrineError
from terrine.export import ArrowSurrogate, to_surrogate_arrow


@pytest.mark.parametrize(
    ("build", "reason"),
    [
        (lambda: ArrowSurrogate(cast(Any, None), {}), "wrong_type"),
        (lambda: ArrowSurrogate.from_arrow(pa.table({"x": [1]})), "missing_surrogate_metadata"),
        (
            lambda: ArrowSurrogate.from_arrow(
                pa.table({"x": [1]}).replace_schema_metadata(
                    {b"terrine.surrogate_arrow.v1": b'{"schema_version": 2}'}
                )
            ),
            "unsupported_surrogate_version",
        ),
    ],
)
def test_surrogate_inputs_refuse_with_codes(build, reason):
    with pytest.raises(TerrineError) as error:
        build()
    assert error.value.code == "terrine.export.invalid_surrogate"
    assert error.value.context["reason"] == reason


def test_surrogate_arrow_round_trip_preserves_selected_columns_and_provenance(frame):
    exported = to_surrogate_arrow(frame, features=("x",), outcomes=("y",))
    restored = ArrowSurrogate.from_arrow(exported.to_arrow())

    assert restored.data.equals(exported.data)
    assert restored.metadata == exported.metadata
    assert {"id", "anchor", "t0", "x", "y", "complete", "reason"} <= set(restored.data.column_names)
    assert restored.metadata["feature_columns"] == ["x"]
    assert restored.metadata["outcome_columns"] == ["y"]


def test_surrogate_round_trip_retains_selected_event_relative_timing(frame):
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    support = ("selected_at", "selected_start", "selected_end")
    table = frame.table.append_column("selected", pa.array([1, 2]))
    table = table.append_column("excluded", pa.array([origin, origin]))
    for offset, name in enumerate(support):
        table = table.append_column(name, pa.array([origin + timedelta(days=offset)] * 2))
    timing = ExpressionTiming(
        "event_time",
        "session",
        (0, 86400),
        (-345600, -86400),
        ("session",),
        {"session": (-345600, -172800)},
        *support,
    )
    materialized = replace(
        frame,
        table=table,
        roles=replace(frame.roles, features=("selected", "excluded"), outcomes=()),
        provenance=replace(
            frame.provenance,
            expression_timings={
                "selected": timing,
                "excluded": replace(timing, point_time_column="excluded"),
            },
        ),
    )
    exported = to_surrogate_arrow(materialized, features=("selected",), outcomes=())
    restored = ArrowSurrogate.from_arrow(exported.to_arrow())
    support_columns = restored.metadata["support_columns"]
    provenance = restored.metadata["provenance"]
    assert isinstance(support_columns, list)
    assert isinstance(provenance, dict)

    for name in support:
        assert name in restored.data.column_names
        assert restored.data[name].equals(materialized.table[name])
        assert name in support_columns
    assert provenance["expression_timings"] == {
        "selected": materialized.to_metadata()["provenance"]["expression_timings"]["selected"]
    }
    assert "excluded" not in restored.data.column_names
    assert restored.metadata == exported.metadata


def test_surrogate_round_trip_retains_sampled_sequential_roles(frame):
    table = frame.table
    role_columns = ("trial", "assignment_probability", "target_weight", "trial_weight")
    for name, values in zip(
        role_columns, ([0, 1], [1.0, 0.5], [1.0, 2.0], [0.5, 0.5]), strict=True
    ):
        table = table.append_column(name, pa.array(values))
    materialized = replace(
        frame,
        table=table,
        roles=replace(
            frame.roles,
            features=("target_weight",),
            outcomes=("a",),
            trial="trial",
            assignment_probability="assignment_probability",
            target_weight="target_weight",
            trial_weight="trial_weight",
        ),
        provenance=replace(frame.provenance, shape="sequential", sampling=SamplingMeta((), 2, 27)),
    )
    exported = to_surrogate_arrow(materialized, features=(), outcomes=("a",))
    restored = ArrowSurrogate.from_arrow(exported.to_arrow())
    roles = restored.metadata["roles"]
    provenance = restored.metadata["provenance"]
    assert isinstance(roles, dict)
    assert isinstance(provenance, dict)

    for role in role_columns:
        name = getattr(materialized.roles, role)
        assert name is not None
        assert roles[role] == name
        assert name in restored.data.column_names
        assert restored.data[name].equals(materialized.table[name])
    assert provenance["sampling"] == materialized.to_metadata()["provenance"]["sampling"]
    assert restored.metadata["feature_columns"] == []
    assert restored.metadata == exported.metadata
