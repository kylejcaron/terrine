"""Pure Arrow fixed-cohort fixtures, including deliberately incomplete raw data."""

from datetime import timedelta
from hashlib import sha256
from importlib.resources import files

import pyarrow as pa

from terrine.data import (
    ColumnMeta,
    Coverage,
    EligibilitySummary,
    ExecutionSnapshot,
    ExpressionTiming,
    FeatureCutoff,
    MaterializedFrame,
    Provenance,
    Roles,
    UnitSource,
)
from terrine.simulate.world import World, WorldConfig
from terrine.windows import Window
from tests.reference.baseline import reference_rows


def materialized_baseline(world: World, *, weighted: bool = False) -> MaterializedFrame:
    """Materialize raw reference rows with explicit baseline-only causal metadata.

    These are original binary64 measurements, not compiler aggregates.
    No warehouse execution snapshot or replay equivalence is claimed.
    """
    table = reference_rows(world)
    cutoff = FeatureCutoff(0, True, "retrospective_effective_time")
    generations = dict.fromkeys(("world_roster", "world_events", "world_attributes"))
    declaration = (
        f"baseline-v1:{world.config.t0.isoformat()}:"
        f"{world.config.horizon_seconds}:{weighted}:{world.config.target_weight_slope}"
    )
    provenance = Provenance(
        feature_cutoffs={"x": cutoff, "plan_tier": cutoff},
        eligibility_cutoffs={},
        eligibility_summary=EligibilitySummary(table.num_rows, 0, 0, table.num_rows),
        design_hash=sha256(declaration.encode("utf-8")).hexdigest(),
        model_hash=sha256(world.semantic_yaml.encode("utf-8")).hexdigest(),
        source_generations=generations,
        execution_snapshot=ExecutionSnapshot(
            snapshot_id=None,
            backend="arrow",
            source_generations=generations,
            input_binding_id=None,
            replay_equivalence="unverified",
        ),
        shape="cross_section",
        expression_timings={
            "net_revenue": ExpressionTiming(
                kind="fixed_t0",
                selector_ref=None,
                relative_offsets=None,
                support_seconds=(0, world.config.horizon_seconds),
                selector_dependencies=(),
                selector_support_seconds={},
                point_time_column=None,
                start_bound_column=None,
                end_bound_column=None,
            )
        },
        treatment_design="baseline_binary",
        treatment_known_at_t0=True,
        treatment_knowledge="retrospective_effective_time",
        weight_kind="baseline_function" if weighted else None,
        weight_cutoff=cutoff if weighted else None,
        weight_depends_on_treatment=False if weighted else None,
    )
    columns = {}
    for name, role in (
        ("x", "feature"),
        ("plan_tier", "feature"),
        ("baseline_enabled", "treatment"),
        ("net_revenue", "outcome"),
    ):
        columns[name] = ColumnMeta(
            window=Window(0, world.config.horizon_seconds) if role == "outcome" else None,
            role=role,
            observed_from_column=f"{name}__observed_from",
            observed_until_column=f"{name}__observed_until",
            complete_column=f"{name}__complete",
            reason_column=f"{name}__reason",
        )
    if weighted:
        columns["target_weight"] = ColumnMeta(
            window=None,
            role="weight",
            observed_from_column="x__observed_from",
            observed_until_column="x__observed_until",
            complete_column="x__complete",
            reason_column="x__reason",
        )
    return MaterializedFrame(
        table=table,
        roles=Roles(
            unit="unit_id",
            anchor="anchor_id",
            t0="t0",
            treatment="baseline_enabled",
            features=("x", "plan_tier"),
            outcomes=("net_revenue",),
            weight="target_weight" if weighted else None,
        ),
        columns=columns,
        provenance=provenance,
    )


def boundary_world() -> World:
    """Hand-built intraday boundaries, tied events, inactivity, and a history gap."""
    config = WorldConfig(
        n_units=3, lookback_seconds=2 * 86400, horizon_seconds=2 * 86400, outcome_noise_sd=0
    )
    t0 = config.t0
    timestamp = pa.timestamp("us", tz="UTC")
    roster = pa.table(
        {
            "unit_id": pa.array([1, 2, 3], type=pa.int64()),
            "observed_from": pa.nulls(3, type=timestamp),
            "observed_until": pa.nulls(3, type=timestamp),
        }
    )
    history_schema = pa.schema(
        [
            ("unit_id", pa.int64()),
            ("valid_from", timestamp),
            ("valid_to", timestamp),
            ("x", pa.float64()),
            ("plan_tier", pa.string()),
            ("baseline_enabled", pa.bool_()),
        ]
    )
    start = t0 - timedelta(days=2)
    attributes = pa.Table.from_pylist(
        [
            {
                "unit_id": 1,
                "valid_from": start,
                "valid_to": t0,
                "x": 0.25,
                "plan_tier": "basic",
                "baseline_enabled": False,
            },
            {
                "unit_id": 1,
                "valid_from": t0,
                "valid_to": None,
                "x": 99.0,
                "plan_tier": "plus",
                "baseline_enabled": True,
            },
            {
                "unit_id": 2,
                "valid_from": start,
                "valid_to": None,
                "x": -0.25,
                "plan_tier": "enterprise",
                "baseline_enabled": True,
            },
            {
                "unit_id": 3,
                "valid_from": start,
                "valid_to": t0 - timedelta(seconds=1),
                "x": 0.5,
                "plan_tier": "plus",
                "baseline_enabled": False,
            },
        ],
        schema=history_schema,
    )
    event_schema = pa.schema(
        [
            ("event_id", pa.string()),
            ("unit_id", pa.int64()),
            ("ts", timestamp),
            ("kind", pa.string()),
            ("value", pa.float64()),
        ]
    )
    events = pa.Table.from_pylist(
        [
            {
                "event_id": "before",
                "unit_id": 1,
                "ts": t0 - timedelta(microseconds=1),
                "kind": "revenue",
                "value": 100.0,
            },
            {"event_id": "left", "unit_id": 1, "ts": t0, "kind": "revenue", "value": 2.0},
            {
                "event_id": "tie-a",
                "unit_id": 1,
                "ts": t0 + timedelta(seconds=1),
                "kind": "revenue",
                "value": 1.0,
            },
            {
                "event_id": "tie-b",
                "unit_id": 1,
                "ts": t0 + timedelta(seconds=1),
                "kind": "revenue",
                "value": 2.0,
            },
            {
                "event_id": "right",
                "unit_id": 1,
                "ts": t0 + timedelta(days=2),
                "kind": "revenue",
                "value": 100.0,
            },
            {
                "event_id": "other-kind",
                "unit_id": 1,
                "ts": t0,
                "kind": "engagement",
                "value": 100.0,
            },
            {"event_id": "third", "unit_id": 3, "ts": t0, "kind": "revenue", "value": 7.0},
        ],
        schema=event_schema,
    )
    oracle = pa.table(
        {
            "unit_id": pa.array([1, 2, 3], type=pa.int64()),
            "x": [0.25, -0.25, 0.5],
            "plan_tier": ["basic", "enterprise", "plus"],
            "target_weight": [1.0, 1.0, 1.0],
        }
    )
    return World(
        config=config,
        metadata={"fixture": "raw_boundaries"},
        roster=roster,
        events=events,
        attributes=attributes,
        analysis_table=pa.table({}),
        oracle=oracle,
        semantic_yaml=files("terrine.simulate")
        .joinpath("baseline.yaml")
        .read_text(encoding="utf-8"),
        readiness={
            "world_events": Coverage(
                (t0 - timedelta(days=3)).date(), (t0 + timedelta(days=3)).date()
            )
        },
        units={
            "user": UnitSource(
                "world_roster",
                "unit_id",
                observed_from="observed_from",
                observed_until="observed_until",
            )
        },
        event_ids={"world_events": "event_id"},
    )
