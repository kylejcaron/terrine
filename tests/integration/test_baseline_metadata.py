"""Consumer-visible completeness, eligibility, and native-boundary contracts."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import replace
from datetime import timedelta
from importlib.resources import files
from pathlib import Path
from tempfile import TemporaryDirectory

import ibis
import pyarrow as pa
import pytest

import terrine as tr
from terrine.data import Coverage
from terrine.errors import TerrineError
from terrine.estimate import aipw, ipw, regression_adjustment
from terrine.semantic.io import load_catalog
from terrine.semantic.models import Fact, Source
from terrine.simulate import WorldConfig, simulate
from terrine.spec import EventAnchor
from tests.fixtures.baseline import materialized_baseline

_ESTIMATORS = (ipw, aipw, regression_adjustment)


def _catalog_file(directory):
    path = f"{directory}/baseline.yaml"
    with open(path, "w", encoding="utf-8") as stream:
        stream.write(files("terrine.simulate").joinpath("baseline.yaml").read_text())
    return path


def test_incomplete_source_keeps_labels_null_and_refuses():
    world = simulate(WorldConfig(n_units=120), seed=62)
    con = ibis.duckdb.connect()
    try:
        with TemporaryDirectory(prefix="terrine-incomplete-") as directory:
            for name, table in (
                ("world_roster", world.roster),
                ("world_events", world.events),
                ("world_attributes", world.attributes),
            ):
                con.create_table(name, table)
            model = tr.Model.from_catalog(
                _catalog_file(directory),
                con=con,
                readiness={
                    "world_events": Coverage(
                        world.readiness["world_events"].start,
                        world.config.t0.date() + timedelta(days=10),
                    )
                },
                units=world.units,
                event_ids=world.event_ids,
            )
            design = tr.Design(
                "user",
                tr.FixedDate(world.config.t0),
                treatment=tr.Treatment("baseline_enabled", known_at_t0=True),
                features={
                    "x": tr.asof("engagement_score", before="0d"),
                    "plan_tier": tr.asof("plan_tier", before="0d"),
                },
                outcomes={"net_revenue": tr.agg("net_revenue", window=("0d", "28d"))},
            )
            frame = design.build(model).materialize()
            assert frame.table.num_rows == world.config.n_units
            meta = frame.columns["net_revenue"]
            assert (
                frame.table.column(meta.complete_column).to_pylist()
                == [False] * world.config.n_units
            )
            assert set(frame.table.column(meta.reason_column).to_pylist()) == {
                "incomplete_coverage"
            }
            assert frame.table.column("net_revenue").null_count == world.config.n_units
            for estimator in _ESTIMATORS:
                with pytest.raises(TerrineError) as exc:
                    estimator(frame, outcome="net_revenue")
                assert exc.value.code == "terrine.estimate.incomplete_followup"
    finally:
        con.disconnect()


@pytest.mark.parametrize("estimator", _ESTIMATORS)
def test_unknown_eligibility_refuses_before_estimation(estimator):
    world = simulate(WorldConfig(n_units=90, lookback_seconds=28 * 86400), seed=71)
    t0 = world.config.t0
    control_id = next(
        row["unit_id"] for row in world.attributes.to_pylist() if not row["baseline_enabled"]
    )
    observed_from = world.roster.column("observed_from").to_pylist()
    unit_ids = world.roster.column("unit_id").to_pylist()
    observed_from[unit_ids.index(control_id)] = t0 - timedelta(days=7)
    roster = world.roster.set_column(
        world.roster.column_names.index("observed_from"),
        "observed_from",
        pa.array(observed_from, type=world.roster.schema.field("observed_from").type),
    )
    world = replace(world, roster=roster)
    con = ibis.duckdb.connect()
    try:
        with TemporaryDirectory(prefix="terrine-eligibility-") as directory:
            for name, table in (
                ("world_roster", world.roster),
                ("world_events", world.events),
                ("world_attributes", world.attributes),
            ):
                con.create_table(name, table)
            model = tr.Model.from_catalog(
                _catalog_file(directory),
                con=con,
                readiness=world.readiness,
                units=world.units,
                event_ids=world.event_ids,
            )
            design = tr.Design(
                "user",
                tr.FixedDate(world.config.t0),
                treatment=tr.Treatment("baseline_enabled", known_at_t0=True),
                features={
                    "x": tr.asof("engagement_score", before="0d"),
                    "plan_tier": tr.asof("plan_tier", before="0d"),
                },
                outcomes={"net_revenue": tr.agg("net_revenue", window=("0d", "28d"))},
                eligibility={
                    "prior_engagement": tr.gt(tr.agg("sessions", window=("-28d", "0d")), 0),
                    "known_control": tr.eq(tr.asof("baseline_enabled", before="0d"), False),
                },
            )
            with pytest.raises(TerrineError) as exc:
                estimator(design.build(model).materialize(), outcome="net_revenue")
            assert exc.value.code == "terrine.study.eligibility_unknown"
            assert exc.value.context["candidate"] == world.config.n_units
            assert exc.value.context["excluded_false"] > 0
            assert exc.value.context["retained"] > 0
            assert (
                exc.value.context["candidate"]
                == exc.value.context["excluded_false"]
                + exc.value.context["unknown"]
                + exc.value.context["retained"]
            )
            assert "value" not in exc.value.context
            assert set(exc.value.context) == {
                "candidate",
                "excluded_false",
                "unknown",
                "retained",
            }

            unavailable = tr.Design(
                "user",
                tr.FixedDate(world.config.t0),
                treatment=tr.Treatment("baseline_enabled", known_at_t0=True),
                outcomes={"net_revenue": tr.agg("net_revenue", window=("0d", "28d"))},
                eligibility={
                    "unavailable_is_null": tr.is_null(tr.agg("sessions", window=("-28d", "0d")))
                },
            ).build(model)
            with pytest.raises(TerrineError) as is_null_error:
                unavailable.materialize()
            assert is_null_error.value.code == "terrine.study.eligibility_unknown"
            assert is_null_error.value.context["unknown"] > 0

            resolved = (
                tr.Design(
                    "user",
                    tr.FixedDate(world.config.t0),
                    treatment=tr.Treatment("baseline_enabled", known_at_t0=True),
                    outcomes={"net_revenue": tr.agg("net_revenue", window=("0d", "28d"))},
                    eligibility={
                        "known_control": tr.eq(tr.asof("baseline_enabled", before="0d"), False)
                    },
                )
                .build(model)
                .materialize()
            )
            summary = resolved.provenance.eligibility_summary
            assert summary is not None
            assert summary.unknown == 0
            assert summary.candidate == summary.excluded_false + summary.retained
            assert summary.retained == resolved.table.num_rows
    finally:
        con.disconnect()


def test_grace_counterexample_refuses_at_causal_consumer():
    frame = materialized_baseline(simulate(WorldConfig(n_units=120), seed=83))
    grace = replace(
        frame,
        provenance=replace(frame.provenance, treatment_design="grace_initiation"),
    )
    for estimator in _ESTIMATORS:
        with pytest.raises(TerrineError) as exc:
            estimator(grace, outcome="net_revenue")
        assert exc.value.code == "terrine.estimate.unsupported_design"
        assert "value" not in exc.value.context


def test_native_catalog_estimation_with_increment_blocked():
    root = Path(__file__).parents[2]
    completed = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("native_estimation_smoke.py"))],
        cwd=root,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize("has_pre_exit_event", [True, False])
def test_observation_exit_preserves_row_and_incomplete_outcome(has_pre_exit_event):
    from terrine.data import UnitSource

    world = simulate(WorldConfig(n_units=90), seed=91)
    t0 = world.config.t0
    event_rows = world.events.to_pylist()
    if has_pre_exit_event:
        for row in event_rows:
            if row["unit_id"] == 1 and row["kind"] == "revenue":
                row["ts"] = t0 + timedelta(days=1)
    else:
        event_rows = [
            row for row in event_rows if not (row["unit_id"] == 1 and row["kind"] == "revenue")
        ]
    world = replace(world, events=pa.Table.from_pylist(event_rows, schema=world.events.schema))
    until = list(world.roster.column("observed_until").to_pylist())
    until[0] = t0 + timedelta(days=14)
    exited = [None] * world.config.n_units
    exited[1] = t0 + timedelta(days=1)
    roster = world.roster.append_column(
        "entered_at", pa.nulls(world.config.n_units, type=pa.timestamp("us", tz="UTC"))
    ).append_column("exited_at", pa.array(exited, type=pa.timestamp("us", tz="UTC")))
    roster = roster.set_column(
        roster.column_names.index("observed_until"),
        "observed_until",
        pa.array(until, type=pa.timestamp("us", tz="UTC")),
    )
    unit_source = UnitSource(
        "world_roster",
        "unit_id",
        entered_at="entered_at",
        exited_at="exited_at",
        observed_from="observed_from",
        observed_until="observed_until",
    )
    world = replace(world, roster=roster, units={"user": unit_source})

    def materialize(candidate):
        con = ibis.duckdb.connect()
        try:
            for name, table in (
                ("world_roster", candidate.roster),
                ("world_events", candidate.events),
                ("world_attributes", candidate.attributes),
            ):
                con.create_table(name, table)
            with TemporaryDirectory(prefix="terrine-observation-exit-") as directory:
                model = tr.Model.from_catalog(
                    _catalog_file(directory),
                    con=con,
                    readiness=candidate.readiness,
                    units=candidate.units,
                    event_ids=candidate.event_ids,
                )
                design = tr.Design(
                    "user",
                    tr.FixedDate(candidate.config.t0),
                    treatment=tr.Treatment("baseline_enabled", known_at_t0=True),
                    features={
                        "x": tr.asof("engagement_score", before="0d"),
                        "plan_tier": tr.asof("plan_tier", before="0d"),
                    },
                    outcomes={"net_revenue": tr.agg("net_revenue", window=("0d", "28d"))},
                )
                return design.build(model).materialize()
        finally:
            con.disconnect()

    frame = materialize(world)
    unit_ids = frame.table.column(frame.roles.unit).to_pylist()
    assert set(unit_ids) == set(range(1, world.config.n_units + 1))
    first = unit_ids.index(1)
    second = unit_ids.index(2)
    meta = frame.columns["net_revenue"]
    assert frame.table.num_rows == world.config.n_units
    assert frame.table.column("net_revenue").null_count == 1
    assert frame.table.column(meta.complete_column).to_pylist()[first] is False
    assert frame.table.column(meta.reason_column).to_pylist()[first] == "incomplete_observation"
    assert frame.table.column(meta.observed_until_column).to_pylist()[first] == t0 + timedelta(
        days=14
    )
    assert frame.table.column(meta.complete_column).to_pylist()[second] is True
    for estimator in _ESTIMATORS:
        with pytest.raises(TerrineError) as exc:
            estimator(frame, outcome="net_revenue")
        assert exc.value.code == "terrine.estimate.incomplete_followup"

    exact_until = list(world.roster.column("observed_until").to_pylist())
    exact_until[0] = t0 + timedelta(days=28)
    exact_roster = world.roster.set_column(
        world.roster.column_names.index("observed_until"),
        "observed_until",
        pa.array(exact_until, type=pa.timestamp("us", tz="UTC")),
    )
    exact = materialize(replace(world, roster=exact_roster))
    exact_first = exact.table.column(exact.roles.unit).to_pylist().index(1)
    exact_complete = exact.table.column(exact.columns["net_revenue"].complete_column).to_pylist()
    assert exact_complete[exact_first] is True


def test_grace_descriptive_event_anchors_are_not_causal_baseline_rows():
    world = simulate(WorldConfig(n_units=12), seed=101)
    extension = pa.table(
        {
            "event_id": ["feature-enabled-a", "feature-enabled-b", "feature-enabled-c"],
            "unit_id": pa.array([1, 1, 2], type=pa.int64()),
            "ts": pa.array(
                [
                    world.config.t0 + timedelta(days=2),
                    world.config.t0 + timedelta(days=3),
                    world.config.t0 + timedelta(days=4),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "kind": ["feature_enabled"] * 3,
            "value": [1.0] * 3,
        }
    )
    world = replace(world, events=pa.concat_tables([world.events, extension]))
    with TemporaryDirectory(prefix="terrine-grace-description-") as directory:
        catalog_path = _catalog_file(directory)
        catalog = load_catalog(catalog_path)
        sources = dict(catalog.sources)
        sources["feature_events"] = Source(
            relation={
                "kind": "sql",
                "sql": "SELECT * FROM world_events WHERE kind = 'feature_enabled'",
            },
            timestamp="ts",
            entities={"user": "unit_id"},
            facts=("feature_enabled",),
            properties=(),
            dimensions=(),
        )
        facts = dict(catalog.facts)
        facts["feature_enabled"] = Fact(source="feature_events", column=None)
        descriptive_catalog = catalog.model_copy(update={"sources": sources, "facts": facts})
        con = ibis.duckdb.connect()
        try:
            con.create_table("world_roster", world.roster)
            con.create_table("world_events", world.events)
            con.create_table("world_attributes", world.attributes)
            model = tr.Model.from_catalog(
                descriptive_catalog,
                con=con,
                readiness={**world.readiness, "feature_events": world.readiness["world_events"]},
                units=world.units,
                event_ids={"world_events": "event_id", "feature_events": "event_id"},
            )
            design = tr.Design(
                "user",
                EventAnchor("feature_enabled", which="each"),
                outcomes={"net_revenue": tr.agg("net_revenue", window=("0d", "1d"))},
            )
            frame = design.build(model).materialize()
            assert frame.table.num_rows == 3
            assert frame.table.column(frame.roles.unit).to_pylist().count(1) == 2
            with pytest.raises(TerrineError) as exc:
                aipw(frame, outcome="net_revenue")
            assert exc.value.code == "terrine.estimate.treatment_required"
            assert "value" not in exc.value.context
        finally:
            con.disconnect()
