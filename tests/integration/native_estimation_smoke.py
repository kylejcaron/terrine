"""Subprocess entry point proving native baseline estimation avoids increment."""

from __future__ import annotations

import importlib.abc
import sys
from importlib.resources import files
from tempfile import TemporaryDirectory


class _NoIncrement(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "increment" or fullname.startswith("increment."):
            raise AssertionError("native baseline estimation imported optional increment")
        return None


sys.meta_path.insert(0, _NoIncrement())

import ibis  # noqa: E402

import terrine as tr  # noqa: E402
from terrine.estimate import aipw, ipw, regression_adjustment  # noqa: E402
from terrine.simulate import WorldConfig, simulate  # noqa: E402

world = simulate(WorldConfig(n_units=120), seed=73)
con = ibis.duckdb.connect()
try:
    con.create_table("world_roster", world.roster)
    con.create_table("world_events", world.events)
    con.create_table("world_attributes", world.attributes)
    with TemporaryDirectory(prefix="terrine-no-increment-") as directory:
        catalog_path = f"{directory}/baseline.yaml"
        with open(catalog_path, "w", encoding="utf-8") as stream:
            stream.write(files("terrine.simulate").joinpath("baseline.yaml").read_text())
        model = tr.Model.from_catalog(
            catalog_path,
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
        )
        frame = design.build(model).materialize()
        for estimator in (ipw, aipw, regression_adjustment):
            result = estimator(frame, outcome="net_revenue", folds=5, seed=17)
            assert result.n == world.config.n_units
            assert result.n_clusters == world.config.n_units
finally:
    con.disconnect()
