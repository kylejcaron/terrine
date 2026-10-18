"""Estimate fixed-cohort baseline adoption effects from a synthetic native world."""

from __future__ import annotations

from importlib.resources import files
from tempfile import TemporaryDirectory

import ibis

import terrine as tr
from terrine.estimate import aipw
from terrine.simulate import WorldConfig, simulate


def main() -> None:
    world = simulate(WorldConfig(), seed=20261017)
    con = ibis.duckdb.connect()
    try:
        con.create_table("world_roster", world.roster)
        con.create_table("world_events", world.events)
        con.create_table("world_attributes", world.attributes)
        with TemporaryDirectory(prefix="terrine-baseline-adoption-") as directory:
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
                treatment=tr.Treatment("baseline_enabled", at="0d", known_at_t0=True),
                features={
                    "x": tr.asof("engagement_score", before="0d"),
                    "plan_tier": tr.asof("plan_tier", before="0d"),
                },
                outcomes={"net_revenue": tr.agg("net_revenue", window=("0d", "28d"))},
            )
            frame = design.build(model).materialize()
            estimate = aipw(frame, outcome="net_revenue", folds=5, seed=17)
            print(f"ATE ({estimate.estimand}): {estimate.value:.6g}")
            print(f"SE: {estimate.se:.6g}")
            print(
                f"{100 * (1 - estimate.alpha):.1f}% CI: "
                f"[{estimate.ci[0]:.6g}, {estimate.ci[1]:.6g}]"
            )
            for diagnostic in estimate.diagnostics:
                print(f"{diagnostic.name}: {diagnostic.status} {dict(diagnostic.metrics)}")
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
