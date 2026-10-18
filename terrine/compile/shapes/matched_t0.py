"""Deterministic matched-t0 risk-set sampling for sequential trials."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import ibis
import pyarrow as pa

from terrine.errors import RefusalSpec
from terrine.spec.panels import MatchedT0Sample

_INSUFFICIENT = RefusalSpec(
    "terrine.compile.insufficient_matched_t0_controls",
    "Matched-t0 stratum at {t0!r} has {controls} eligible controls but requires {required}.",
)


@dataclass(frozen=True, slots=True)
class InsufficientControlsCheck:
    table: ibis.Table

    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None:
        result = execute(self.table)
        if not isinstance(result, pa.Table):
            raise TypeError("The supplied executor must return a PyArrow table.")
        if result.num_rows:
            row = result.slice(0, 1).to_pylist()[0]
            raise _INSUFFICIENT.error(
                t0=str(row["t0"]), controls=row["n_controls"], required=row["n_required"]
            )


def sample_risk_sets(table, sample: MatchedT0Sample):
    keys = ["t0", *sample.strata]
    treated = table.treatment.fill_null(False)
    controls = ~treated
    counts = table.group_by(keys).aggregate(
        n_treated=treated.cast("int64").sum(),
        n_controls=controls.cast("int64").sum(),
    )
    required = counts.n_treated * sample.controls_per_treated
    insufficiency = counts.filter(counts.n_controls < required).select(
        *[counts[key] for key in keys],
        counts.n_controls,
        required.name("n_required"),
    )
    joined = table.join(
        counts,
        [table[key].identical_to(counts[key]) for key in keys],
        how="inner",
    )
    rank = ibis.row_number().over(
        group_by=[joined[key] for key in keys],
        order_by=[
            (joined.unit.cast("string") + str(sample.seed)).hash(),
            joined.unit.cast("string"),
        ],
    )
    n_sampled = joined.n_treated * sample.controls_per_treated
    probability = joined.treatment.ifelse(1.0, n_sampled / joined.n_controls)
    selected = joined.filter(joined.treatment | (rank <= n_sampled)).mutate(
        assignment_probability=probability
    )
    return (
        selected.select(
            **{name: joined[name] for name in table.columns},
            assignment_probability=selected.assignment_probability,
        ),
        InsufficientControlsCheck(insufficiency),
    )
