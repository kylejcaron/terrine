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

    def reads(self) -> tuple[ibis.Table, ...]:
        return (self.table,)

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
    n_treated_name = "__terrine_matched_n_treated"
    n_controls_name = "__terrine_matched_n_controls"
    counts = table.group_by(keys).aggregate(
        **{
            n_treated_name: treated.cast("int64").sum(),
            n_controls_name: controls.cast("int64").sum(),
        }
    )
    required = counts[n_treated_name] * sample.controls_per_treated
    insufficiency = counts.filter(counts[n_controls_name] < required).select(
        *[counts[key] for key in keys],
        counts[n_controls_name].name("n_controls"),
        required.name("n_required"),
    )
    controls_table = table.filter(controls)
    control_keys = [controls_table[key].identical_to(counts[key]) for key in keys]
    controls_with_counts = controls_table.join(counts, control_keys, how="inner")
    n_sampled = controls_with_counts[n_treated_name] * sample.controls_per_treated
    rank = ibis.row_number().over(
        group_by=[controls_with_counts[key] for key in keys],
        order_by=[
            (controls_with_counts.unit.cast("string") + str(sample.seed)).hash(),
            controls_with_counts.unit.cast("string"),
        ],
    )
    selected_controls = controls_with_counts.filter(rank < n_sampled).select(
        **{name: controls_with_counts[name] for name in table.columns},
        assignment_probability=n_sampled / controls_with_counts[n_controls_name],
    )
    treated_table = table.filter(treated)
    treated_keys = [treated_table[key].identical_to(counts[key]) for key in keys]
    treated_with_counts = treated_table.join(counts, treated_keys, how="inner")
    selected_treated = treated_with_counts.select(
        **{name: treated_with_counts[name] for name in table.columns},
        assignment_probability=1.0,
    )
    return selected_treated.union(selected_controls), InsufficientControlsCheck(insufficiency)
