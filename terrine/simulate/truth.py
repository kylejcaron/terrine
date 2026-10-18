"""Analytical population targets and realized paired-outcome cohort targets."""

from math import fsum, isfinite
from typing import Literal

from terrine.errors import RefusalSpec
from terrine.simulate.world import PanelWorld, World

_INVALID_ORACLE = RefusalSpec(
    "terrine.simulate.invalid_oracle",
    "Cannot evaluate cohort truth ({reason}); "
    "supply finite paired outcomes and positive target weights.",
)


def ate(
    world: World, *, target: Literal["population", "cohort"] = "population", weighted: bool = False
) -> float:
    """Return the fixed-baseline ATE, never a fitted or compiled estimate.

    E[X]=0, E[X²]=1/3, P(plus)=.3 and q=1+sX give population
    E[q delta]/E[q] = 2+.5(.3)+.75s/3. Cohort truth instead averages
    the recorded paired differences, including their floating-point rounding.

    Args:
        world: Observed world with its segregated paired potential outcomes.
        target: Analytical population or realized-cohort target.
        weighted: Whether to use the declared baseline-function target weights.

    Returns:
        The population formula or normalized realized paired-outcome average.

    Raises:
        ValueError: The requested target is not population or cohort.
        TypeError: The weighting flag is not Boolean.
        TerrineError: Cohort outcomes or weights cannot define a finite target.
    """
    if target not in ("population", "cohort"):
        raise ValueError("target must be 'population' or 'cohort'")
    if not isinstance(weighted, bool):
        raise TypeError("weighted must be a bool")
    if target == "population":
        return 2 + 0.5 * 0.3 + (0.75 * world.config.target_weight_slope / 3 if weighted else 0)
    rows = world.oracle.select(["y0", "y1", "target_weight"]).to_pylist()
    if not rows:
        raise _INVALID_ORACLE.error(reason="empty oracle")
    differences = [row["y1"] - row["y0"] for row in rows]
    weights = [row["target_weight"] if weighted else 1.0 for row in rows]
    if any(not isfinite(value) for value in differences) or any(
        not isfinite(value) or value <= 0 for value in weights
    ):
        raise _INVALID_ORACLE.error(reason="nonfinite outcomes or nonpositive weights")
    scale = max(weights)
    scaled = [value / scale for value in weights]
    total = fsum(scaled)
    return fsum(
        (weight / total) * difference
        for weight, difference in zip(scaled, differences, strict=True)
    )


def panel_ate(
    world: PanelWorld, *, target: Literal["population", "cohort"] = "population"
) -> float:
    """Return equal-unit or realized-cohort trial-weighted truth for a panel world."""
    if target not in ("population", "cohort"):
        raise ValueError("target must be 'population' or 'cohort'")
    config = world.config
    masses = (0.5, 0.5) if config.trial_masses == "equal" else (0.25, 0.75)
    if target == "population":
        effects = (0.0, 0.0) if config.effect_kind == "zero" else (1.5, 4.5)
    else:
        rows = world.oracle.to_pylist()
        if not rows:
            raise _INVALID_ORACLE.error(reason="empty panel oracle")
        effects_by_trial = {}
        for trial in ("early", "late"):
            values = [row["y1"] - row["y0"] for row in rows if row["trial"] == trial]
            if not values or any(not isfinite(value) for value in values):
                raise _INVALID_ORACLE.error(reason="invalid panel potential outcomes")
            effects_by_trial[trial] = fsum(values) / len(values)
        effects = (effects_by_trial["early"], effects_by_trial["late"])
    return fsum(mass * effect for mass, effect in zip(masses, effects, strict=True))
