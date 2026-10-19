"""Deterministic, unit-level sampling for nuisance-model training only."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

type UnitId = int | str


@dataclass(frozen=True, slots=True)
class NuisanceSample:
    """Bound nuisance training rows without changing the analysis population."""

    max_rows: int
    seed: int
    strata: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_rows, bool)
            or not isinstance(self.max_rows, int)
            or self.max_rows < 1
        ):
            raise ValueError("max_rows must be a positive integer")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int) or self.seed < 0:
            raise ValueError("seed must be a nonnegative integer")
        strata = tuple(self.strata)
        if any(not isinstance(name, str) or not name.strip() for name in strata):
            raise ValueError("strata must contain non-empty column names")
        if len(set(strata)) != len(strata):
            raise ValueError("strata names must be unique")
        object.__setattr__(self, "strata", strata)


@dataclass(frozen=True, slots=True)
class NuisanceSelection:
    selected_units: tuple[UnitId, ...]
    inclusion_probability: Mapping[UnitId, float]
    inverse_inclusion_weight: Mapping[UnitId, float]


def _unit_bytes(unit: UnitId) -> bytes:
    if isinstance(unit, bool) or not isinstance(unit, (int, str)):
        raise TypeError("sampling unit identifiers must be integers or strings")
    return (b"i:" + str(unit).encode()) if isinstance(unit, int) else b"s:" + unit.encode()


def _rank(seed: int, unit: UnitId) -> bytes:
    digest = hashlib.sha256()
    digest.update(seed.to_bytes(max(1, (seed.bit_length() + 7) // 8), "big"))
    digest.update(b"\0")
    digest.update(_unit_bytes(unit))
    return digest.digest()


def select_nuisance_units(
    units: list[UnitId] | tuple[UnitId, ...],
    policy: NuisanceSample,
    *,
    strata: dict[UnitId, tuple[object, ...]] | None = None,
) -> NuisanceSelection:
    """Select a deterministic unit sample with known stratum inclusion rates."""
    values = tuple(units)
    encoded = [_unit_bytes(unit) for unit in values]
    if len(set(encoded)) != len(encoded):
        raise ValueError("sampling unit identifiers must be unique")
    groups: dict[tuple[object, ...], list[UnitId]]
    if strata is None:
        if policy.strata:
            raise ValueError("stratum values are required by the sampling policy")
        groups = {(): list(values)}
    else:
        if not policy.strata:
            raise ValueError("stratum values require declared stratum columns")
        if set(strata) != set(values):
            raise ValueError("strata must provide exactly one value for every unit")
        groups_by_key: dict[tuple[object, ...], list[UnitId]] = defaultdict(list)
        for unit in values:
            group = tuple(strata[unit])
            if len(group) != len(policy.strata):
                raise ValueError("each stratum value must match the declared columns")
            groups_by_key[group].append(unit)
        groups = dict(groups_by_key)
    if len(values) <= policy.max_rows:
        selected = values
        probabilities = {unit: 1.0 for unit in selected}
    else:
        keys = sorted(groups, key=repr)
        if policy.max_rows < len(keys):
            raise ValueError("max_rows must retain at least one unit per stratum")
        quotas = {key: 1 for key in keys}
        remaining = policy.max_rows - len(keys)
        while remaining:
            available = [key for key in keys if quotas[key] < len(groups[key])]
            if not available:
                break
            total = sum(len(groups[key]) for key in available)
            shares = {key: remaining * len(groups[key]) / total for key in available}
            increments = {
                key: min(len(groups[key]) - quotas[key], int(shares[key])) for key in available
            }
            progressed = sum(increments.values())
            for key, count in increments.items():
                quotas[key] += count
            remaining -= progressed
            if remaining:
                ranked = sorted(
                    (key for key in available if quotas[key] < len(groups[key])),
                    key=lambda key: (-(shares[key] % 1), repr(key)),
                )
                if not ranked:
                    break
                for key in ranked[:remaining]:
                    quotas[key] += 1
                    remaining -= 1
        chosen: list[UnitId] = []
        probabilities: dict[UnitId, float] = {}
        for key in keys:
            group = groups[key]
            quota = quotas[key]
            ranked_units = sorted(
                group, key=lambda unit: (_rank(policy.seed, unit), _unit_bytes(unit))
            )
            chosen.extend(ranked_units[:quota])
            probability = quota / len(group)
            probabilities.update({unit: probability for unit in ranked_units[:quota]})
        selected = tuple(sorted(chosen, key=_unit_bytes))
    return NuisanceSelection(
        selected,
        MappingProxyType(probabilities),
        MappingProxyType({unit: 1.0 / probability for unit, probability in probabilities.items()}),
    )


__all__ = ["NuisanceSample", "NuisanceSelection", "select_nuisance_units"]
