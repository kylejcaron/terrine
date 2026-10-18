"""Configuration and Arrow containers for independent baseline and panel worlds."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Annotated, Any, Literal

import pyarrow as pa
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from terrine._time import utc_instant
from terrine.data import Coverage, UnitSource
from terrine.errors import freeze


class PanelWorldConfig(BaseModel):
    """Two-cohort potential-outcome law for repeated-trial calibration."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    n_units: Annotated[int, Field(strict=True, gt=0, le=2**63 - 1)] = 2000
    effect_kind: Literal["zero", "heterogeneous"] = "heterogeneous"
    trial_masses: Literal["equal", "unequal"] = "equal"
    overlap: Literal["disjoint", "overlap"] = "disjoint"
    assignment_profile: Literal["profile_a", "profile_b"] = "profile_a"
    outcome_noise_sd: Annotated[float, Field(ge=0)] = 2


@dataclass(frozen=True, slots=True)
class PanelWorld:
    """Observed repeated-trial rows and their segregated paired-outcome oracle."""

    config: PanelWorldConfig
    metadata: Mapping[str, Any]
    observations: pa.Table
    oracle: pa.Table

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", freeze(self.metadata))


class WorldConfig(BaseModel):
    """Fixed-cohort data-generating parameters; durations are elapsed seconds."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    n_units: Annotated[int, Field(strict=True, gt=0, le=2**63 - 1)] = 2000
    t0: AwareDatetime = datetime(2025, 3, 3, 12, 34, 56, tzinfo=UTC)
    lookback_seconds: Annotated[int, Field(strict=True, gt=0, le=2**63 - 1)] = 2419200
    horizon_seconds: Annotated[int, Field(strict=True, gt=0, le=2**63 - 1)] = 2419200
    propensity_quadratic: float = 0
    outcome_quadratic: float = 0
    outcome_noise_sd: Annotated[float, Field(ge=0)] = 2
    target_weight_slope: Annotated[float, Field(gt=-1, lt=1)] = 0

    @field_validator("t0", mode="before")
    @classmethod
    def utc_time_zero(cls, value: object) -> object:
        if not isinstance(value, (datetime, str)):
            return value
        try:
            return utc_instant(value)
        except OverflowError as exc:
            raise ValueError("time zero must fit UTC datetime's range") from exc

    @model_validator(mode="after")
    def representable_endpoints(self) -> WorldConfig:
        try:
            self.t0 - timedelta(seconds=self.lookback_seconds)
            end = self.t0 + timedelta(seconds=self.horizon_seconds)
            end.date() + timedelta(days=1)
        except OverflowError as exc:
            raise ValueError(
                "measurement and calendar coverage endpoints must fit datetime's range"
            ) from exc
        return self


@dataclass(frozen=True, slots=True)
class World:
    """Observed physical tables and a segregated paired potential-outcome oracle."""

    config: WorldConfig
    metadata: Mapping[str, Any]
    roster: pa.Table
    events: pa.Table
    attributes: pa.Table
    analysis_table: pa.Table
    oracle: pa.Table
    semantic_yaml: str
    readiness: Mapping[str, Coverage]
    units: Mapping[str, UnitSource]
    event_ids: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", freeze(self.metadata))
        for name in ("readiness", "units", "event_ids"):
            object.__setattr__(self, name, MappingProxyType(dict(getattr(self, name))))
