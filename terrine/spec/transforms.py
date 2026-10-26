"""Portable fixed limits and exact training-fitted winsorization identities."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from terrine.errors import RefusalSpec, freeze, thaw

__all__ = ["Winsorization", "WinsorizationFit"]

INVALID_WINSORIZATION = RefusalSpec(
    "terrine.transform.invalid_winsorization", "Invalid winsorization: {reason}. {route}"
)


def _invalid(reason: str):
    return INVALID_WINSORIZATION.error(
        reason=reason, route="Use finite explicit Decimal limits and a supported frozen transform."
    )


def decimal_value(value: Decimal | str | int) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, str, int)):
        raise _invalid("limits and probabilities must be Decimal, decimal strings, or integers")
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise _invalid("malformed decimal value") from error
    if not result.is_finite():
        raise _invalid("limits and probabilities must be finite")
    return result


def decimal_string(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if value == 0 else text


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _digest(value: str) -> bool:
    return (
        isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
    )


@dataclass(frozen=True, slots=True)
class WinsorizationFit:
    definition_hash: str
    view_hash: str
    design_hash: str
    model_hash: str
    feature_definition_hash: str
    population_hash: str
    snapshot: Mapping[str, Any]
    split_policy: Mapping[str, Any]
    probabilities: tuple[Decimal, Decimal]
    row_count: int
    non_null_count: int
    algorithm: str = "linear_type7"
    version: str = "1"

    def __post_init__(self) -> None:
        for name in (
            "definition_hash",
            "view_hash",
            "design_hash",
            "model_hash",
            "feature_definition_hash",
            "population_hash",
        ):
            if not _digest(getattr(self, name)):
                raise _invalid(f"invalid {name}")
        probabilities = tuple(decimal_value(value) for value in self.probabilities)
        if len(probabilities) != 2 or not Decimal(0) <= probabilities[0] <= probabilities[
            1
        ] <= Decimal(1):
            raise _invalid("probabilities must be ordered in [0, 1]")
        if self.algorithm != "linear_type7" or self.version != "1":
            raise _invalid("unsupported threshold algorithm or version")
        if (
            any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (self.row_count, self.non_null_count)
            )
            or not 0 < self.non_null_count <= self.row_count
        ):
            raise _invalid("fit counts must describe a nonempty non-null training population")
        if not isinstance(self.snapshot, Mapping) or not isinstance(self.split_policy, Mapping):
            raise _invalid("snapshot and split identity must be mappings")
        if self.split_policy.get("kind") not in ("time", "group"):
            raise _invalid("fit requires a declared temporal or group split")
        object.__setattr__(self, "probabilities", probabilities)
        object.__setattr__(self, "snapshot", freeze(self.snapshot))
        object.__setattr__(self, "split_policy", freeze(self.split_policy))

    def to_record(self) -> dict[str, Any]:
        return {
            "definition_hash": self.definition_hash,
            "view_hash": self.view_hash,
            "design_hash": self.design_hash,
            "model_hash": self.model_hash,
            "feature_definition_hash": self.feature_definition_hash,
            "population_hash": self.population_hash,
            "snapshot": thaw(self.snapshot),
            "split_policy": thaw(self.split_policy),
            "probabilities": [decimal_string(value) for value in self.probabilities],
            "row_count": self.row_count,
            "non_null_count": self.non_null_count,
            "algorithm": self.algorithm,
            "version": self.version,
        }

    @classmethod
    def from_record(cls, value: Any) -> WinsorizationFit:
        fields = {
            "definition_hash",
            "view_hash",
            "design_hash",
            "model_hash",
            "feature_definition_hash",
            "population_hash",
            "snapshot",
            "split_policy",
            "probabilities",
            "row_count",
            "non_null_count",
            "algorithm",
            "version",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise _invalid("invalid fit record")
        if (
            not isinstance(value["probabilities"], list)
            or len(value["probabilities"]) != 2
            or any(not isinstance(p, str) for p in value["probabilities"])
        ):
            raise _invalid("serialized probabilities must be two decimal strings")
        record: dict[str, Any] = dict(value)
        record["probabilities"] = tuple(decimal_value(p) for p in value["probabilities"])
        return cls(**record)


@dataclass(frozen=True, slots=True)
class Winsorization:
    column: str
    lower: Decimal | str | int
    upper: Decimal | str | int
    stage: str = "post_aggregation"
    source_type: str | None = None
    output_precision: int | None = None
    output_scale: int | None = None
    fit: WinsorizationFit | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.column, str) or not self.column:
            raise _invalid("column must be a nonempty feature name")
        lower, upper = decimal_value(self.lower), decimal_value(self.upper)
        lower, upper = Decimal(decimal_string(lower)), Decimal(decimal_string(upper))
        if lower > upper or self.stage != "post_aggregation":
            raise _invalid("limits must be ordered and the stage must be post_aggregation")
        typed = (self.source_type, self.output_precision, self.output_scale)
        if any(item is not None for item in typed):
            if (
                not isinstance(self.source_type, str)
                or not self.source_type
                or isinstance(self.output_precision, bool)
                or not isinstance(self.output_precision, int)
                or isinstance(self.output_scale, bool)
                or not isinstance(self.output_scale, int)
                or not 0 <= self.output_scale <= self.output_precision <= 38
                or self.output_precision < 1
            ):
                raise _invalid("invalid typed output limits")
        if self.fit is not None and (
            not isinstance(self.fit, WinsorizationFit) or self.source_type is None
        ):
            raise _invalid("a fitted transform requires its typed training identity")
        object.__setattr__(self, "lower", lower)
        object.__setattr__(self, "upper", upper)

    def to_record(self) -> dict[str, Any]:
        return {
            "kind": "winsorization",
            "schema_version": 1,
            "column": self.column,
            "lower": decimal_string(decimal_value(self.lower)),
            "upper": decimal_string(decimal_value(self.upper)),
            "stage": self.stage,
            "source_type": self.source_type,
            "output_precision": self.output_precision,
            "output_scale": self.output_scale,
            "fit": None if self.fit is None else self.fit.to_record(),
        }

    def to_json(self) -> bytes:
        return _canonical(self.to_record())

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.to_json()).hexdigest()

    @classmethod
    def from_record(cls, value: Any) -> Winsorization:
        fields = {
            "kind",
            "schema_version",
            "column",
            "lower",
            "upper",
            "stage",
            "source_type",
            "output_precision",
            "output_scale",
            "fit",
        }
        if (
            not isinstance(value, dict)
            or set(value) != fields
            or value["kind"] != "winsorization"
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
            or not isinstance(value["lower"], str)
            or not isinstance(value["upper"], str)
        ):
            raise _invalid("invalid transform record or version")
        return cls(
            value["column"],
            lower=value["lower"],
            upper=value["upper"],
            stage=value["stage"],
            source_type=value["source_type"],
            output_precision=value["output_precision"],
            output_scale=value["output_scale"],
            fit=None if value["fit"] is None else WinsorizationFit.from_record(value["fit"]),
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> Winsorization:
        from terrine.spec.dataset import _bad_constant, _unique_pairs

        try:
            return cls.from_record(
                json.loads(value, object_pairs_hook=_unique_pairs, parse_constant=_bad_constant)
            )
        except (ValueError, TypeError, KeyError) as error:
            raise _invalid("malformed serialized transform") from error
