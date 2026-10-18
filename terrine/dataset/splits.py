"""Deterministic temporal and typed-group split policies."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any, Literal

__all__ = [
    "TimeSplit",
    "GroupSplit",
    "SplitAssignment",
    "group_bucket",
    "assign_splits",
    "assign_group_splits",
    "reason_counts",
]


def _instant(value: date | datetime | str) -> datetime:
    if isinstance(value, str):
        try:
            value = date.fromisoformat(value) if len(value) == 10 else datetime.fromisoformat(value)
        except ValueError as error:
            raise ValueError(
                "split boundaries must be explicit ISO dates or aware instants"
            ) from error
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("split datetime boundaries must be timezone-aware")
        return value.astimezone(UTC)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), UTC)
    raise TypeError("split boundary must be date, aware datetime, or ISO string")


def _duration(value: str | timedelta) -> timedelta:
    if isinstance(value, timedelta):
        result = value
    elif isinstance(value, str):
        import re

        match = re.fullmatch(r"(\d+(?:\.\d+)?)([dhms])", value)
        if not match:
            raise ValueError("embargo must be elapsed duration in d/h/m/s")
        result = timedelta(
            **{
                "d": {"days": float(match[1])},
                "h": {"hours": float(match[1])},
                "m": {"minutes": float(match[1])},
                "s": {"seconds": float(match[1])},
            }[match[2]]
        )
    else:
        raise TypeError("embargo must be a duration string or timedelta")
    if result < timedelta(0):
        raise ValueError("embargo must be nonnegative")
    return result


@dataclass(frozen=True, slots=True, init=False)
class TimeSplit:
    train_until: datetime
    validation_until: datetime | None
    embargo: timedelta
    groups: GroupSplit | None

    def __init__(
        self,
        *,
        train_until: date | datetime | str,
        validation_until: date | datetime | str | None = None,
        embargo: str | timedelta = "0d",
        groups: GroupSplit | None = None,
    ) -> None:
        train = _instant(train_until)
        validation = None if validation_until is None else _instant(validation_until)
        if validation is not None and validation <= train:
            raise ValueError("validation_until must follow train_until")
        if groups is not None and not isinstance(groups, GroupSplit):
            raise TypeError("groups must be GroupSplit or None")
        object.__setattr__(self, "train_until", train)
        object.__setattr__(self, "validation_until", validation)
        object.__setattr__(self, "embargo", _duration(embargo))
        object.__setattr__(self, "groups", groups)

    def to_record(self) -> dict[str, Any]:
        return _time_record(self)


@dataclass(frozen=True, slots=True, init=False)
class GroupSplit:
    group: str
    train_fraction: float
    validation_fraction: float
    seed: int

    def __init__(
        self,
        *,
        group: str = "unit",
        train_fraction: float = 0.8,
        validation_fraction: float = 0.1,
        seed: int = 0,
    ) -> None:
        if not isinstance(group, str) or not group:
            raise ValueError("group must be non-empty")
        if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed < 2**63:
            raise ValueError("seed must be a nonnegative signed-Int64 integer")
        for fraction in (train_fraction, validation_fraction):
            if (
                isinstance(fraction, bool)
                or not isinstance(fraction, (int, float))
                or not math.isfinite(fraction)
            ):
                raise ValueError("split fractions must be finite numbers")
        if (
            not 0 < train_fraction < 1
            or validation_fraction < 0
            or train_fraction + validation_fraction > 1
        ):
            raise ValueError("split fractions must be positive and sum to at most one")
        object.__setattr__(self, "group", group)
        object.__setattr__(self, "train_fraction", float(train_fraction))
        object.__setattr__(self, "validation_fraction", float(validation_fraction))
        object.__setattr__(self, "seed", seed)

    def to_record(self) -> dict[str, Any]:
        return {
            "kind": "group",
            "group": self.group,
            "train_fraction": str(self.train_fraction),
            "validation_fraction": str(self.validation_fraction),
            "seed": self.seed,
            "codec": "terrine.group.v1",
        }


def _time_record(policy: TimeSplit) -> dict[str, Any]:
    return {
        "kind": "time",
        "train_until": policy.train_until.isoformat(),
        "validation_until": None
        if policy.validation_until is None
        else policy.validation_until.isoformat(),
        "embargo_microseconds": policy.embargo // timedelta(microseconds=1),
        "groups": None if policy.groups is None else policy.groups.to_record(),
    }


def group_bucket(key: str | int, policy: GroupSplit) -> Literal["train", "validation", "test"]:
    if isinstance(key, bool) or not isinstance(key, (str, int)):
        raise TypeError("group keys must be strings or integers, excluding bool")
    if not isinstance(policy, GroupSplit):
        raise TypeError("policy must be GroupSplit")
    kind, value = ("str", key) if isinstance(key, str) else ("int", str(key))
    payload = json.dumps(
        ["terrine.group.v1", str(policy.seed), kind, value],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    h = int.from_bytes(hashlib.sha256(payload).digest(), "big")
    train = Fraction(Decimal(str(policy.train_fraction)))
    validation = Fraction(Decimal(str(policy.validation_fraction)))
    scale = 1 << 256
    if h < train.numerator * scale // train.denominator:
        return "train"
    if h < (train + validation).numerator * scale // (train + validation).denominator:
        return "validation"
    return "test"


def assign_group_splits(
    rows: list[dict[str, Any]], policy: GroupSplit, *, key_field: str | None = None
) -> dict[Any, SplitAssignment]:
    """Assign repeated group keys to one deterministic bucket independent of row order."""
    field = policy.group if key_field is None else key_field
    if not isinstance(field, str) or not field:
        raise ValueError("key_field must be non-empty")
    assignments: dict[Any, SplitAssignment] = {}
    for row in rows:
        identity = row["id"]
        key = row.get(field)
        if key is None:
            assignments[identity] = SplitAssignment("unassigned", "group_missing")
        else:
            assignments[identity] = SplitAssignment(group_bucket(key, policy))
    return assignments


@dataclass(frozen=True, slots=True)
class SplitAssignment:
    split: Literal["train", "validation", "test", "unassigned"]
    reason: str | None = None


def assign_splits(
    rows: list[dict[str, Any]], policy: TimeSplit, *, group_keys: dict[Any, str | int] | None = None
) -> dict[Any, SplitAssignment]:
    """Assign origins; purge any selected label reaching its split's next boundary."""
    output: dict[Any, SplitAssignment] = {}
    for row in rows:
        key = row["id"]
        origin = _instant(row["origin"])
        if origin < policy.train_until:
            bucket, boundary = "train", policy.train_until
        elif (
            policy.validation_until is not None
            and origin >= policy.train_until + policy.embargo
            and origin < policy.validation_until
        ):
            bucket, boundary = "validation", policy.validation_until
        elif (
            policy.validation_until is not None
            and origin >= policy.validation_until + policy.embargo
        ):
            bucket, boundary = "test", None
        elif policy.validation_until is None and origin >= policy.train_until + policy.embargo:
            bucket, boundary = "validation", None
        else:
            output[key] = SplitAssignment("unassigned", "embargo")
            continue
        end_value = row.get("label_end")
        if end_value is not None and boundary is not None:
            end = _instant(end_value)
            available = end < boundary if row.get("point_label", False) else end <= boundary
            if not available:
                output[key] = SplitAssignment("unassigned", "label_overlap")
                continue
        if policy.groups is not None:
            group = (group_keys or {}).get(key, row.get(policy.groups.group))
            if group is None:
                output[key] = SplitAssignment("unassigned", "group_missing")
                continue
            if group_bucket(group, policy.groups) != bucket:
                output[key] = SplitAssignment("unassigned", "split_mismatch")
                continue
        output[key] = SplitAssignment(bucket)
    return output


def reason_counts(assignments: dict[Any, SplitAssignment]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for assignment in assignments.values():
        if assignment.reason is not None:
            counts[assignment.reason] = counts.get(assignment.reason, 0) + 1
    return dict(sorted(counts.items()))
