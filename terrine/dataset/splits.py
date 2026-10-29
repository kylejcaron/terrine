"""Deterministic temporal and typed-group split policies."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, tzinfo
from decimal import Decimal
from fractions import Fraction
from typing import Any, Literal

from terrine.errors import RefusalSpec, TerrineError
from terrine.semantic.time import instant

__all__ = [
    "TimeSplit",
    "GroupSplit",
    "SplitAssignment",
    "group_bucket",
    "assign_splits",
    "assign_group_splits",
    "reason_counts",
]

_INVALID = RefusalSpec("terrine.dataset.invalid_split", "Split input {field} is invalid: {reason}.")


def _boundary(value: date | datetime | str, field: str) -> date | datetime:
    if isinstance(value, datetime):
        return _instant(value, field)
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        if len(value) != 10:
            return _instant(value, field)
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise _INVALID.error(reason="invalid_date", field=field) from error
    raise _INVALID.error(reason="wrong_type", field=field)


def _instant(value: date | datetime | str, field: str) -> datetime:
    if isinstance(value, date) and not isinstance(value, datetime):
        raise _INVALID.error(reason="unbound_calendar_date", field=field)
    if isinstance(value, datetime) and (value.tzinfo is None or value.utcoffset() is None):
        raise _INVALID.error(reason="naive_datetime", field=field)
    if not isinstance(value, (datetime, str)):
        raise _INVALID.error(reason="wrong_type", field=field)
    try:
        return instant(value, "UTC")
    except (ValueError, OverflowError) as error:
        raise _INVALID.error(reason="invalid_instant", field=field) from error


def _order_instant(value: date | datetime, field: str) -> datetime:
    if isinstance(value, datetime):
        return _instant(value, field)
    return datetime.combine(value, datetime.min.time(), UTC)


def _duration(value: str | timedelta) -> timedelta:
    if isinstance(value, timedelta):
        result = value
    elif isinstance(value, str):
        import re

        match = re.fullmatch(r"(\d+(?:\.\d+)?)([dhms])", value)
        if not match:
            raise _INVALID.error(reason="invalid_duration", field="embargo")
        unit = {"d": "days", "h": "hours", "m": "minutes", "s": "seconds"}[match[2]]
        try:
            result = timedelta(**{unit: float(match[1])})
        except OverflowError as error:
            raise _INVALID.error(reason="out_of_range", field="embargo") from error
    else:
        raise _INVALID.error(reason="wrong_type", field="embargo")
    if result < timedelta(0):
        raise _INVALID.error(reason="negative", field="embargo")
    return result


@dataclass(frozen=True, slots=True, init=False)
class TimeSplit:
    train_until: date | datetime
    validation_until: date | datetime | None
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
        train = _boundary(train_until, "train_until")
        validation = (
            None if validation_until is None else _boundary(validation_until, "validation_until")
        )
        if validation is not None and isinstance(validation, datetime) == isinstance(
            train, datetime
        ):
            if _order_instant(validation, "validation_until") <= _order_instant(
                train, "train_until"
            ):
                raise _INVALID.error(reason="validation_not_after_train", field="validation_until")
        if groups is not None and not isinstance(groups, GroupSplit):
            raise _INVALID.error(reason="wrong_type", field="groups")
        object.__setattr__(self, "train_until", train)
        object.__setattr__(self, "validation_until", validation)
        object.__setattr__(self, "embargo", _duration(embargo))
        object.__setattr__(self, "groups", groups)

    def bind(self, catalog_timezone: tzinfo) -> TimeSplit:
        """Resolve calendar-date boundaries at the Catalog-local start of day."""
        if not isinstance(catalog_timezone, tzinfo):
            raise _INVALID.error(reason="wrong_type", field="catalog_timezone")

        def resolve(value: date | datetime | None, field: str) -> datetime | None:
            if value is None:
                return None
            if isinstance(value, datetime):
                return _instant(value, field)
            return datetime.combine(value, datetime.min.time(), catalog_timezone).astimezone(UTC)

        train = resolve(self.train_until, "train_until")
        assert train is not None  # train_until is a required constructor argument
        return TimeSplit(
            train_until=train,
            validation_until=resolve(self.validation_until, "validation_until"),
            embargo=self.embargo,
            groups=self.groups,
        )

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
            raise _INVALID.error(reason="empty_name", field="group")
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise _INVALID.error(reason="wrong_type", field="seed")
        if not 0 <= seed < 2**63:
            raise _INVALID.error(reason="out_of_range", field="seed")
        for field, fraction in (
            ("train_fraction", train_fraction),
            ("validation_fraction", validation_fraction),
        ):
            if isinstance(fraction, bool) or not isinstance(fraction, (int, float)):
                raise _INVALID.error(reason="wrong_type", field=field)
            if not math.isfinite(fraction):
                raise _INVALID.error(reason="not_finite", field=field)
        if not 0 < train_fraction < 1:
            raise _INVALID.error(reason="out_of_range", field="train_fraction")
        if validation_fraction < 0:
            raise _INVALID.error(reason="negative", field="validation_fraction")
        if train_fraction + validation_fraction > 1:
            raise _INVALID.error(reason="fractions_exceed_one", field="validation_fraction")
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


def policy_from_record(record: dict[str, Any]) -> TimeSplit | GroupSplit:
    """Restore a versioned split policy through its ordinary input validation."""
    if not isinstance(record, dict):
        raise _INVALID.error(reason="unknown_record", field="record")
    try:
        if (
            record.get("kind") == "group"
            and set(record)
            == {"kind", "group", "train_fraction", "validation_fraction", "seed", "codec"}
            and record["codec"] == "terrine.group.v1"
        ):
            return GroupSplit(
                group=record["group"],
                train_fraction=float(record["train_fraction"]),
                validation_fraction=float(record["validation_fraction"]),
                seed=record["seed"],
            )
        if record.get("kind") == "time" and set(record) == {
            "kind",
            "train_until",
            "validation_until",
            "embargo_microseconds",
            "groups",
        }:
            groups = None if record["groups"] is None else policy_from_record(record["groups"])
            if groups is not None and not isinstance(groups, GroupSplit):
                raise _INVALID.error(reason="unknown_record", field="groups")
            return TimeSplit(
                train_until=record["train_until"],
                validation_until=record["validation_until"],
                embargo=timedelta(microseconds=record["embargo_microseconds"]),
                groups=groups,
            )
    except TerrineError:
        raise
    except (TypeError, ValueError, OverflowError) as error:
        raise _INVALID.error(reason="malformed_record", field="record") from error
    raise _INVALID.error(reason="unknown_record", field="record")


def group_bucket(key: str | int, policy: GroupSplit) -> Literal["train", "validation", "test"]:
    if isinstance(key, bool) or not isinstance(key, (str, int)):
        raise _INVALID.error(reason="unsupported_group_key", field="key")
    if not isinstance(policy, GroupSplit):
        raise _INVALID.error(reason="wrong_type", field="policy")
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
        raise _INVALID.error(reason="empty_name", field="key_field")
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
    rows: list[dict[str, Any]],
    policy: TimeSplit,
    *,
    group_keys: dict[Any, str | int] | None = None,
    catalog_timezone: Any = None,
) -> dict[Any, SplitAssignment]:
    """Assign origins using label support and leakage-safe split boundaries."""
    if isinstance(policy.train_until, date) and not isinstance(policy.train_until, datetime):
        if catalog_timezone is None:
            raise _INVALID.error(reason="unbound_calendar_date", field="catalog_timezone")
        policy = policy.bind(catalog_timezone)
    elif (
        policy.validation_until is not None
        and isinstance(policy.validation_until, date)
        and not isinstance(policy.validation_until, datetime)
    ):
        if catalog_timezone is None:
            raise _INVALID.error(reason="unbound_calendar_date", field="catalog_timezone")
        policy = policy.bind(catalog_timezone)
    output: dict[Any, SplitAssignment] = {}
    for row in rows:
        key = row["id"]
        origin = _instant(row["origin"], "origin")
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
        support = row.get("label_support")
        if isinstance(support, dict):
            unavailable = [
                (name, item) for name, item in support.items() if not item.get("available", False)
            ]
            if unavailable:
                reasons = row.get("label_reasons", {})
                name, _ = unavailable[0]
                output[key] = SplitAssignment("unassigned", reasons.get(name, "label_unavailable"))
                continue
        elif row.get("label_available", row.get("label_end") is not None) is False:
            output[key] = SplitAssignment(
                "unassigned", row.get("label_reason", "label_unavailable")
            )
            continue
        elif "label_end" not in row:
            output[key] = SplitAssignment("unassigned", "label_unavailable")
            continue
        ends: list[tuple[Any, bool]] = []
        if isinstance(support, dict):
            ends.extend(
                (item.get("end"), bool(item.get("point", False))) for item in support.values()
            )
        else:
            ends.append((row.get("label_end"), bool(row.get("point_label", False))))
        if boundary is not None:
            for end_value, point in ends:
                if end_value is None:
                    output[key] = SplitAssignment(
                        "unassigned", row.get("label_reason", "label_unavailable")
                    )
                    break
                end = _instant(end_value, "label_end")
                if not (end < boundary if point else end <= boundary):
                    output[key] = SplitAssignment("unassigned", "label_overlap")
                    break
            if key in output:
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
