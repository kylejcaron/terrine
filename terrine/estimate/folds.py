"""Stable typed SHA-256 independent-unit folds."""

from __future__ import annotations

from collections.abc import Sequence
from hashlib import sha256
from numbers import Integral

import numpy as np

from terrine.errors import RefusalSpec

INVALID_FOLDS = RefusalSpec(
    "terrine.estimate.invalid_folds",
    "Use 2 <= folds <= n, an unsigned 64-bit seed, and nonempty held-out folds; "
    "no seed retries are performed.",
)
ONE_ARM = RefusalSpec(
    "terrine.estimate.one_arm", "Both treatment arms are required in every training complement."
)


def encode_unit(unit: int | str) -> bytes:
    if isinstance(unit, Integral) and not isinstance(unit, (bool, np.bool_)):
        integer = int(unit)
        if not -(2**63) <= integer < 2**63:
            raise INVALID_FOLDS.error(reason="unit_identity_requires_int64_or_utf8")
        payload = integer.to_bytes(8, "big", signed=True)
        tag = b"i"
    elif isinstance(unit, str):
        payload = unit.encode("utf-8")
        tag = b"s"
    else:
        raise INVALID_FOLDS.error(reason="unit_identity_requires_int64_or_utf8")
    return tag + len(payload).to_bytes(8, "big") + payload


def assign_folds(units: Sequence[int | str], *, folds: int, seed: int) -> np.ndarray:
    if (
        isinstance(folds, bool)
        or not isinstance(folds, int)
        or not 2 <= folds <= len(units)
        or isinstance(seed, bool)
        or not isinstance(seed, int)
        or not 0 <= seed < 2**64
    ):
        raise INVALID_FOLDS.error(reason="fold_or_seed_domain", n=len(units))
    prefix = b"terrine-fold-v1" + seed.to_bytes(8, "big")
    ids = np.fromiter(
        (
            int.from_bytes(sha256(prefix + encode_unit(unit)).digest()[:8], "big") % folds
            for unit in units
        ),
        dtype=np.int64,
        count=len(units),
    )
    if len(np.unique(ids)) != folds:
        raise INVALID_FOLDS.error(reason="empty_heldout_fold", folds=folds, n=len(units))
    return ids


def fold_digest(units: Sequence[int | str], ids: np.ndarray) -> str:
    digest = sha256(b"terrine-fold-assignment-v1")
    for unit, fold in sorted(
        zip((encode_unit(u) for u in units), ids, strict=True), key=lambda item: item[0]
    ):
        digest.update(unit)
        digest.update(int(fold).to_bytes(8, "big"))
    return digest.hexdigest()
