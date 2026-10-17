from hashlib import sha256

import numpy as np
import pytest

from terrine.errors import TerrineError
from terrine.estimate.folds import assign_folds, encode_unit


def test_unit_hash_is_typed_stable_and_row_order_invariant():
    assert encode_unit(-7) == b"i" + (8).to_bytes(8, "big") + (-7).to_bytes(8, "big", signed=True)
    assert encode_unit("é") == b"s" + (2).to_bytes(8, "big") + b"\xc3\xa9"
    assert encode_unit("é") != encode_unit("e\u0301")
    units = list(range(-30, 30))
    got = assign_folds(units, folds=3, seed=17)
    expected = [
        int.from_bytes(
            sha256(
                b"terrine-fold-v1"
                + (17).to_bytes(8, "big")
                + b"i"
                + (8).to_bytes(8, "big")
                + i.to_bytes(8, "big", signed=True)
            ).digest()[:8],
            "big",
        )
        % 3
        for i in units
    ]
    np.testing.assert_array_equal(got, expected)
    np.testing.assert_array_equal(assign_folds(units[::-1], folds=3, seed=17), got[::-1])


def test_invalid_and_empty_folds_refuse():
    for folds, seed, units in [(1, 0, list(range(10))), (2, -1, list(range(10))), (2, 0, [1, 2])]:
        # The last case deliberately puts both typed hashes in the same fold.
        if folds == 2 and seed == 0 and len(units) == 2:
            buckets = {}
            for i in range(10):
                bucket = (
                    int.from_bytes(
                        sha256(b"terrine-fold-v1" + bytes(8) + encode_unit(i)).digest()[:8], "big"
                    )
                    % 2
                )
                buckets.setdefault(bucket, []).append(i)
            units = next(v[:2] for v in buckets.values() if len(v) >= 2)
        with pytest.raises(TerrineError) as exc:
            assign_folds(units, folds=folds, seed=seed)
        assert exc.value.code == "terrine.estimate.invalid_folds"
