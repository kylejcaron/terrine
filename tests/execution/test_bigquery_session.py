"""BigQuery query failures preserve their known arithmetic domain."""

import pytest

from terrine.errors import TerrineError
from terrine.execution._bigquery import _overflow_refusal


@pytest.mark.parametrize(
    ("message", "exact"),
    [
        ("BIGNUMERIC overflow: value exceeds the supported range", True),
        ("NUMERIC overflow: value exceeds the supported range", True),
        ("Floating point overflow in SUM", False),
    ],
)
def test_overflow_translation_requires_an_explicit_exact_domain(message, exact):
    failure = RuntimeError(message)
    with pytest.raises((RuntimeError, TerrineError)) as caught:
        with _overflow_refusal():
            raise failure
    if exact:
        assert isinstance(caught.value, TerrineError)
        assert caught.value.code == "terrine.temporal.exact_overflow"
        assert caught.value.__cause__ is failure
    else:
        assert caught.value is failure
