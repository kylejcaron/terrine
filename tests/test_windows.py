import pytest

from terrine.errors import TerrineError
from terrine.windows import Window, seconds


def test_equivalent_durations_and_half_open_windows():
    assert seconds("-2w") == seconds("-14d") == -1209600
    assert Window("-60m", "+0s") == Window("-1h", "0d")
    assert seconds("24h") == seconds("1d")
    assert seconds("+000s") == 0
    assert Window("-1m", "1s").start == -60


@pytest.mark.parametrize(
    "value", [" 1d", "1d ", "1.5h", "1h30m", "1M", True, 1.5, "１d", "9223372036854775808s"]
)
def test_invalid_duration_refuses_without_coercion(value):
    with pytest.raises(TerrineError) as error:
        seconds(value)
    assert error.value.code == "terrine.window.invalid_duration"


def test_empty_reversed_and_difference_overflow_refuse():
    for bounds in [("0d", "0d"), ("1d", "-1d")]:
        with pytest.raises(TerrineError) as error:
            Window(*bounds)
        assert error.value.code == "terrine.window.invalid_window"
