import pytest

from terrine.errors import TerrineError
from terrine.windows import Window, seconds


def test_equivalent_durations_and_half_open_windows():
    assert seconds("-2w", field="window") == seconds("-14d", field="window") == -1209600
    assert Window("-60m", "+0s") == Window("-1h", "0d")
    assert seconds("24h", field="window") == seconds("1d", field="window")
    assert seconds("+000s", field="window") == 0
    assert Window("-1m", "1s").start == -60


@pytest.mark.parametrize(
    "value",
    [" 1d", "1d ", "1.5h", "1h30m", "1M", "1mo", "1y", True, 1.5, "１d", "9223372036854775808s"],
)
def test_invalid_duration_refuses_without_coercion(value):
    with pytest.raises(TerrineError) as error:
        seconds(value, field="before")
    assert error.value.code == "terrine.window.invalid_duration"
    assert error.value.context == {"field": "before", "value": str(value)}


def test_window_refusals_name_the_window_field():
    with pytest.raises(TerrineError) as error:
        Window("-1mo", "0s")
    assert error.value.code == "terrine.window.invalid_duration"
    assert error.value.context == {"field": "window", "value": "-1mo"}
    for bounds, expected in [
        (("0d", "0d"), (0, 0)),
        (("1d", "-1d"), (86400, -86400)),
    ]:
        with pytest.raises(TerrineError) as error:
            Window(*bounds)
        assert error.value.code == "terrine.window.invalid_window"
        assert error.value.context["start"] == expected[0]
        assert error.value.context["end"] == expected[1]
