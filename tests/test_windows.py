from typing import Any, cast

import pytest

from terrine.errors import TerrineError
from terrine.windows import Duration, Months, Window, duration


def test_bare_integer_durations_refuse_except_zero():
    with pytest.raises(TerrineError) as error:
        Window(cast(Any, -30), 0)
    assert error.value.code == "terrine.window.invalid_duration"
    assert error.value.context == {
        "field": "window",
        "reason": "bare_integer",
        "value": "-30",
    }
    assert Window("-30d", 0) == Window("-30d", "0s")
    assert duration(0, field="window") == Duration(0)
    assert duration(Duration(-30), field="window") == Duration(-30)


def test_suggested_duration_correction_is_accepted():
    assert Window("-30s", "0s").start == Duration(-30)
    assert Window("-30d", "0s").start.seconds == -30 * 86_400
    assert Window("0s", "4w").end.seconds == 4 * 7 * 86_400


def test_equivalent_durations_and_half_open_windows():
    assert duration("-2w", field="window") == duration("-14d", field="window") == Duration(-1209600)
    assert Window("-60m", "+0s") == Window("-1h", "0d")
    assert Duration(86400) == duration("24h", field="gap")
    assert duration("+000s", field="window") == Duration(0)
    assert Window("-1m", "1s").start.seconds == -60


@pytest.mark.parametrize(
    ("seconds", "text"),
    [
        (0, "0s"),
        (90, "90s"),
        (-2_592_000, "-30d"),
        (604_800, "1w"),
        (2**63 - 1, None),
        (-(2**63), None),
    ],
)
def test_duration_text_is_the_largest_exact_unit_and_parses_back(seconds, text):
    value = Duration(seconds)
    if text is not None:
        assert str(value) == text
    assert duration(str(value), field="gap") == value
    assert hash(value) == hash(Duration(seconds))
    assert value != seconds


@pytest.mark.parametrize("value", [True, 2**63, -(2**63) - 1, "1d", 1.0])
def test_duration_values_refuse_non_int64_seconds(value):
    with pytest.raises(TerrineError) as error:
        Duration(value)
    assert error.value.code == "terrine.window.invalid_duration"
    assert error.value.context == {"field": "seconds", "value": str(value)}


def test_months_text_uses_years_when_whole():
    assert (str(Months(1)), str(Months(18)), str(Months(24))) == ("1mo", "18mo", "2y")
    for value in (0, -1, True):
        with pytest.raises(TerrineError):
            Months(value)


@pytest.mark.parametrize(
    "value",
    [
        " 1d",
        "1d ",
        "1.5h",
        "1h30m",
        "1M",
        "1mo",
        "1y",
        1.5,
        "１d",
        "9223372036854775808s",
        Months(1),
    ],
)
def test_invalid_duration_refuses_without_coercion(value):
    with pytest.raises(TerrineError) as error:
        duration(value, field="before")
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
