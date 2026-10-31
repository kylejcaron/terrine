from typing import Any, cast

import pytest

from terrine.errors import TerrineError
from terrine.windows import Window, duration, seconds, seconds_text


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
    assert duration(0, field="window") == 0
    assert seconds(-30, field="window") == -30
    assert seconds_text(-86_400) == "-86400s"


def test_suggested_duration_correction_is_accepted():
    assert Window("-30s", "0s").start == -30
    assert Window("-30d", "0s").start == -30 * 86_400
    assert Window("0s", "4w").end == 4 * 7 * 86_400


def test_saved_integer_second_documents_still_decode():
    import terrine as tr

    design = tr.recipes.time_series(
        every="7d",
        start="2025-01-06",
        end="2025-02-03",
        features={"x": tr.agg("orders", window=("-30d", "0s"))},
    )
    assert tr.from_yaml(tr.to_yaml(design)) == design


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
