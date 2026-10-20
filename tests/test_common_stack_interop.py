from __future__ import annotations

import pytest

from terrine.errors import TerrineError


@pytest.mark.parametrize("extra", ["pandas", "polars"])
def test_missing_dataframe_extra_refuses_before_execution(native_inputs, monkeypatch, extra):
    from terrine.data import Coverage
    from terrine.model import Model
    from terrine.spec import Design, EventAnchor

    catalog, con = native_inputs
    frame = Design("user", EventAnchor("session")).build(
        Model.from_catalog(
            catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-01-31")}
        )
    )
    con.con.execute("DROP TABLE events")
    original = __import__("terrine.frame", fromlist=["import_module"]).import_module

    def unavailable(name):
        if name == extra:
            raise ImportError("optional dataframe unavailable")
        return original(name)

    monkeypatch.setattr("terrine.frame.import_module", unavailable)
    with pytest.raises(TerrineError) as error:
        getattr(frame, f"to_{extra}")()
    assert error.value.code == "terrine.convert.missing_dependency"
    assert error.value.context["extra"] == extra
