from __future__ import annotations

import pytest

from terrine.data import Budget
from terrine.errors import TerrineError
from tests.dataset.test_staging import _durable_frame


@pytest.mark.parametrize(
    ("role", "window"), [("features", ("-1d", "0d")), ("outcomes", ("0d", "1d"))]
)
def test_approximate_quantile_stage_refuses_before_capture(
    events_world, tmp_path, monkeypatch, role, window
):
    from decimal import Decimal
    from importlib.metadata import version

    from terrine.model import Model
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.semantic.models import Catalog
    from terrine.spec import Design, FixedDate, SketchRequest, approx_quantile

    pytest.importorskip("datasketches")
    frame, con = _durable_frame(events_world, tmp_path)
    try:
        con.raw_sql("ALTER TABLE events ADD COLUMN value INTEGER DEFAULT 1")
        definition = frame._model.catalog.model_dump(mode="python")
        definition["facts"]["session"]["column"] = "value"
        definition["measures"]["sessions_count"]["aggregation"] = "sum"
        model = Model.from_catalog(
            Catalog.model_validate(definition),
            con=con,
            units=frame._model.units,
            readiness=frame._model.readiness,
        )
        expressions = {
            "q": approx_quantile(
                "sessions",
                "0.5",
                window=window,
                sketch=SketchRequest("tdigest", version("datasketches"), Decimal("0.01")),
            )
        }
        design = Design(
            "user",
            FixedDate("2025-01-05"),
            features=expressions if role == "features" else {},
            outcomes=expressions if role == "outcomes" else {},
        )
        approximate = design.build(model)
        owner = RuntimeOwner.from_connection(con)

        def capture_forbidden(*args, **kwargs):
            raise AssertionError("unsupported staging must refuse before snapshot capture")

        monkeypatch.setattr(SnapshotAuthority, "capture", capture_forbidden)
        with pytest.raises(TerrineError) as error:
            approximate.stage("approximate", budget=Budget(strict_scan=False))
        assert error.value.code == "terrine.runtime.sketch_sidecar_unavailable"
        assert not owner.snapshot_root.exists()
    finally:
        con.disconnect()
