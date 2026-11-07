from __future__ import annotations

import pytest

from terrine.dataset.export import export_dataset
from terrine.errors import TerrineError


def test_export_parquet_refuses_on_windows(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr("sys.platform", "win32")

    with pytest.raises(TerrineError) as error:
        export_dataset(object(), destination=str(tmp_path / "export"))

    assert error.value.code == "terrine.dataset.unsupported_platform"
    assert error.value.context["reason"] == "windows"
    assert list(tmp_path.iterdir()) == []
