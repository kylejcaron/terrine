from __future__ import annotations

from pathlib import Path

import pytest

from terrine.errors import TerrineError
from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer
from terrine.runtime.state import RuntimeStore


def test_control_server_refuses_on_windows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("sys.platform", "win32")
    endpoint_path = tmp_path / "nested" / "control.sock"
    server = ControlServer(
        RuntimeStore(None, namespace="owner"),
        namespace="owner",
        endpoint=ControlEndpoint(endpoint_path),
    )

    with pytest.raises(TerrineError) as error:
        server.start()

    assert error.value.code == "terrine.runtime.unsupported_platform"
    assert error.value.context["reason"] == "windows"
    assert not endpoint_path.parent.exists()


def test_control_client_refuses_on_windows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("sys.platform", "win32")
    client = ControlClient(ControlEndpoint(tmp_path / "control.sock"), namespace="owner")

    with pytest.raises(TerrineError) as error:
        client._request("status")

    assert error.value.code == "terrine.runtime.unsupported_platform"
    assert error.value.context["reason"] == "windows"
