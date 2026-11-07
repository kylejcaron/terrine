"""Datasets staged by each released version reopen in place with the current code."""

import os
import subprocess
import sys
from pathlib import Path

import ibis
import pyarrow.parquet as pq
import pytest

from terrine import Dataset
from terrine.dataset.manifest import DatasetManifest
from tests._timing import HANG_GUARD_SECONDS

CHECKOUT = Path(__file__).resolve().parents[1]
ARTIFACTS = CHECKOUT / "tests" / "artifacts"
# Generation 1 was written by 1.0.0rc1, so every 1.x release reads its documents.
CORPUS = ARTIFACTS / "1"
# One released version, or the absolute path of a wheel file, per line.
RELEASES = [
    line.strip()
    for line in (ARTIFACTS / "RELEASES").read_text(encoding="utf-8").splitlines()
    if line.strip()
]
RUNTIME_SETTINGS = ("TERRINE_RUNTIME_NAMESPACE", "TERRINE_RUNTIME_SNAPSHOT_ROOT")

# Runs on the released package alone, so it uses only its public API.
PRODUCER = """\
import importlib.metadata
import sys
from pathlib import Path

expected, checkout, corpus, output = sys.argv[1:]
installed = importlib.metadata.version("terrine")
assert installed == expected, f"terrine {installed} is installed, not {expected}"

import ibis
import pyarrow.parquet as pq

import terrine
from terrine import Budget, Coverage, Model, UnitSource, recipes
from terrine.spec import BinnedMetricState, from_json

location = Path(terrine.__file__).resolve()
assert not location.is_relative_to(checkout), f"terrine was imported from {location}"
corpus, output = Path(corpus), Path(output)
con = ibis.duckdb.connect(str(output / "owner.duckdb"))
for path in sorted((corpus / "data").glob("*.parquet")):
    con.create_table(path.stem, pq.read_table(path))
model = Model.from_catalog(
    corpus / "catalog.yaml",
    con=con,
    units={"customer": UnitSource("customers", "customer_id")},
    readiness={"orders": Coverage("2025-01-01", "2025-02-28")},
)
budget = Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)
for index, design_path in enumerate(sorted(corpus.glob("**/design.json"))):
    dataset = (
        from_json(design_path.read_text(encoding="utf-8"))
        .build(model)
        .stage(f"corpus-{index}", budget=budget)
    )
    if index == 0:
        (output / "manifest.json").write_text(dataset.manifest.to_json(), encoding="utf-8")
        pq.write_table(dataset.to_pyarrow(), output / "rows.parquet")

state_panel = recipes.state_panel(
    state_source=BinnedMetricState("spend", cutpoints=(0,), levels=("no_spend", "spend")),
    every="1d",
    start="2025-01-01",
    end="2025-01-02",
).build(model).stage("state_panel", budget=budget)
(output / "state-panel-manifest.json").write_text(state_panel.manifest.to_json(), encoding="utf-8")
con.disconnect()
"""


def _version(entry: str) -> str:
    return Path(entry).name.split("-")[1] if entry.endswith(".whl") else entry


def _requirement(entry: str) -> str:
    if entry.endswith(".whl"):
        return f"terrine[duckdb] @ {Path(entry).as_uri()}"
    return f"terrine[duckdb]=={entry}"


if RELEASES:  # An empty list defines no test, rather than collecting a skipped one.

    @pytest.mark.scale
    @pytest.mark.parametrize("entry", RELEASES, ids=_version)
    def test_dataset_staged_by_release_reopens_in_place(entry, tmp_path, monkeypatch):
        for name in RUNTIME_SETTINGS:
            monkeypatch.delenv(name, raising=False)
        environment = {
            name: value
            for name, value in os.environ.items()
            if name not in ("VIRTUAL_ENV", *RUNTIME_SETTINGS)
        }
        producer = tmp_path / "producer"
        producer.mkdir()
        (producer / "producer.py").write_text(PRODUCER, encoding="utf-8")
        result = subprocess.run(
            [
                "uv",
                "run",
                "--no-project",
                "--isolated",
                "--python",
                f"{sys.version_info.major}.{sys.version_info.minor}",
                "--with",
                _requirement(entry),
                "python",
                "-I",
                "producer.py",
                _version(entry),
                str(CHECKOUT),
                str(CORPUS),
                str(tmp_path),
            ],
            cwd=producer,
            env=environment,
            capture_output=True,
            text=True,
            timeout=HANG_GUARD_SECONDS,
            check=False,
        )
        assert result.returncode == 0, result.stderr

        manifest = DatasetManifest.from_json((tmp_path / "manifest.json").read_text("utf-8"))
        con = ibis.duckdb.connect(str(tmp_path / "owner.duckdb"))
        try:
            reopened_dataset = Dataset.from_manifest(manifest, con=con)
            reopened = reopened_dataset.to_pyarrow()
            restored_manifest = DatasetManifest.from_json(
                (tmp_path / "state-panel-manifest.json").read_text("utf-8")
            )
            restored = Dataset.from_manifest(restored_manifest, con=con)
            assert restored.manifest.frame_metadata["provenance"]["state_panel"] is not None
            restored.to_pandas()
        finally:
            con.disconnect()
        staged = pq.read_table(tmp_path / "rows.parquet")
        assert reopened.schema.equals(staged.schema)
        assert reopened.to_pylist() == staged.to_pylist()
