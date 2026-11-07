"""Datasets staged by each released version reopen in place with the current code."""

import json
import os
import subprocess
import sys
from pathlib import Path

import ibis
import pyarrow.parquet as pq
import pytest

from scripts.write_artifact_corpus import (
    _reproducible_staging,
    _role,
    bind_model,
    output_schema,
)
from terrine import Budget, Dataset
from terrine.data import MaterializedFrame
from terrine.dataset.manifest import DatasetManifest
from terrine.spec import from_json
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
import json
import sys
from pathlib import Path

expected, checkout, corpus, output = sys.argv[1:]
installed = importlib.metadata.version("terrine")
assert installed == expected, f"terrine {installed} is installed, not {expected}"

import ibis
import pyarrow.parquet as pq

import terrine
from terrine.errors import TerrineError
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
    name = design_path.parent.name
    frame = from_json(design_path.read_text(encoding="utf-8")).build(model)
    try:
        dataset = frame.stage(f"corpus_{index}", budget=budget)
    except TerrineError as error:
        if error.code != "terrine.compile.runtime_shape_unsupported":
            raise
        (output / f"{name}-refusal.json").write_text(
            json.dumps({"code": error.code}), encoding="utf-8"
        )
        materialized = frame.materialize(budget=budget)
        pq.write_table(materialized.to_pyarrow(), output / f"{name}-rows.parquet")
    else:
        (output / f"{name}-manifest.json").write_text(dataset.manifest.to_json(), encoding="utf-8")
        pq.write_table(dataset.to_pyarrow(), output / f"{name}-rows.parquet")
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

        con = ibis.duckdb.connect(str(tmp_path / "owner.duckdb"))
        try:
            for design_path in sorted(CORPUS.glob("**/design.json")):
                name = design_path.parent.name
                staged = pq.read_table(tmp_path / f"{name}-rows.parquet")
                expected_schema = json.loads(
                    (design_path.parent / "expected_schema.json").read_text("utf-8")
                )["design.json"]
                refusal_path = tmp_path / f"{name}-refusal.json"
                if refusal_path.exists():
                    refusal = json.loads(refusal_path.read_text("utf-8"))
                    assert refusal == {"code": "terrine.compile.runtime_shape_unsupported"}
                    with _reproducible_staging():
                        direct_con = ibis.duckdb.connect()
                        try:
                            model = bind_model(direct_con, CORPUS / "data")
                            frame = from_json(design_path.read_text("utf-8")).build(model)
                            assert {
                                column["name"]: column
                                for column in output_schema(frame)
                                if column["role"] != "support"
                            } == {
                                column["name"]: column
                                for column in expected_schema
                                if column["role"] != "support"
                            }
                            current_rows = frame.materialize(
                                budget=Budget(
                                    max_rows=100,
                                    max_output_bytes=1_000_000,
                                    strict_scan=False,
                                )
                            ).to_pyarrow()
                        finally:
                            direct_con.disconnect()
                    assert current_rows.schema.equals(staged.schema, check_metadata=False)
                    assert current_rows.to_pylist() == staged.to_pylist()
                    continue

                manifest = DatasetManifest.from_json(
                    (tmp_path / f"{name}-manifest.json").read_text("utf-8")
                )
                reopened = Dataset.from_manifest(manifest, con=con).to_pyarrow()
                assert reopened.schema.equals(staged.schema)
                assert reopened.to_pylist() == staged.to_pylist()
                materialized = MaterializedFrame.from_arrow(reopened)
                roles = materialized.roles
                reopened_schema = [
                    {
                        "name": field.name,
                        "type": str(field.type),
                        "role": _role(field.name, roles, materialized.columns),
                    }
                    for field in reopened.schema
                ]
                assert {
                    column["name"]: column
                    for column in reopened_schema
                    if column["role"] != "support"
                } == {
                    column["name"]: column
                    for column in expected_schema
                    if column["role"] != "support"
                }

            restored_manifest = DatasetManifest.from_json(
                (tmp_path / "state-panel-manifest.json").read_text("utf-8")
            )
            restored = Dataset.from_manifest(restored_manifest, con=con)
            assert restored.manifest.frame_metadata["provenance"]["state_panel"] is not None
            restored.to_pandas()
        finally:
            con.disconnect()
