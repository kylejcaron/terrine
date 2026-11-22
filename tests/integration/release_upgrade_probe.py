"""Stage and read the saved-artifact corpus in separately installed packages."""

from __future__ import annotations

import importlib.metadata
import json
import runpy
import sys
from hashlib import file_digest
from pathlib import Path
from urllib.parse import unquote, urlsplit

from terrine import Budget

CHECKOUT = Path(__file__).resolve().parents[2]
CORPUS = CHECKOUT / "tests" / "artifacts" / "1"
BUDGET = Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)
UNSUPPORTED = "terrine.compile.runtime_shape_unsupported"


def produce(output: Path) -> None:
    import ibis
    import pyarrow.parquet as pq

    from terrine import Coverage, Model, UnitSource, recipes
    from terrine.errors import TerrineError
    from terrine.spec import BinnedMetricState, from_json

    con = ibis.duckdb.connect(str(output / "owner.duckdb"))
    try:
        for path in sorted((CORPUS / "data").glob("*.parquet")):
            con.create_table(path.stem, pq.read_table(path))
        model = Model.from_catalog(
            CORPUS / "catalog.yaml",
            con=con,
            units={"customer": UnitSource("customers", "customer_id")},
            readiness={"orders": Coverage("2025-01-01", "2025-02-28")},
        )
        for index, design in enumerate(sorted(CORPUS.rglob("design.json"))):
            name = design.parent.name
            frame = from_json(design.read_text("utf-8")).build(model)
            try:
                artifact = frame.stage(f"corpus_{index}", budget=BUDGET)
            except TerrineError as error:
                if error.code != UNSUPPORTED:
                    raise
                (output / f"{name}-refusal.json").write_text(
                    json.dumps({"code": error.code}), encoding="utf-8"
                )
                rows = frame.materialize(budget=BUDGET).to_pyarrow()
            else:
                (output / f"{name}-manifest.json").write_text(artifact.manifest.to_json(), "utf-8")
                rows = artifact.to_pyarrow()
            pq.write_table(rows, output / f"{name}-rows.parquet")
        state_panel = (
            recipes.state_panel(
                state_source=BinnedMetricState(
                    "spend", cutpoints=(0,), levels=("no_spend", "spend")
                ),
                every="1d",
                start="2025-01-01",
                end="2025-01-02",
            )
            .build(model)
            .stage("state_panel", budget=BUDGET)
        )
        (output / "state-panel-manifest.json").write_text(state_panel.manifest.to_json(), "utf-8")
        pq.write_table(state_panel.to_pyarrow(), output / "state-panel-rows.parquet")
    finally:
        con.disconnect()


def reopen(output: Path) -> None:
    import ibis
    import pyarrow.parquet as pq

    from terrine import Dataset
    from terrine.data import MaterializedFrame
    from terrine.dataset.manifest import DatasetManifest
    from terrine.spec import from_json

    # Execute just the test helpers, without adding the checkout to sys.path.
    corpus_tools = runpy.run_path(str(CHECKOUT / "scripts" / "write_artifact_corpus.py"))
    materialized_schema = corpus_tools["materialized_schema"]

    def compare_rows(reopened, staged, name):
        assert reopened.schema.equals(staged.schema), name
        assert reopened.to_pylist() == staged.to_pylist(), name

    con = ibis.duckdb.connect(str(output / "owner.duckdb"))
    try:
        for design in sorted(CORPUS.rglob("design.json")):
            name = design.parent.name
            staged = pq.read_table(output / f"{name}-rows.parquet")
            expected = json.loads((design.parent / "expected_schema.json").read_text("utf-8"))
            expected_schema = expected["design.json"]
            refusal_path = output / f"{name}-refusal.json"
            if refusal_path.exists():
                assert json.loads(refusal_path.read_text("utf-8")) == {"code": UNSUPPORTED}, name
                with corpus_tools["_reproducible_staging"]():
                    direct_con = ibis.duckdb.connect()
                    try:
                        model = corpus_tools["bind_model"](direct_con, CORPUS / "data")
                        frame = from_json(design.read_text("utf-8")).build(model)
                        materialized = frame.materialize(budget=BUDGET)
                    finally:
                        direct_con.disconnect()
                columns = {column["name"] for column in expected_schema}
                current = materialized.to_pyarrow()
                compare_rows(
                    current.select(
                        [column for column in current.column_names if column in columns]
                    ),
                    staged.select([column for column in staged.column_names if column in columns]),
                    name=name,
                )
            else:
                manifest = DatasetManifest.from_json(
                    (output / f"{name}-manifest.json").read_text("utf-8")
                )
                rows = Dataset.from_manifest(manifest, con=con).to_pyarrow()
                compare_rows(rows, staged, name=name)
                materialized = MaterializedFrame.from_arrow(rows)
            assert {column["name"]: column for column in materialized_schema(materialized)} == {
                column["name"]: column for column in expected_schema
            }, name

        panel_json = (output / "state-panel-manifest.json").read_text("utf-8")
        panel = Dataset.from_manifest(DatasetManifest.from_json(panel_json), con=con)
        expected_panel = json.loads(panel_json)["frame_metadata"]["provenance"]["state_panel"]
        assert expected_panel is not None
        reopened_panel = json.loads(panel.manifest.to_json())["frame_metadata"]["provenance"]
        assert reopened_panel["state_panel"] == expected_panel
        compare_rows(
            panel.to_pyarrow(),
            pq.read_table(output / "state-panel-rows.parquet"),
            name="state panel",
        )
        panel.to_pandas()
    finally:
        con.disconnect()


def _verify_install(version: str, wheel_url: str) -> None:
    import terrine

    distribution = importlib.metadata.distribution("terrine")
    assert distribution.version == version, (distribution.version, version)
    assert terrine.__file__ is not None
    location = Path(terrine.__file__).resolve()
    assert not location.is_relative_to(CHECKOUT), f"terrine imported from checkout: {location}"
    assert not any(Path(p).resolve().is_relative_to(CHECKOUT) for p in sys.path), sys.path
    assert not Path.cwd().is_relative_to(CHECKOUT), Path.cwd()
    if wheel_url:
        assert location.is_relative_to(Path(sys.prefix).resolve()), location
        assert location == Path(str(distribution.locate_file("terrine/__init__.py"))).resolve()
        direct_url = distribution.read_text("direct_url.json")
        assert direct_url is not None, location
        origin = json.loads(direct_url)
        url, _, fragment = wheel_url.partition("#")
        installed_url, _, installed_fragment = origin["url"].partition("#")
        assert installed_url == url and installed_fragment in ("", fragment), origin
        source = urlsplit(url)
        assert source.scheme == "file" and not source.netloc, url
        with Path(unquote(source.path)).open("rb") as wheel:
            digest = file_digest(wheel, "sha256").hexdigest()
        assert fragment == f"sha256={digest}", (fragment, digest)
        # Some installers put the digest in archive_info instead of the URL.
        archive = origin["archive_info"]
        hashes = archive.get("hashes", {})
        if "sha256" in hashes:
            assert hashes["sha256"] == digest, archive
        legacy_hash = archive.get("hash", "")
        if legacy_hash.startswith("sha256="):
            assert legacy_hash == fragment, archive
        print(f"Installed terrine {version} from {location} (wheel SHA-256: {digest})", flush=True)


if __name__ == "__main__":
    operation, version, wheel_url, path = sys.argv[1:]
    _verify_install(version, wheel_url)
    {"produce": produce, "reopen": reopen}[operation](Path(path))
