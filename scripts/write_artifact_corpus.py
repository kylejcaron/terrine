"""Write one generation of the saved-artifact corpus.

``uv run python scripts/write_artifact_corpus.py --generation <name>`` writes
``tests/artifacts/<name>/``: Design, definition and Catalog documents, the manifest and
retained-input receipt of a staged dataset, the Arrow/Parquet metadata of its Parquet
export, tiny source tables, and each recipe's Design and output schema. Existing root
documents are preserved; recipe documents are refreshed deterministically.

The documents are path-independent and reproducible. Staging draws its random identifiers
from a fixed seed and runs DuckDB on one thread, and the receipt and manifest name the
owner database and snapshot root relative to the temporary directory they were staged
in. No owner database is stored, because it binds its absolute location.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import tempfile
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import fields, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import duckdb
import ibis
import pyarrow as pa
import pyarrow.parquet as pq

import terrine as tr
from terrine import recipes
from terrine.data import Roles
from terrine.dataset import GroupSplit
from terrine.dataset.manifest import DatasetManifest
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.snapshot import ReopenableSnapshot, SnapshotAuthority
from terrine.spec import (
    BinnedMetricState,
    DatasetDefinition,
    Design,
    FixedDate,
    Initiation,
    Winsorization,
    agg,
    asof,
    eq,
    event_time,
    gt,
    to_json,
    to_yaml,
)

ARTIFACTS = Path(__file__).resolve().parents[1] / "tests" / "artifacts"
NAMESPACE = "terrine_runtime"
_SEED = 20261101
_BUDGET = tr.Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)


def source_tables() -> dict[str, pa.Table]:
    """Three customers and six orders between January and mid-February 2025."""
    ordered_at = [
        datetime(2025, 1, 3, 9, tzinfo=UTC),
        datetime(2025, 1, 20, 12, tzinfo=UTC),
        datetime(2025, 1, 5, 8, tzinfo=UTC),
        datetime(2025, 1, 28, 18, tzinfo=UTC),
        datetime(2025, 2, 3, 10, tzinfo=UTC),
        datetime(2025, 2, 10, 15, tzinfo=UTC),
    ]
    return {
        "customers": pa.table({"customer_id": ["alice", "bob", "carol"]}),
        "orders": pa.table(
            {
                "customer_id": ["alice", "alice", "bob", "carol", "alice", "bob"],
                "ordered_at": pa.array(ordered_at, pa.timestamp("us", tz="UTC")),
                "status": ["paid", "refunded", "paid", "paid", "paid", "paid"],
                "amount": [30.0, 45.0, 80.0, 12.5, 25.0, 20.0],
            }
        ),
    }


def bind_model(con: Any, data: Path) -> tr.Model:
    """Load each ``data/*.parquet`` table into ``con`` and bind the corpus Model.

    The schema check binds every stored generation's data with this declaration.
    """
    for path in sorted(data.glob("*.parquet")):
        con.create_table(path.stem, pq.read_table(path))
    return tr.Model.from_tables(
        con,
        units={"customer": tr.UnitSource("customers", "customer_id")},
        events={
            "orders": tr.Events(
                "orders",
                time="ordered_at",
                key="customer_id",
                labels=("status",),
                metrics={
                    "orders": tr.count(),
                    "refunds": tr.count(where={"status": "refunded"}),
                    "spend": tr.sum("amount"),
                },
                complete=("2025-01-01", "2025-02-28"),
            )
        },
    )


def designs() -> dict[str, Design]:
    """The stored Designs, keyed by document name; the JSON Design is also staged."""
    window = ("-30d", "0s")
    return {
        "design.json": Design(
            "customer",
            FixedDate("2025-02-01"),
            features={
                "orders_30d": agg("orders", window=window),
                "spend_30d": agg("spend", window=window),
                "refunds_30d": agg("refunds", window=window),
                "last_status": asof("status"),
                "first_refund": event_time(
                    "orders", window=window, label_property="status", equals="refunded"
                ),
            },
            outcomes={"spend_next_14d": agg("spend", window=("0s", "14d"))},
        ),
        "design.yaml": recipes.time_series(
            every="1mo",
            start="2025-02-01",
            end="2025-03-01",
            features={"orders_30d": agg("orders", window=window)},
            outcomes={"spend_next_30d": agg("spend", window=("0s", "30d"))},
        ),
    }


def recipe_designs() -> dict[str, Design]:
    """Return one representative, serializable design for each public recipe."""
    window = ("-30d", "0s")

    return {
        "time_series": recipes.time_series(
            every="1mo",
            start="2025-02-01",
            end="2025-02-01",
            features={"orders_30d": agg("orders", window=window)},
            outcomes={"spend_next_14d": agg("spend", window=("0s", "14d"))},
            eligibility={"has_orders": gt(agg("orders", window=window), 0)},
        ),
        "trial_panel": recipes.trial_panel(
            every="1d",
            start="2025-02-01",
            end="2025-02-01",
            treatment=Initiation("orders", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
            features={"orders_30d": agg("orders", window=window)},
            eligibility={"paid_status": eq(asof("status"), "paid")},
        ),
        "event_panel": recipes.event_panel(
            event="orders",
            periods=(-1, 0, 1),
            period="7d",
            features={"orders_30d": agg("orders", window=window)},
        ),
        "ltv": recipes.ltv(
            start_event="orders",
            metric="spend",
            period="7d",
            max_periods=2,
            features={"orders_30d": agg("orders", window=window)},
        ),
        "survival": recipes.survival(
            start_event="orders",
            end_event="orders",
            features={"orders_30d": agg("orders", window=window)},
        ),
        "life_table": recipes.life_table(
            start_event="orders",
            end_event="orders",
            bin="7d",
            group_by={"has_orders": gt(agg("orders", window=window), 0)},
        ),
        "state_panel": recipes.state_panel(
            state_source=BinnedMetricState("spend", cutpoints=(0,), levels=("no_spend", "spend")),
            every="1d",
            start="2025-02-01",
            end="2025-02-02",
            features={"orders_30d": agg("orders", window=window)},
        ),
        "event_sequence": recipes.event_sequence(
            event="orders",
            horizon="30d",
            max_events=3,
            session_gap=None,
            features={"orders_30d": agg("orders", window=window)},
        ),
    }


def _recipe_documents(data: Path) -> dict[str, dict[str, str]]:
    con = ibis.duckdb.connect()
    try:
        model = bind_model(con, data)
        documents = {}
        for name, design in recipe_designs().items():
            definition = DatasetDefinition(base_design=design)
            definition_text = definition.canonical_json().decode()
            frames = {
                "design.json": design.build(model),
                "design.yaml": design.build(model),
                "definition.json": DatasetDefinition.from_json(definition_text).build(model),
            }
            documents[name] = {
                "design.json": to_json(design),
                "design.yaml": to_yaml(design),
                "definition.json": definition_text,
                "expected_schema.json": json.dumps(
                    {document: output_schema(frame) for document, frame in frames.items()},
                    indent=2,
                )
                + "\n",
            }
    finally:
        con.disconnect()
    return documents


def output_schema(frame: Any) -> list[dict[str, str]]:
    """Return the ordered non-support columns of a built Frame's output."""
    return materialized_schema(frame.materialize(budget=_BUDGET))


def materialized_schema(materialized: Any) -> list[dict[str, str]]:
    """Return the ordered non-support columns of a MaterializedFrame."""
    support = {
        *materialized._bookkeeping_columns(),
        *materialized._support_columns(),
        *(name for name in materialized.table.column_names if name.startswith("__terrine_")),
    }
    return [
        {
            "name": field.name,
            "type": str(field.type),
            "role": _role(field.name, materialized.roles, materialized.columns),
        }
        for field in materialized.table.schema
        if field.name not in support
    ]


def _role(name: str, roles: Roles, columns: Any) -> str:
    for role in fields(roles):
        value = getattr(roles, role.name)
        if value == name:
            return role.name
        if isinstance(value, tuple) and name in value:
            return role.name.removesuffix("s")
    metadata = columns.get(name)
    return "output" if metadata is None else metadata.role


@contextmanager
def _reproducible_staging() -> Generator[None]:
    """Seed random identifiers and run DuckDB on one thread, so staged rows keep one order.

    The runtime namespace is the default and retained snapshots use the default location
    beside the owner database.
    """
    generator = random.Random(_SEED)
    connect = duckdb.connect

    def uuid4() -> uuid.UUID:
        return uuid.UUID(int=generator.getrandbits(128), version=4)

    def one_thread(*args: Any, **kwargs: Any) -> Any:
        config = dict(kwargs.pop("config", None) or {})
        config.setdefault("threads", 1)
        return connect(*args, config=config, **kwargs)

    with (
        mock.patch.dict(os.environ, {"TERRINE_RUNTIME_NAMESPACE": NAMESPACE}),
        mock.patch.object(uuid, "uuid4", uuid4),
        mock.patch.object(duckdb, "connect", one_thread),
    ):
        os.environ.pop("TERRINE_RUNTIME_SNAPSHOT_ROOT", None)
        yield


def _relocated_receipt(receipt: ReopenableSnapshot, prefix: str) -> ReopenableSnapshot:
    """Name the receipt's locations relative to the staging directory and reseal it."""
    relocated = replace(
        receipt,
        owner_reference=receipt.owner_reference.replace(prefix, ""),
        artifact_path=receipt.artifact_path.replace(prefix, ""),
        receipt_path=receipt.receipt_path.replace(prefix, ""),
    )
    payload = json.loads(relocated.to_json())
    del payload["receipt_hash"]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return ReopenableSnapshot.from_json(
        replace(relocated, receipt_hash=hashlib.sha256(canonical).hexdigest()).to_json()
    )


def _documents(root: Path, data: Path) -> dict[str, str]:
    stored = designs()
    con = ibis.duckdb.connect(str(root / "owner.duckdb"))
    try:
        model = bind_model(con, data)
        training = (
            stored["design.json"]
            .build(model)
            .training_set(label="spend_next_14d", split=GroupSplit(seed=7))
            .winsorize(Winsorization("orders_30d", lower="0", upper="1"))
        )
        dataset = training.stage("corpus", budget=_BUDGET)
        snapshots = dataset.manifest.source_snapshots
        prefix = f"{root}{os.sep}"
        receipt = _relocated_receipt(
            SnapshotAuthority(RuntimeOwner.from_connection(con)).find_retained(
                snapshots["snapshot_id"], namespace=NAMESPACE
            ),
            prefix,
        )
        manifest = json.loads(dataset.manifest.to_json())
        manifest["source_snapshots"].update(
            owner=receipt.owner_reference, receipt_hash=receipt.receipt_hash
        )
        definition = dataset.manifest.definition.canonical_json()
        if hashlib.sha256(definition).hexdigest() != manifest["definition_hash"]:
            raise RuntimeError("the stored definition does not match the manifest's digest")
        export = dataset.export_parquet(destination=str(root / "export"), budget=_BUDGET)
        parquet = pq.ParquetFile(root / "export" / export.shards[0].path)
        arrow_metadata = {
            key.decode(): value.decode() for key, value in parquet.metadata.metadata.items()
        }
        frames = {
            "design.json": stored["design.json"].build(model),
            "design.yaml": stored["design.yaml"].build(model),
            "definition.json": DatasetDefinition.from_json(definition).build(model),
        }
        documents = {
            "design.json": to_json(stored["design.json"]),
            "design.yaml": to_yaml(stored["design.yaml"]),
            "definition.json": definition.decode(),
            "catalog.yaml": model.to_catalog(),
            "manifest.json": DatasetManifest.from_json(json.dumps(manifest)).to_json(),
            "arrow_metadata.json": json.dumps(arrow_metadata, indent=2, sort_keys=True) + "\n",
            "receipt.json": receipt.to_json(),
            "expected_schema.json": json.dumps(
                {name: output_schema(frame) for name, frame in frames.items()}, indent=2
            )
            + "\n",
        }
    finally:
        con.disconnect()
    return documents


def write_generation(directory: Path) -> None:
    """Write generation documents and update the recipe-specific documents."""
    write_base = not directory.exists()
    with tempfile.TemporaryDirectory(prefix="terrine-corpus-") as temporary:
        root = Path(temporary).resolve()
        staged = root / "generation"
        (staged / "data").mkdir(parents=True)
        for name, table in source_tables().items():
            pq.write_table(table, staged / "data" / f"{name}.parquet")
        if write_base:
            with _reproducible_staging():
                documents = _documents(root, staged / "data")
            for name, text in documents.items():
                if temporary in text or str(root) in text:
                    raise RuntimeError(f"{name} records the staging directory")
                (staged / name).write_text(text, encoding="utf-8")
        with _reproducible_staging():
            recipe_documents = _recipe_documents(staged / "data")
        for name, documents in recipe_documents.items():
            recipe_directory = staged / name
            recipe_directory.mkdir()
            for filename, text in documents.items():
                (recipe_directory / filename).write_text(text, encoding="utf-8")
        if write_base:
            shutil.copytree(staged, directory)
        else:
            directory.mkdir(parents=True, exist_ok=True)
            for name, documents in recipe_documents.items():
                recipe_directory = directory / name
                recipe_directory.mkdir(exist_ok=True)
                for filename, text in documents.items():
                    (recipe_directory / filename).write_text(text, encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--generation", required=True, help="format generation name, such as 1")
    arguments = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9A-Za-z_-]+", arguments.generation):
        parser.error("a generation name contains only letters, digits, '_' and '-'")
    write_generation(ARTIFACTS / arguments.generation)


if __name__ == "__main__":
    main()
