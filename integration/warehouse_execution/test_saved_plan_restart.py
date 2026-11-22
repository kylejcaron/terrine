# ruff: noqa: F811
from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import ibis
import pytest

from terrine.compile.anchors import compile_anchors
from terrine.compile.columns import compile_columns
from terrine.compile.layers import plan_layers
from terrine.compile.materialize import compose_materialized_frame
from terrine.data import Budget, Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.saved_plan import DestinationRef, ModelReference, SavedPlan
from terrine.runtime.state import RuntimeStore
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg
from tests._timing import HANG_GUARD_SECONDS
from tests.compile.test_columns import column_model  # noqa: F401
from tests.compile.test_columns import column_model as column_model
from tests.compile.test_persisted_layers import _persisted_anchors


def _owner_cli(*args):
    env = os.environ.copy()
    env.pop("TERRINE_RUNTIME_SOCKET", None)
    return subprocess.run(
        [sys.executable, "-m", "terrine.runtime.cli", *map(str, args)],
        env=env,
        capture_output=True,
        text=True,
        timeout=HANG_GUARD_SECONDS,
        check=False,
    )


def test_offline_backup_preserves_mixed_case_namespace_generations(tmp_path, monkeypatch):
    from terrine import Dataset, UnitSource
    from terrine.runtime.backup import backup_owner, restore_owner
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.snapshot import SnapshotAuthority

    monkeypatch.setenv("TERRINE_RUNTIME_NAMESPACE", "runtime")
    monkeypatch.delenv("TERRINE_RUNTIME_SNAPSHOT_ROOT", raising=False)
    state = tmp_path / "source.duckdb"
    con = ibis.duckdb.connect(str(state))
    owner = RuntimeOwner.from_connection(con)
    try:
        con.raw_sql(
            "CREATE TABLE events AS SELECT TIMESTAMP '2025-01-01 09:00:00' AS ts, "
            "1::INTEGER AS user_id, 3::INTEGER AS value"
        )
        con.raw_sql("CREATE TABLE users AS SELECT 1::INTEGER AS id")
        model = Model.from_catalog(
            _catalog(),
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-02-01")},
            units={"user": UnitSource("users", "id")},
        )
        frame = Design(
            "user", FixedDate("2025-01-02"), features={"total": agg("total", window=("-1d", "0s"))}
        ).build(model)
        lower = frame.stage("lower")
        monkeypatch.setenv("TERRINE_RUNTIME_NAMESPACE", "RUNTIME")
        upper = frame.stage("upper")
        con.raw_sql("CHECKPOINT")
    finally:
        con.disconnect()

    bundle = tmp_path / "backup"
    inventory = backup_owner(state, owner.snapshot_root, bundle)
    restored = restore_owner(bundle, inventory["digest"], tmp_path / "restored")
    con = ibis.duckdb.connect(restored["state_path"])
    try:
        authority = SnapshotAuthority(RuntimeOwner.from_connection(con))
        for name, namespace, original in (("lower", "runtime", lower), ("upper", "RUNTIME", upper)):
            generation = original.manifest.generation.generation_id
            opened = Dataset.open_generation(name, generation, con=con, namespace="runtime")
            assert opened.to_pyarrow()["total"].to_pylist() == [3.0]
            snapshot = authority.find_retained(
                original.manifest.source_snapshots["snapshot_id"], namespace=namespace
            )
            assert snapshot.namespace == namespace
            assert snapshot.receipt_hash == original.manifest.source_snapshots["receipt_hash"]
    finally:
        con.disconnect()


def test_offline_backup_restores_only_selected_owner_and_exact_request(tmp_path, monkeypatch):
    import shutil
    from pathlib import Path

    from terrine import Dataset
    from terrine.dataset.manifest import DatasetManifest
    from terrine.model import UnitSource
    from terrine.spec.io import canonical_digest

    monkeypatch.setenv("TERRINE_RUNTIME_NAMESPACE", "runtime")
    monkeypatch.delenv("TERRINE_RUNTIME_SNAPSHOT_ROOT", raising=False)
    root = tmp_path / ".terrine" / "snapshots"
    a_path, b_path = tmp_path / "a.duckdb", tmp_path / "b.duckdb"
    a, b = (ibis.duckdb.connect(str(path)) for path in (a_path, b_path))
    design = Design(
        "user", FixedDate("2025-01-02"), features={"total": agg("total", window=("-1d", "0s"))}
    )
    try:
        for con, value in ((a, 3), (b, 87)):
            con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
            con.con.execute("INSERT INTO events VALUES ('2025-01-01 09:00:00', 1, ?)", [value])
            con.raw_sql("CREATE TABLE users AS SELECT 1::INTEGER AS id")
        a_model, b_model = (
            Model.from_catalog(
                _catalog(),
                con=con,
                readiness={"events": Coverage("2025-01-01", "2025-02-01")},
                units={"user": UnitSource("users", "id")},
            )
            for con in (a, b)
        )
        first = design.build(a_model).stage("study")
        a.raw_sql("UPDATE events SET value=5")
        second = first.refresh(a_model)
        foreign = design.build(b_model).stage("study")
        foreign_task, foreign_blob = b.con.execute(
            "SELECT task_id,output_blob FROM runtime.runtime_tasks ORDER BY task_id LIMIT 1"
        ).fetchone()
        foreign_paths = {
            Path(json.loads(blob.split(b"\n", 1)[1])["path"]).relative_to(root)
            for (blob,) in b.con.execute("SELECT output_blob FROM runtime.runtime_tasks").fetchall()
        }
        foreign_snapshot_id = foreign.manifest.source_snapshots["snapshot_id"]
        foreign_paths.update(
            path.relative_to(root)
            for parent in (root / "runtime").glob(f"*-{foreign_snapshot_id}")
            for path in parent.iterdir()
        )
        first_file = tmp_path / "first.json"
        historical = json.loads(first.manifest.to_json())
        historical["definition"]["transforms"] = []
        historical["definition_hash"] = canonical_digest(historical["definition"])
        encoded = json.dumps(historical)
        a.con.execute(
            "UPDATE runtime.runtime_dataset_recipes SET manifest_json=? WHERE generation=?",
            [encoded, first.manifest.generation.generation_id],
        )
        first_file.write_text(encoded, encoding="utf-8")
        key, plan_json = a.con.execute(
            "SELECT request_key,recipe_json FROM runtime.runtime_dataset_recipes "
            "WHERE generation=?",
            [second.manifest.generation.generation_id],
        ).fetchone()
        run_id = a.con.execute(
            "SELECT run_id FROM runtime.runtime_runs WHERE request_key=?", [key]
        ).fetchone()[0]
        plan_file = tmp_path / "saved.plan"
        plan_file.write_text(plan_json, encoding="utf-8")
        a.raw_sql("CHECKPOINT")
        bundle, restored = tmp_path / "bundle", tmp_path / "restored"
        assert _owner_cli("backup", a_path, root, bundle).returncode != 0
        assert not bundle.exists()
        a.disconnect()

        raw_copy = tmp_path / "copy.duckdb"
        shutil.copyfile(a_path, raw_copy)
        copied = ibis.duckdb.connect(str(raw_copy))
        try:
            with pytest.raises(TerrineError) as refused:
                Dataset.from_manifest(DatasetManifest.from_json(first_file.read_text()), con=copied)
            assert refused.value.code == "terrine.dataset.invalid_generation"
        finally:
            copied.disconnect()
        a = ibis.duckdb.connect(str(a_path))
        original_blob = a.con.execute(
            "SELECT output_blob FROM runtime.runtime_tasks WHERE run_id=? AND task_id=?",
            [run_id, foreign_task],
        ).fetchone()[0]
        replace_blob = "UPDATE runtime.runtime_tasks SET output_blob=? WHERE run_id=? AND task_id=?"
        a.con.execute(replace_blob, [foreign_blob, run_id, foreign_task])
        a.disconnect()
        refused = _owner_cli("backup", a_path, root, bundle)
        assert json.loads(refused.stderr)["code"] == "terrine.runtime.backup_invalid"
        assert not bundle.exists()
        a = ibis.duckdb.connect(str(a_path))
        a.con.execute(replace_blob, [original_blob, run_id, foreign_task])
        a.disconnect()

        # A copied foreign reference with a valid digest and an A-shaped path
        # still does not prove which owner produced the file.
        forged = json.loads(foreign_blob.split(b"\n", 1)[1])
        own_path = Path(json.loads(original_blob.split(b"\n", 1)[1])["path"])
        forged_path = own_path.with_name("f" * 32 + ".arrow")
        shutil.copyfile(forged["path"], forged_path)
        forged["path"] = str(forged_path)
        a = ibis.duckdb.connect(str(a_path))
        prefix = foreign_blob.split(b"\n", 1)[0] + b"\n"
        a.con.execute(replace_blob, [prefix + json.dumps(forged).encode(), run_id, foreign_task])
        a.disconnect()
        refused = _owner_cli("backup", a_path, root, bundle)
        assert json.loads(refused.stderr)["reason"] == "arrow_owner_binding"
        assert not bundle.exists()
        a = ibis.duckdb.connect(str(a_path))
        a.con.execute(replace_blob, [original_blob, run_id, foreign_task])
        old_run = a.con.execute(
            "SELECT run_id FROM runtime.runtime_runs WHERE published_generation=?",
            [first.manifest.generation.generation_id],
        ).fetchone()[0]
        a.con.execute(
            replace_blob,
            [b"terrine.arrow.output.v1\n" + json.dumps(forged).encode(), old_run, foreign_task],
        )
        a.disconnect()
        # The old run is terminal: its historical task pointer is not needed to replay.
        forged_path.unlink()

        referenced = Path(json.loads(original_blob.split(b"\n", 1)[1])["path"])
        hidden = referenced.with_suffix(".missing")
        referenced.rename(hidden)
        try:
            refused = _owner_cli("backup", a_path, root, bundle)
            assert json.loads(refused.stderr)["code"] == "terrine.runtime.backup_invalid"
            assert not bundle.exists()
        finally:
            hidden.rename(referenced)

        # A is offline; B holds an uncommitted write and can still publish.
        b.con.execute("BEGIN TRANSACTION")
        b.raw_sql("UPDATE events SET value=91")

        result = _owner_cli("backup", a_path, root, bundle)
        assert result.returncode == 0, result.stderr
        inventory = json.loads(result.stdout)
        files = {item["path"] for item in inventory["files"]}
        assert "owner.duckdb" in files
        assert files.isdisjoint(f".terrine/snapshots/{path}" for path in foreign_paths)
        assert all(item["size"] > 0 for item in inventory["files"])

        b.con.execute("COMMIT")
        assert foreign.refresh(b_model).to_pyarrow()["total"].to_pylist() == [91.0]
        assert all((root / path).is_file() for path in foreign_paths)

        result = _owner_cli("restore", bundle, "0" * 64, restored)
        assert result.returncode != 0
        assert not restored.exists()
        interrupted = subprocess.run(
            [
                sys.executable,
                "-c",
                "import os,sys; from terrine.runtime.cli import main; "
                "os.fsync=lambda _: os._exit(73); main(sys.argv[1:])",
                "restore",
                str(bundle),
                inventory["digest"],
                str(restored),
            ],
            check=False,
            timeout=HANG_GUARD_SECONDS,
        )
        assert interrupted.returncode == 73
        assert not restored.exists()
        result = _owner_cli("restore", bundle, inventory["digest"], restored)
        assert result.returncode == 0, result.stderr
        assert not any((restored / ".terrine" / "snapshots" / p).exists() for p in foreign_paths)
        assert _owner_cli("restore", bundle, inventory["digest"], restored).returncode == 2
        original = tmp_path / "offline.duckdb"
        a_path.rename(original)
        for name in files - {"owner.duckdb"}:
            artifact = tmp_path / name
            offline = tmp_path / "offline-artifacts" / name
            offline.parent.mkdir(parents=True, exist_ok=True)
            artifact.rename(offline)
        second_id = second.manifest.generation.generation_id
        code = """
import json
import sys
from contextlib import closing

import ibis
from pathlib import Path
from terrine import Dataset
from terrine.dataset.manifest import DatasetManifest
from terrine.errors import TerrineError
from terrine.model import Model, UnitSource
from terrine.data import Coverage
from terrine.runtime import get_run, read_run_events, run_serialized_plan
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.semantic.io import validate_catalog
from dataclasses import replace
from terrine.spec.dataset import DatasetDefinition, DatasetDefinitionStore
from terrine.spec.io import _canonical_bytes

state, manifest, second, plan, key, run_id = sys.argv[1:]
con = ibis.duckdb.connect(state)
first = Dataset.from_manifest(DatasetManifest.from_json(Path(manifest).read_text()), con=con)
assert first.to_pyarrow()['total'].to_pylist() == [3.0]
assert first.manifest.definition_hash == json.loads(Path(manifest).read_text())['definition_hash']
assert Dataset.open_generation(
    'study', first.manifest.generation.generation_id, con=con, namespace='runtime',
).manifest.to_json() == first.manifest.to_json()
saved_manifest = json.loads(Path(manifest).read_text())
definition = saved_manifest.pop('definition')
store = DatasetDefinitionStore(Path(state).parent / 'definitions', owner='study-definitions')
store.root.mkdir()
digest = saved_manifest['definition_hash']
(store.root / f'{digest}.json').write_bytes(_canonical_bytes(definition))
saved_manifest['definition_reference'] = {'owner': store.owner, 'digest': digest}
referenced = DatasetManifest.from_json(json.dumps(saved_manifest), definition_store=store)
assert Dataset.from_manifest(referenced, con=con).to_pyarrow()['total'].to_pylist() == [3.0]
changed_design = replace(referenced.definition.base_design, gap='1d')
for forged in (
    replace(referenced, definition=DatasetDefinition(base_design=changed_design)),
    replace(referenced, columns=referenced.columns[:-1]),
):
    try:
        Dataset.from_manifest(forged, con=con)
    except TerrineError as error:
        assert error.code == 'terrine.dataset.invalid_generation'
    else:
        raise AssertionError('altered referenced manifest authenticated')
latest = Dataset.open_generation('study', second, con=con, namespace='runtime')
assert latest.to_pyarrow()['total'].to_pylist() == [5.0]
assert con.con.execute('SELECT value FROM events').fetchone() == (5,)
authority = SnapshotAuthority(RuntimeOwner.from_connection(con))
snapshot_id = first.manifest.source_snapshots['snapshot_id']
receipt = authority.find_retained(snapshot_id, namespace='runtime')
assert receipt.receipt_hash == first.manifest.source_snapshots['receipt_hash']
with closing(authority.reopen(receipt, con=con, namespace='runtime')) as cx:
    catalog = validate_catalog(json.loads(receipt.catalog_json))
    assert cx.execute(cx.resolve(catalog.sources['events'].relation)).to_pylist()[0]['value'] == 3
saved = Path(plan).read_bytes()
run = run_serialized_plan(saved, con=con.con, namespace='runtime', request_key=key)
assert (run.run_id, run.published_generation) == (run_id, second)
first_id = first.manifest.generation.generation_id
first_key, first_plan = con.con.execute(
    "SELECT request_key,recipe_json FROM runtime.runtime_dataset_recipes WHERE generation=?",
    [first_id],
).fetchone()
first_replay = run_serialized_plan(
    first_plan, con=con.con, namespace='runtime', request_key=first_key,
)
assert first_replay.published_generation == first_id
assert get_run(run_id, con=con.con, namespace='runtime').durable_state == 'succeeded'
assert read_run_events(run_id, con=con.con, namespace='runtime').events[-1].kind == 'lifecycle'
store = RuntimeStore(con.con, namespace='runtime')
assert any(kind == 'saved_plan' for kind, _ in store.snapshot_references(snapshot_id))
try:
    run_serialized_plan(saved, con=con.con, namespace='runtime', request_key='other')
except TerrineError as error:
    assert error.code == 'terrine.runtime.saved_plan_mismatch'
else:
    raise AssertionError('a new request reused the imported destination')
try:
    Dataset.from_manifest(first.manifest, con=con, namespace='other')
except TerrineError as error:
    assert error.code == 'terrine.dataset.invalid_generation'
else:
    raise AssertionError('imported manifest reopened in another namespace')
con.raw_sql("UPDATE events SET value=11")
model = Model.from_catalog(
    catalog, con=con, readiness={'events': Coverage('2025-01-01', '2025-02-01')},
    units={'user': UnitSource('users', 'id')},
)
fresh = latest.refresh(model)
assert fresh.to_pyarrow()['total'].to_pylist() == [11.0]
assert fresh.manifest.source_snapshots['owner'] == authority.owner.reference
assert first.to_pyarrow()['total'].to_pylist() == [3.0]
stored, = con.con.execute(
    "SELECT manifest_json FROM runtime.runtime_dataset_recipes WHERE generation=?", [first_id],
).fetchone()
con.con.execute(
    "UPDATE runtime.runtime_dataset_recipes SET manifest_json=? WHERE generation=?",
    [stored + ' ', first_id],
)
try:
    Dataset.from_manifest(referenced, con=con)
except TerrineError as error:
    assert error.code == 'terrine.dataset.invalid_generation'
else:
    raise AssertionError('changed persisted manifest bytes authenticated')
con.con.execute(
    "UPDATE runtime.runtime_dataset_recipes SET manifest_json=? WHERE generation=?",
    [stored, first_id],
)
table, = con.con.execute(
    "SELECT table_name FROM runtime.runtime_partitions "
    "WHERE dataset='study' AND generation=? AND partition=?",
    [first.manifest.generation.generation_id, first.manifest.generation.output_refs[0]],
).fetchone()
con.con.execute(f'UPDATE runtime."{table}" SET total=77')
try:
    Dataset.open_generation('study', first.manifest.generation.generation_id,
                            con=con, namespace='runtime')
except TerrineError as error:
    assert error.code == 'terrine.dataset.invalid_generation'
else:
    raise AssertionError('corrupt imported generation opened')
con.disconnect()
"""
        restarted = subprocess.run(
            [
                sys.executable,
                "-c",
                code,
                str(restored / "owner.duckdb"),
                str(first_file),
                second_id,
                str(plan_file),
                key,
                run_id,
            ],
            capture_output=True,
            text=True,
            timeout=HANG_GUARD_SECONDS,
            check=False,
        )
        assert restarted.returncode == 0, restarted.stderr
        original_con = ibis.duckdb.connect(str(original), read_only=True)
        try:
            assert original_con.con.execute("SELECT value FROM events").fetchone() == (5,)
            assert original_con.con.execute(
                "SELECT count(*) FROM runtime.runtime_publications"
            ).fetchone() == (2,)
        finally:
            original_con.disconnect()
    finally:
        a.disconnect()
        b.disconnect()


def _catalog() -> Catalog:
    return Catalog.model_validate(
        {
            "schema_version": 3,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["value"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "events": {"value": {"source": "events", "column": "value"}},
            "properties": {},
            "dimensions": {},
            "measures": {"total": {"event": "value", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "unit": "user", "measure": "total"}},
        }
    )


def _model(con, catalog: Catalog) -> Model:
    return Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )


def test_verified_planned_request_can_start_at_restored_owner(tmp_path):
    from datetime import timedelta

    from terrine.model import UnitSource
    from terrine.runtime import open_published_frame, run_serialized_plan
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner, PlanRequest
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.runtime.state import RuntimeStore

    state = tmp_path / "owner.duckdb"
    owner = RuntimeOwner(state, tmp_path / "inputs")
    catalog = _catalog()

    def model_for(con):
        return Model.from_catalog(
            catalog,
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-02-01")},
            units={"user": UnitSource("users", "id")},
        )

    class Resolver:
        def resolve(self, reference):
            assert reference.owner_key == "workspace/model"
            return model_for(con)

    con = ibis.duckdb.connect(str(state))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01', 1, 8)")
    con.raw_sql("CREATE TABLE users AS SELECT 1 id")
    design = Design(
        "user", FixedDate("2025-01-02"), features={"total": agg("total", window=("-1d", "0s"))}
    )
    captured = SnapshotAuthority(owner).capture(
        model_for(con),
        design=design,
        con=con,
        namespace="runtime",
        run_id="unused",
        budget=Budget(),
    )
    invocation = OwnerPlanner(
        resolver=Resolver(), snapshot_authority=SnapshotAuthority(owner)
    ).plan(
        PlanRequest(
            "only-planned",
            ModelReference.from_model(model_for(con), owner_key="workspace/model"),
            design,
            DestinationRef("output.duckdb", "runtime", "study"),
        ),
        con=con.con,
        namespace="runtime",
    )
    assert con.con.execute("SELECT count(*) FROM runtime.runtime_runs").fetchone() == (0,)
    con.disconnect()

    bundle, destination = tmp_path / "bundle", tmp_path / "restored"
    backed = _owner_cli("backup", state, owner.snapshot_root, bundle)
    assert backed.returncode == 0, backed.stderr
    restored = _owner_cli("restore", bundle, json.loads(backed.stdout)["digest"], destination)
    assert restored.returncode == 0, restored.stderr
    state.rename(tmp_path / "offline.duckdb")
    owner.snapshot_root.rename(tmp_path / "offline-inputs")
    con = ibis.duckdb.connect(str(destination / "owner.duckdb"))
    owner = RuntimeOwner.from_connection(con)

    def submit(key, *, authority=None):
        return run_serialized_plan(
            invocation.saved_plan_json,
            request_key=key,
            con=con.con,
            namespace="runtime",
            resolver=Resolver(),
            snapshot_authority=authority or SnapshotAuthority(owner),
        )

    try:
        with pytest.raises(TerrineError) as error:
            submit("other")
        assert error.value.code == "terrine.runtime.saved_plan_mismatch"
        assert con.con.execute("SELECT count(*) FROM runtime.runtime_runs").fetchone() == (0,)
        assert con.con.execute("SELECT count(*) FROM runtime.runtime_manifests").fetchone() == (0,)
        wrong_owner = RuntimeOwner(tmp_path / "offline.duckdb", owner.snapshot_root)
        with pytest.raises(TerrineError) as unbound:
            submit(invocation.request_key, authority=SnapshotAuthority(wrong_owner))
        assert unbound.value.code == "terrine.runtime.snapshot_invalid"
        run = submit(invocation.request_key)
        assert run.state == "succeeded"
        frame = open_published_frame(run, con=con.con, namespace="runtime")
        assert frame.table["total"].to_pylist() == [8.0]
        assert submit(invocation.request_key) == run

        authority = SnapshotAuthority(owner)
        store = RuntimeStore(con.con, namespace="runtime")
        snapshot_id = captured.snapshot_id
        (snapshot_dir,) = (owner.snapshot_root / "runtime").glob(f"*-{snapshot_id}")
        receipt = snapshot_dir / "receipt.json"
        inputs = snapshot_dir / "inputs.duckdb"
        inputs.unlink()
        with pytest.raises(TerrineError) as missing:
            authority.collect_unreferenced(store, namespace="runtime", retain=timedelta(0))
        assert missing.value.code == "terrine.runtime.snapshot_invalid"
        assert receipt.is_file()

        assert store.mark_snapshot_for_gc(snapshot_id)
        encoded = receipt.read_bytes()
        receipt.write_bytes(b"{}")
        with pytest.raises(TerrineError) as corrupt:
            authority.collect_unreferenced(store, namespace="runtime", retain=timedelta(0))
        assert corrupt.value.code == "terrine.runtime.snapshot_invalid"
        assert receipt.is_file()
        receipt.write_bytes(encoded)
        foreign = tmp_path / "offline.duckdb"
        inputs.symlink_to(foreign)
        with pytest.raises(TerrineError) as linked:
            authority.collect_unreferenced(store, namespace="runtime", retain=timedelta(0))
        assert linked.value.code == "terrine.runtime.snapshot_invalid"
        assert receipt.is_file() and foreign.is_file()
        inputs.unlink()
        assert authority.collect_unreferenced(
            store, namespace="runtime", retain=timedelta(0)
        ) == (snapshot_id,)
        assert not snapshot_dir.exists()
    finally:
        con.disconnect()


@pytest.mark.parametrize("interrupted", [False, True, "relocated"])
def test_saved_plan_materializes_retained_model_and_design_in_fresh_process(
    tmp_path, interrupted, monkeypatch
):
    import pyarrow as pa

    from terrine.model import UnitSource
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.runtime.state import RuntimeStore
    from terrine.spec.dataset import DatasetDefinition

    monkeypatch.delenv("TERRINE_RUNTIME_SNAPSHOT_ROOT", raising=False)

    state_path = tmp_path / "owner.duckdb"
    snapshot_root = tmp_path / "snapshots"
    con = ibis.duckdb.connect(str(state_path))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01 09:00:00', 1, 3)")
    con.raw_sql("CREATE TABLE users(id INTEGER)")
    con.raw_sql("INSERT INTO users VALUES (1)")
    catalog = _catalog()
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
        units={"user": UnitSource("users", "id")},
    )
    design = Design(
        "user", FixedDate("2025-01-02"), features={"total": agg("total", window=("-1d", "0s"))}
    )
    expected = design.build(model).materialize(budget=Budget())
    expected_path = tmp_path / "expected.arrow"
    with pa.OSFile(str(expected_path), "wb") as sink:
        with pa.ipc.new_file(sink, expected.to_pyarrow().schema) as writer:
            writer.write_table(expected.to_pyarrow())
    owner = RuntimeOwner(state_path=state_path, snapshot_root=snapshot_root)
    snapshot = SnapshotAuthority(owner).capture(
        model, design=design, con=con, namespace="runtime", run_id="planned", budget=Budget()
    )
    plan = plan_refresh(
        namespace="runtime",
        dataset="study",
        definition_hash=design.spec_hash,
        snapshot_id=snapshot.snapshot_id,
        execution_mode="design",
        graph=dependency_graph(model, design, sources=("events",), partitions=("all",)),
    )
    saved_plan = SavedPlan.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference.from_model(
            model, owner_key=owner.reference if interrupted is not True else "workspace/model"
        ),
        destination=DestinationRef(
            str(state_path) if interrupted else "output.duckdb", "analytics", "study"
        ),
        namespace="runtime",
        expected_base_manifest=None,
        dataset_definition=DatasetDefinition(base_design=design)
        if interrupted == "relocated"
        else None,
    )
    store = RuntimeStore(con.con, namespace="runtime")
    store.initialize()
    saved_plan.pin_snapshot(store)
    if interrupted == "relocated":
        con.con.execute(
            "INSERT INTO runtime.runtime_dataset_requests VALUES (?,?)",
            ["fresh-process", saved_plan.to_json()],
        )
    saved_plan_path = tmp_path / "plan.json"
    catalog_path = tmp_path / "catalog.json"
    saved_plan_path.write_text(saved_plan.to_json(), encoding="utf-8")
    catalog_path.write_text(
        json.dumps(catalog.model_dump(mode="json"), sort_keys=True), encoding="utf-8"
    )
    con.raw_sql("UPDATE events SET value=99")
    con.disconnect()
    script = r"""
import json, os, sys, threading
import ibis
import pyarrow as pa
from terrine.data import Coverage, MaterializedFrame
from terrine.model import Model, UnitSource
from terrine.runtime import run_serialized_plan, open_published_frame
from terrine.runtime.api import DesignExecutor
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.saved_plan import SavedPlan
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.semantic.models import Catalog

state_path, snapshot_root, saved_plan_path, catalog_path, expected_path, interrupted = sys.argv[1:]
connection = ibis.duckdb.connect(state_path)
class OwnerResolver:
    def resolve(self, reference):
        assert reference.owner_key in {
            'workspace/model', RuntimeOwner(state_path, snapshot_root).reference,
        }
        catalog = Catalog.model_validate(json.loads(open(catalog_path).read()))
        return Model.from_catalog(catalog, con=connection,
            readiness={'events': Coverage('2025-01-01', '2025-02-01')},
            units={'user': UnitSource('users', 'id')})
serialized = open(saved_plan_path).read()
if interrupted == 'True':
    original_task = DesignExecutor.run_snapshot_task
    completed = []
    def interrupted_task(self, context, task, dependencies):
        if completed:
            while connection.con.execute(
                "SELECT heartbeat_at FROM runtime.runtime_runs"
            ).fetchone()[0] is None:
                threading.Event().wait(0.01)
            os._exit(73)
        result = original_task(self, context, task, dependencies)
        completed.append(task.task_id)
        return result
    DesignExecutor.run_snapshot_task = interrupted_task
saved_plan = SavedPlan.from_json(serialized)
owner = RuntimeOwner(state_path=state_path, snapshot_root=snapshot_root)
receipt = run_serialized_plan(serialized, con=connection.con, namespace='runtime',
    request_key='fresh-process', resolver=OwnerResolver(),
    snapshot_authority=SnapshotAuthority(owner))
assert receipt.state == 'succeeded'
published = open_published_frame(receipt, con=connection.con, namespace='runtime')
expected = MaterializedFrame.from_arrow(pa.ipc.open_file(expected_path).read_all())
assert published.table.to_pylist() == expected.table.to_pylist()
assert published.columns == expected.columns
assert published.table.schema.remove_metadata() == expected.table.schema.remove_metadata()
assert published.table.to_pylist()[0]['total'] == 3
assert published.provenance.execution_snapshot.snapshot_id == (
    json.loads(saved_plan.plan_json)['snapshot_id'])
assert run_serialized_plan(serialized, con=connection.con, namespace='runtime',
    request_key='fresh-process') == receipt
assert connection.table('events').execute()['value'].tolist() == [99]
connection.disconnect()
"""
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(state_path),
            str(snapshot_root),
            str(saved_plan_path),
            str(catalog_path),
            str(expected_path),
            str(bool(interrupted)),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=HANG_GUARD_SECONDS,
        env=os.environ.copy(),
    )
    assert process.returncode == (73 if interrupted else 0), process.stderr
    if interrupted:
        from pathlib import Path
        from uuid import uuid4

        from terrine.runtime import get_run, open_published_frame
        from terrine.runtime.control import ControlClient, ControlEndpoint, ControlServer

        reopened = ibis.duckdb.connect(str(state_path))
        try:
            run_id = reopened.con.execute("SELECT run_id FROM runtime.runtime_runs").fetchone()[0]
            disconnected = get_run(run_id, con=reopened.con, namespace="runtime")
            assert disconnected.durable_state == "running"
            assert disconnected.heartbeat_at is not None
            assert disconnected.phase == "executing"
            assert disconnected.progress["completed_tasks"] == 1
            retained_task = reopened.con.execute(
                "SELECT task_id,output_blob,task_fence FROM runtime.runtime_tasks "
                "WHERE status='complete'"
            ).fetchone()
            reopened.con.execute(
                "UPDATE runtime.runtime_leases SET expires_at=TIMESTAMP '2000-01-01'"
            )
            stale = get_run(run_id, con=reopened.con, namespace="runtime")
            if interrupted == "relocated":
                task_id, blob, _ = retained_task
                legacy = b"terrine.arrow.output.v1\n" + blob.split(b"\n", 1)[1]
                update = "UPDATE runtime.runtime_tasks SET output_blob=? WHERE task_id=?"
                reopened.con.execute(update, [legacy, task_id])
                reopened.disconnect()
                refused = _owner_cli("backup", state_path, snapshot_root, tmp_path / "legacy")
                assert json.loads(refused.stderr)["reason"] == "legacy_retry_artifact_unbound"
                assert not (tmp_path / "legacy").exists()
                reopened = ibis.duckdb.connect(str(state_path))
                reopened.con.execute(update, [blob, task_id])
                reopened.raw_sql("CHECKPOINT")
                reopened.disconnect()
                bundle, destination = tmp_path / "backup", tmp_path / "new-owner"
                backed = _owner_cli("backup", state_path, snapshot_root, bundle)
                assert backed.returncode == 0, backed.stderr
                restored = _owner_cli(
                    "restore", bundle, json.loads(backed.stdout)["digest"], destination
                )
                assert restored.returncode == 0, restored.stderr
                state_path.rename(tmp_path / "offline.duckdb")
                snapshot_root.rename(tmp_path / "offline-snapshots")
                state_path = destination / "owner.duckdb"
                reopened = ibis.duckdb.connect(str(state_path))
                owner = RuntimeOwner(state_path, destination / ".terrine" / "snapshots")
            assert stale.durable_state == "running"
            assert stale.freshness == "stale"
            assert stale.heartbeat_at == disconnected.heartbeat_at

            class Resolver:
                def resolve(self, reference):
                    expected_key = (
                        owner.reference if interrupted == "relocated" else "workspace/model"
                    )
                    assert reference.owner_key == expected_key
                    return Model.from_catalog(
                        catalog,
                        con=reopened,
                        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
                        units={"user": UnitSource("users", "id")},
                    )

            endpoint = ControlEndpoint(Path(f"/tmp/terrine-resume-{uuid4().hex}.sock"))
            server = ControlServer(
                RuntimeStore(reopened.con, namespace="runtime"),
                namespace="runtime",
                endpoint=endpoint,
                resolver=Resolver(),
                snapshot_authority=SnapshotAuthority(owner),
            )
            server.start()
            try:
                client = ControlClient(endpoint, namespace="runtime")
                recovered = client.get_run(run_id)
                assert recovered.durable_state == "failed"
                assert recovered.error_code == "terrine.runtime.owner_interrupted"
                resumed = client.run(saved_plan.to_json(), request_key="fresh-process")
                assert resumed.run_id == run_id
                assert resumed.state == "succeeded"
                frame = open_published_frame(resumed, con=reopened.con, namespace="runtime")
                assert frame.table.to_pylist() == expected.table.to_pylist()
                assert frame.columns == expected.columns
                assert (
                    reopened.con.execute(
                        "SELECT task_id,output_blob,task_fence FROM runtime.runtime_tasks "
                        "WHERE task_id=?",
                        [retained_task[0]],
                    ).fetchone()
                    == retained_task
                )
                resumed_events = client.read_run_events(
                    run_id, after=disconnected.latest_event_cursor
                )
                assert [event.payload["state"] for event in resumed_events.events] == [
                    "failed",
                    "running",
                    "succeeded",
                ]
                assert client.get_run(run_id).progress["completed_tasks"] == len(plan.tasks)
            finally:
                server.close()
            if interrupted == "relocated":
                from terrine import Dataset
                from terrine.dataset.reference import dataset_from_run

                result = dataset_from_run(
                    saved_plan,
                    resumed,
                    model=Resolver().resolve(replace(saved_plan.model, owner_key=owner.reference)),
                    namespace="runtime",
                    request_key="fresh-process",
                    snapshot_authority=SnapshotAuthority(owner),
                )
                assert result.manifest.source_snapshots["owner"] == owner.reference
                reopened.disconnect()
                bundle, destination = tmp_path / "second-backup", tmp_path / "second-restore"
                backed = _owner_cli("backup", state_path, owner.snapshot_root, bundle)
                assert backed.returncode == 0, backed.stderr
                restored = _owner_cli(
                    "restore", bundle, json.loads(backed.stdout)["digest"], destination
                )
                assert restored.returncode == 0, restored.stderr
                restored_con = ibis.duckdb.connect(str(destination / "owner.duckdb"))
                try:
                    migrated = Dataset.from_manifest(
                        result.manifest, con=restored_con, namespace="runtime"
                    )
                    assert migrated.to_pyarrow()["total"].to_pylist() == [3.0]
                finally:
                    restored_con.disconnect()
        finally:
            reopened.disconnect()


def test_reopenable_snapshot_resumes_in_fresh_process_after_source_mutation(tmp_path):
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.snapshot import SnapshotAuthority

    state_path = tmp_path / "snapshot-owner.duckdb"
    snapshot_root = tmp_path / "snapshots"
    con = ibis.duckdb.connect(str(state_path))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
    catalog = _catalog()
    model = _model(con, catalog)
    owner = RuntimeOwner(state_path=state_path, snapshot_root=snapshot_root)
    design = Design(
        "user", FixedDate("2025-01-15"), features={"total": agg("total", window=("-1d", "0s"))}
    )
    receipt = SnapshotAuthority(owner).capture(
        model,
        design=design,
        con=con,
        namespace="runtime",
        run_id="snapshot-run",
        budget=Budget(),
    )
    con.raw_sql("UPDATE events SET value=99")
    con.disconnect()

    script = r"""
import json, sys
import ibis
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.snapshot import ReopenableSnapshot, SnapshotAuthority
from terrine.semantic.io import validate_catalog

state_path, snapshot_root, receipt_path = sys.argv[1:]
owner = RuntimeOwner(state_path=state_path, snapshot_root=snapshot_root)
receipt = ReopenableSnapshot.from_json(open(receipt_path).read())
catalog = validate_catalog(json.loads(receipt.catalog_json))
connection = ibis.duckdb.connect(state_path)
execution = SnapshotAuthority(owner).reopen(receipt, con=connection, namespace='runtime')
relation = execution.resolve(catalog.sources['events'].relation)
assert execution.execute(relation).to_pylist()[0]['value'] == 3
execution.close()
connection.disconnect()
"""
    subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(state_path),
            str(snapshot_root),
            receipt.receipt_path,
        ],
        check=True,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )


def test_state_panel_composition_matches_full_rebuild(column_model):
    from terrine.compile.dispatch import compile_design
    from terrine.compile.layers import build_layer
    from terrine.compile.materialize import compose_materialized_frame
    from terrine.spec import BinnedMetricState, Schedule, StatePanel

    design = Design(
        "user",
        Schedule("1d", "2025-01-02", "2025-01-02"),
        shape=StatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("none", "some")), periods=2
        ),
    )
    persisted = {
        replace(key, snapshot_id="snapshot"): build_layer(design, column_model, key)
        for key in plan_layers(design, column_model).keys
    }
    actual = compose_materialized_frame(design, column_model, persisted=persisted)
    expected = compile_design(design, column_model)
    assert column_model.con.to_pyarrow(actual.table.order_by("unit")).equals(
        column_model.con.to_pyarrow(expected.table.order_by("unit"))
    )
    assert actual.roles == expected.roles
    assert actual.columns == expected.columns
    assert actual.expression_timings == expected.expression_timings


@pytest.mark.parametrize("shape", ["sequential", "event_panel"])
def test_saved_panel_publication_preserves_ordinary_provenance(column_model, tmp_path, shape):
    import ibis

    from terrine.data import Budget, Coverage
    from terrine.model import Model
    from terrine.runtime import open_published_frame, run_serialized_plan
    from terrine.runtime.graph import dependency_graph
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planner import plan_refresh
    from terrine.runtime.saved_plan import DestinationRef, ModelReference, SavedPlan
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.spec import EventAnchor, EventPanel, Initiation, Schedule, Sequential

    owner = RuntimeOwner(
        state_path=tmp_path / "panel_state.duckdb", snapshot_root=tmp_path / "snapshots"
    )
    con = ibis.duckdb.connect(str(owner.state_path))
    try:
        for table in ("events", "users"):
            con.create_table(table, column_model.con.to_pyarrow(column_model.con.table(table)))
        con.raw_sql("INSERT INTO users VALUES ('c')")
        model = Model.from_catalog(
            column_model.catalog,
            con=con,
            units=column_model.units,
            readiness={"events": Coverage("2025-01-01", "2025-01-05")},
        )
        if shape == "sequential":
            design = Sequential(
                Design(
                    "user",
                    Schedule("1d", "2025-01-03", "2025-01-04"),
                    features={"total": agg("total", window=("-1d", "0s"))},
                ),
                treatment=Initiation("amount", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
            )
        else:
            design = EventPanel(
                Design(
                    "user",
                    EventAnchor("amount", "first"),
                    features={"total": agg("total", window=("-1h", "0s"))},
                ),
                periods=(-1, 0, 1),
                period="1h",
            )
        budget = Budget(max_rows=100, max_output_bytes=1_000_000)
        expected = design.build(model).materialize(budget=budget)
        assert expected.table.num_rows > 0
        authority = SnapshotAuthority(owner)
        snapshot = authority.capture(
            model, design=design, con=con, namespace="owner", run_id=shape, budget=budget
        )
        plan = plan_refresh(
            namespace="owner",
            dataset="panels",
            definition_hash=design.spec_hash,
            snapshot_id=snapshot.snapshot_id,
            execution_mode="design",
            graph=dependency_graph(model, design, sources=("events",)),
            budget=budget,
        )
        saved_plan = SavedPlan.create(
            plan_json=plan.serialize(),
            design=design,
            model=ModelReference.from_model(model, owner_key="panel-model"),
            destination=DestinationRef(str(owner.state_path), "published", "panels"),
            namespace="owner",
            expected_base_manifest=None,
        )
        store = RuntimeStore(con.con, namespace="owner")
        store.initialize()
        saved_plan.pin_snapshot(store)

        class Resolver:
            def resolve(self, reference):
                return model

        receipt = run_serialized_plan(
            saved_plan.to_json(),
            con=con.con,
            namespace="owner",
            request_key=shape,
            resolver=Resolver(),
            snapshot_authority=authority,
        )
        published = open_published_frame(receipt, con=con.con, namespace="owner")
        order = [("unit", "ascending"), ("t0", "ascending")]
        assert published.table.sort_by(order).equals(expected.table.sort_by(order))
        assert published.columns == expected.columns
        assert published.provenance.shape == expected.provenance.shape == shape
        assert published.provenance.panel == expected.provenance.panel
        assert published.provenance.expression_timings == expected.provenance.expression_timings
    finally:
        con.disconnect()


def test_persisted_anchor_reuse_matches_ordinary_frame_compiler(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-03T00:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    ordinary = compile_columns(design, column_model)
    persisted = _persisted_anchors(design, column_model)
    anchor_key = next(
        key for key in plan_layers(design, column_model).keys if key.layer == "anchor"
    )
    anchor_table = compile_anchors(design, column_model).table
    anchor_arrow = column_model.con.to_pyarrow(anchor_table)
    persisted_anchor = ibis.memtable(anchor_arrow)
    persisted[replace(anchor_key, snapshot_id="same-snapshot")] = persisted_anchor
    reused = compose_materialized_frame(design, column_model, persisted=persisted)
    assert reused._anchors.table is persisted_anchor
    ordinary.validate(execute=column_model.con.to_pyarrow)
    reused.validate(execute=column_model.con.to_pyarrow)
    assert column_model.con.to_pyarrow(ordinary.audit_table.order_by("unit")).equals(
        column_model.con.to_pyarrow(reused.audit_table.order_by("unit"))
    )
    assert ordinary.columns == reused.columns


def test_runtime_and_frame_share_unknown_eligibility_refusal(column_model):
    from terrine.runtime.materialize import compose_runtime_frame
    from terrine.spec import all_of, asof, eq, gt

    design = Design(
        "user",
        FixedDate("2025-01-03T00:00:00+00:00"),
        eligibility={
            "eligible": all_of(
                eq(asof("tier", before="0s"), "not-present"),
                gt(asof("tier", before="-2d"), "a"),
            )
        },
    )
    frame = design.build(column_model)
    refusals = []
    for operation in (
        lambda: frame.materialize(),
        lambda: compose_runtime_frame(design, column_model, persisted={}),
    ):
        with pytest.raises(TerrineError) as error:
            operation()
        refusals.append((error.value.code, error.value.context))
    assert refusals[0] == refusals[1]
    assert refusals[0][0] == "terrine.study.eligibility_unknown"


@pytest.mark.parametrize("shape", ["sequential", "event_panel"])
def test_persisted_shape_matrix_preserves_panel_rows_and_metadata(column_model, shape):
    from datetime import UTC, datetime

    from terrine.compile.dispatch import compile_design
    from terrine.data import Coverage
    from terrine.spec import EventAnchor, EventPanel, Initiation, Schedule, Sequential

    column_model = Model.from_catalog(
        column_model.catalog,
        con=column_model.con,
        units=column_model.units,
        readiness={"events": Coverage("2025-01-01", "2025-01-05")},
    )
    if shape == "sequential":
        design = Sequential(
            Design(
                "user",
                Schedule("12h", "2025-01-03", "2025-01-04"),
                features={"total": agg("total", window=("-1d", "0s"))},
            ),
            treatment=Initiation("amount", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
        )
    else:
        design = EventPanel(
            Design(
                "user",
                EventAnchor("amount", "first"),
                features={"total": agg("total", window=("-1h", "0s"))},
            ),
            periods=(-1, 0, 1),
            period="1h",
        )
    ordinary = compile_design(design, column_model)
    persisted = _persisted_anchors(design, column_model)
    anchor_key = next(
        key for key in plan_layers(design, column_model).keys if key.layer == "anchor"
    )
    parent = ibis.memtable(column_model.con.to_pyarrow(compile_anchors(design, column_model).table))
    persisted[replace(anchor_key, snapshot_id="same-snapshot")] = parent
    reused = compose_materialized_frame(design, column_model, persisted=persisted)
    ordinary.validate(execute=column_model.con.to_pyarrow)
    reused.validate(execute=column_model.con.to_pyarrow)
    assert column_model.con.to_pyarrow(ordinary.audit_table.order_by("unit", "t0")).equals(
        column_model.con.to_pyarrow(reused.audit_table.order_by("unit", "t0"))
    )
    assert reused.roles == ordinary.roles
    assert reused.columns == ordinary.columns
    assert reused.expression_timings == ordinary.expression_timings
    assert reused.sequence_meta == ordinary.sequence_meta
    assert reused.range_plans == ordinary.range_plans


@pytest.mark.parametrize("state_kind", ["binned", "observed"])
@pytest.mark.parametrize(
    ("every", "totals", "fresh_totals"),
    [
        ("1d", [5, 10], [105, 210]),
        ("12h", [5, 8, 10, 7], [105, 208, 210, 107]),
    ],
)
def test_state_panel_consumes_frozen_snapshot_and_scheduled_anchors(
    column_model, state_kind, every, totals, fresh_totals
):
    from terrine.compile.dispatch import compile_design
    from terrine.compile.layers import build_layer
    from terrine.data import Coverage
    from terrine.spec import BinnedMetricState, Schedule, StateObservations, StatePanel

    con = column_model.con
    con.raw_sql(
        "INSERT INTO events VALUES "
        "('a', '2025-01-01 12:00:00+00', 5, 'old'), "
        "('a', '2025-01-03 12:00:00+00', 6, 'new')"
    )
    definition = column_model.catalog.model_dump(mode="python")
    readiness = {"events": Coverage("2025-01-01", "2025-01-05")}
    state: BinnedMetricState | StateObservations = BinnedMetricState(
        "total", cutpoints=(20,), levels=("low", "high")
    )
    if state_kind == "observed":
        con.raw_sql(
            "CREATE TABLE statuses AS SELECT user_id, ts::TIMESTAMPTZ AS ts, state "
            "FROM (VALUES "
            "('a', '2025-01-02 00:00:00+00', 'active'), "
            "('a', '2025-01-03 00:00:00+00', 'active'), "
            "('a', '2025-01-04 00:00:00+00', 'closed')) AS points(user_id, ts, state)"
        )
        definition["sources"]["statuses"] = {
            "relation": {"kind": "table", "name": "statuses"},
            "time": "ts",
            "units": {"user": "user_id"},
            "events": [],
            "properties": ["state"],
            "dimensions": [],
        }
        definition["properties"]["state"] = {
            "source": "statuses",
            "column": "state",
            "dtype": "string",
            "as_of": "event_time",
        }
        readiness["statuses"] = Coverage("2025-01-01", "2025-01-05")
        state = StateObservations("state", ("active", "closed"), absorbing=("closed",))
    model = Model.from_catalog(
        Catalog.model_validate(definition),
        con=con,
        units=column_model.units,
        readiness=readiness,
    )
    design = Design(
        "user",
        Schedule(every, "2025-01-02", "2025-01-03"),
        features={"total": agg("total", window=("-1d", "0s"))},
        shape=StatePanel(
            state,
            periods=len(totals),
            baseline_features={"baseline_total": agg("total", window=("-1d", "0s"))},
        ),
    )
    ordinary = compile_design(design, model)
    expected = con.to_pyarrow(ordinary.table.filter(ordinary.table.unit == "a").order_by("t0"))
    assert expected["total"].to_pylist() == [Decimal(value) for value in totals]
    assert expected["baseline_total"].to_pylist() == [Decimal(5)] * len(totals)
    persisted = {
        replace(key, snapshot_id="frozen"): con.create_table(
            f"frozen_state_layer_{index}", obj=build_layer(design, model, key)
        )
        for index, key in enumerate(plan_layers(design, model).keys)
    }
    anchor_key = next(key for key in persisted if key.layer == "anchor")
    anchors = persisted[anchor_key]
    persisted[anchor_key] = anchors.filter(anchors.unit == "a")
    con.raw_sql("CREATE TABLE frozen_events AS SELECT * FROM events")
    frozen_model = Model._bind(
        model.catalog,
        con=con,
        readiness=model.readiness,
        units=model.units,
        event_ids=model.event_ids,
        input_binding=model.input_binding,
        source_names=set(model.sources),
        engine=model._engine,
        resolver=lambda relation: (
            con.table("frozen_events")
            if getattr(relation, "name", None) == "events"
            else model._relation(relation)
        ),
    )
    con.raw_sql("UPDATE events SET amount=amount+100")

    cached = compose_materialized_frame(design, frozen_model, persisted=persisted)
    actual = con.to_pyarrow(cached.table.order_by("t0"))
    assert actual.equals(expected)
    assert cached._anchors.table is persisted[anchor_key]
    assert cached.roles == ordinary.roles
    assert cached.columns == ordinary.columns
    assert cached.expression_timings == ordinary.expression_timings
    fresh = compile_design(design, model)
    assert con.to_pyarrow(fresh.table.filter(fresh.table.unit == "a").order_by("t0"))[
        "total"
    ].to_pylist() == [Decimal(value) for value in fresh_totals]
