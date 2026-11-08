from __future__ import annotations

import ibis
import pytest

import terrine as tr
from terrine.backends.engines import duckdb as duckdb_engine
from terrine.errors import TerrineError
from terrine.execution import _native
from terrine.runtime.api import find_run_by_request_key, get_run, run_serialized_plan
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planning import OwnerPlanner, PlanRequest
from terrine.runtime.saved_plan import DestinationRef, ModelReference
from terrine.runtime.snapshot import SnapshotAuthority
from tests.fixtures.readme import readme_model, readme_tables


@pytest.fixture
def readme_backend():
    con = ibis.duckdb.connect()
    try:
        for name, table in readme_tables().items():
            con.create_table(name, table)
        yield con, readme_model(con)
    finally:
        con.disconnect()


def test_count_only_design_runs_on_duckdb_above_minimum(monkeypatch, readme_backend):
    _, model = readme_backend
    witnesses = []
    monkeypatch.setattr(duckdb_engine, "version", lambda _: "1.6.0")
    monkeypatch.setattr(_native, "verify_exact_arithmetic", lambda raw: witnesses.append(raw))
    design = tr.recipes.time_series(
        every="1mo",
        start="2025-02-01",
        end="2025-03-01",
        features={"n": tr.agg("orders", window=("-30d", "0s"))},
    )

    frame = design.build(model)

    assert len(frame.to_pandas()) == 6
    assert witnesses == []


def test_exact_measure_runs_arithmetic_witness(readme_backend, monkeypatch):
    _, model = readme_backend
    witnesses = []
    verify = _native.verify_exact_arithmetic

    def record_and_verify(con):
        witnesses.append(con)
        verify(con)

    con, _ = readme_backend
    model = readme_model(con, exact=True)
    monkeypatch.setattr(_native, "verify_exact_arithmetic", record_and_verify)
    design = tr.recipes.time_series(
        every="1mo",
        start="2025-02-01",
        end="2025-03-01",
        features={"spend": tr.agg("spend", window=("-30d", "0s"))},
    )

    design.build(model).to_pandas()

    assert len(witnesses) == 1


@pytest.fixture
def exact_owner(tmp_path):
    path = tmp_path / "readme.duckdb"
    con = ibis.duckdb.connect(str(path))
    try:
        for name, table in readme_tables().items():
            con.create_table(name, table)
        yield path, con, readme_model(con, exact=True)
    finally:
        con.disconnect()


_SPEND = tr.recipes.time_series(
    every="1mo",
    start="2025-02-01",
    end="2025-03-01",
    features={"spend": tr.agg("spend", window=("-30d", "0s"))},
)


def _fail_witness(monkeypatch) -> None:
    monkeypatch.setattr(_native, "_exact_arithmetic_answer", lambda _: ("SELECT 1", (2,)))


def _assert_witness_refusal(error: TerrineError) -> None:
    assert error.code == "terrine.backend.capability_unavailable"
    assert error.context["operation"] == "exact_arithmetic"
    assert error.context["status"] == "witness_failed"


def test_failing_witness_refuses_staging_before_publication(exact_owner, monkeypatch):
    _, con, model = exact_owner
    _fail_witness(monkeypatch)

    with pytest.raises(TerrineError) as error:
        _SPEND.build(model).stage("spend")

    _assert_witness_refusal(error.value)
    assert DuckDBBackend(con.con, namespace="terrine_runtime").current_generation("spend") is None


def test_failing_witness_refuses_a_saved_plan_run_before_publication(
    exact_owner, tmp_path, monkeypatch
):
    path, con, model = exact_owner

    class Resolver:
        def resolve(self, reference):
            return model

    authority = SnapshotAuthority(RuntimeOwner(path, tmp_path / "snapshots"))
    request = PlanRequest(
        "exact-spend",
        ModelReference.from_model(model, owner_key="owner-model"),
        _SPEND,
        DestinationRef(str(path), "published", "spend"),
    )
    invocation = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority).plan(
        request, con=con.con, namespace="owner"
    )
    _fail_witness(monkeypatch)

    with pytest.raises(TerrineError) as error:
        run_serialized_plan(
            invocation.saved_plan_json,
            con=con.con,
            namespace="owner",
            request_key=invocation.request_key,
            resolver=Resolver(),
            snapshot_authority=authority,
        )

    _assert_witness_refusal(error.value)
    run = find_run_by_request_key(
        invocation.request_key, destination=request.destination, con=con.con, namespace="owner"
    )
    assert run is not None and run.state == "failed" and run.published_generation is None
    assert get_run(run.run_id, con=con.con, namespace="owner").error_code == error.value.code
    assert DuckDBBackend(con.con, namespace="owner").current_generation("spend") is None
