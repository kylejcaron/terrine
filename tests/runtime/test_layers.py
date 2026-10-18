from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from terrine.compile.layers import plan_layers
from terrine.runtime.backends.duckdb import DuckDBBackend, StagedGeneration
from terrine.runtime.layers import translate_persisted_layers
from terrine.runtime.leases import LeaseStore
from terrine.runtime.state import DefinitionVersion, GenerationRef, RuntimeStore, SourceSnapshot
from terrine.spec import Design, FixedDate, agg

pytest_plugins = ("tests.compile.test_columns",)


def _lease(con):
    store = LeaseStore(con, namespace="owner")
    store.initialize()
    return store.acquire("dataset:orders", "run", ttl=timedelta(minutes=1), now=datetime.now(UTC))


def _published_generation(model, design):
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    ref = GenerationRef("owner", "orders", "g1")
    store.write_generation(
        ref,
        definition=DefinitionVersion(design.spec_hash, "schema", "v1"),
        snapshot=SourceSnapshot("snapshot", {}, {}, datetime.now(UTC), "owner"),
    )
    anchor_partition = pa.table(
        {
            "unit": pa.array(["a"], type=pa.string()),
            "anchor_id": pa.array(["fixed"], type=pa.string()),
            "t0": pa.array([datetime(2025, 1, 2, tzinfo=UTC)], type=pa.timestamp("us", tz="UTC")),
            "_observed_from": pa.array([None], type=pa.timestamp("us", tz="UTC")),
            "_observed_until": pa.array([None], type=pa.timestamp("us", tz="UTC")),
            "anchor_provenance": pa.array(["fixed_date"], type=pa.string()),
        }
    )
    receipt = backend.publish_generation(
        StagedGeneration(
            "orders",
            "g1",
            {
                "prefix-partition": pa.table({"unit": ["a"], "sum": [7]}),
                "anchor-partition": anchor_partition,
            },
        ),
        expected_manifest=None,
        lease=_lease(con),
    )
    key = replace(
        next(key for key in plan_layers(design, model).keys if key.layer == "prefix"),
        snapshot_id="snapshot",
    )
    return con, store, backend, ref, receipt, key


def _design():
    return Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )


def test_runtime_translates_verified_physical_partition_to_compiler_layer_key(column_model):
    design = _design()
    con, store, backend, ref, receipt, key = _published_generation(column_model, design)
    actual = translate_persisted_layers(
        store,
        backend,
        design=design,
        model=column_model,
        ref=ref,
        receipt=receipt,
        partitions={key: "prefix-partition"},
    )
    assert actual[key].execute().to_dict("records") == [{"unit": "a", "sum": 7}]


def test_runtime_translates_persisted_anchor_record(column_model):
    design = _design()
    _, store, backend, ref, receipt, _prefix_key = _published_generation(column_model, design)
    key = replace(
        next(key for key in plan_layers(design, column_model).keys if key.layer == "anchor"),
        snapshot_id="snapshot",
    )
    actual = translate_persisted_layers(
        store,
        backend,
        design=design,
        model=column_model,
        ref=ref,
        receipt=receipt,
        partitions={key: "anchor-partition"},
    )
    assert actual[key].select("unit", "anchor_id", "anchor_provenance").execute().to_dict(
        "records"
    ) == [{"unit": "a", "anchor_id": "fixed", "anchor_provenance": "fixed_date"}]


def test_runtime_partition_does_not_resolve_same_name_from_model_database(column_model):
    design = _design()
    con, store, backend, ref, receipt, key = _published_generation(column_model, design)
    table_name = con.execute(
        "SELECT table_name FROM owner.runtime_partitions WHERE dataset=? AND generation=? "
        "AND partition=?",
        [receipt.dataset, receipt.generation, "prefix-partition"],
    ).fetchone()[0]
    column_model.con.raw_sql("CREATE SCHEMA owner")
    column_model.con.raw_sql(
        f'CREATE TABLE owner."{table_name}" AS SELECT '
        "'wrong'::VARCHAR AS unit, 999::INTEGER AS sum"
    )
    actual = translate_persisted_layers(
        store,
        backend,
        design=design,
        model=column_model,
        ref=ref,
        receipt=receipt,
        partitions={key: "prefix-partition"},
    )
    assert actual[key].execute().to_dict("records") == [{"unit": "a", "sum": 7}]


@pytest.mark.parametrize(
    "field,value",
    [("definition_hash", "wrong"), ("snapshot_id", "wrong"), ("unit", "other")],
)
def test_runtime_refuses_physical_partition_with_mismatched_logical_identity(
    column_model, field, value
):
    design = _design()
    _, store, backend, ref, receipt, key = _published_generation(column_model, design)
    with pytest.raises(ValueError):
        translate_persisted_layers(
            store,
            backend,
            design=design,
            model=column_model,
            ref=ref,
            receipt=receipt,
            partitions={replace(key, **{field: value}): "prefix-partition"},
        )
