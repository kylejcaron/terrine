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
    receipt = backend.publish_generation(
        StagedGeneration(
            "orders",
            "g1",
            {"prefix-partition": pa.table({"unit": ["a"], "sum": [7]})},
        ),
        expected_manifest=None,
        lease=_lease(con),
    )
    key = replace(plan_layers(design, model).keys[0], snapshot_id="snapshot")
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
