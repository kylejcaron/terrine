# ruff: noqa: F811
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
from tests.compile.test_columns import column_model  # noqa: F401,F811


def _lease(con):
    store = LeaseStore(con, namespace="owner")
    store.initialize()
    return store.acquire("dataset:orders", "run", ttl=timedelta(minutes=1), now=datetime.now(UTC))


def _published_generation(model, design, *, con=None, prefix_value=7, prefix_table=None):
    con = duckdb.connect(":memory:") if con is None else con
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
                "prefix-partition": (
                    pa.table({"unit": ["a"], "sum": [prefix_value]})
                    if prefix_table is None
                    else prefix_table
                ),
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


def test_build_layer_composition_matches_direct_compilation(column_model):
    from terrine.compile.columns import compile_columns
    from terrine.compile.layers import build_layer
    from terrine.compile.materialize import compose_materialized_frame

    design = _design()
    persisted = {
        replace(key, snapshot_id="snapshot"): build_layer(design, column_model, key)
        for key in plan_layers(design, column_model).keys
    }
    actual = compose_materialized_frame(design, column_model, persisted=persisted)
    expected = compile_columns(design, column_model)
    assert column_model.con.to_pyarrow(actual.table.order_by("unit")).equals(
        column_model.con.to_pyarrow(expected.table.order_by("unit"))
    )
    assert actual.roles == expected.roles
    assert actual.columns == expected.columns
    assert actual.expression_timings == expected.expression_timings


def test_build_layer_composition_matches_state_panel_compilation(column_model):
    from terrine.compile.dispatch import compile_design
    from terrine.compile.layers import build_layer
    from terrine.compile.materialize import compose_materialized_frame
    from terrine.spec import BinnedMetricState, Schedule, StatePanel

    design = Design(
        "user",
        Schedule("1d", "2025-01-02", "2025-01-03"),
        shape=StatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("none", "some")), max_periods=2
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


def test_runtime_layer_resolution_preserves_owner_timezone(column_model):
    design = _design()
    con, store, backend, ref, receipt, key = _published_generation(column_model, design)
    con.execute("SET TimeZone = 'America/New_York'")
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
    assert con.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "America/New_York"


def test_runtime_layer_resolution_reuses_model_backend_for_shared_connection(column_model):
    design = _design()
    con, store, backend, ref, receipt, key = _published_generation(
        column_model, design, con=column_model.con.con
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
    assert actual[key]._find_backend(use_default=False) is column_model.con
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
