# ruff: noqa: F811
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb
import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest
import sqlglot.expressions as sge

from terrine.compile.layers import plan_layers
from terrine.runtime.backends.duckdb import DuckDBBackend, StagedGeneration
from terrine.runtime.layers import translate_persisted_layers
from terrine.runtime.leases import LeaseStore
from terrine.runtime.state import DefinitionVersion, GenerationRef, RuntimeStore, SourceSnapshot
from terrine.spec import Design, FixedDate, agg
from tests.compile.test_columns import column_model  # noqa: F401,F811

_BIGNUM = dt.Unknown(raw_type=sge.DataType.build("BIGNUM", dialect="duckdb"))


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


def _text_layer(relation, execute):
    """A layer as earlier runtimes wrote it: every exact column as text, all named."""
    columns = tuple(name for name, dtype in relation.schema().items() if dtype.is_unknown())
    arrow = execute(relation.mutate(**{name: relation[name].cast("string") for name in columns}))
    return arrow.replace_schema_metadata(
        {b"terrine.runtime.exact_layer_columns": json.dumps(columns).encode()}
    )


@pytest.mark.parametrize("persist", ["current", "text"])
def test_exact_prefix_layers_roundtrip_through_arrow_storage(column_model, persist):
    from types import SimpleNamespace

    from terrine.compile.columns import compile_columns
    from terrine.compile.layers import build_layer
    from terrine.compile.materialize import compose_materialized_frame
    from terrine.runtime.api import _persistable_layer, _restore_persisted_layer

    design = _design()
    writer = _persistable_layer if persist == "current" else _text_layer
    keys = plan_layers(design, column_model).keys
    layers = {
        key: writer(build_layer(design, column_model, key), column_model.con.to_pyarrow)
        for key in keys
    }
    prefix = next(key for key in keys if key.layer == "prefix")
    arrow = layers[prefix]
    exact_columns = set(json.loads(arrow.schema.metadata[b"terrine.runtime.exact_layer_columns"]))
    assert "sum_value" in exact_columns
    assert arrow.schema.field("sum_value").type == pa.string()
    if persist == "current":
        # Counts travel as DuckDB's Arrow type for HUGEINT, not as text.
        assert "event_count" not in exact_columns
        assert arrow.schema.field("event_count").type == pa.decimal128(38, 0)
    restored = {
        replace(key, snapshot_id="snapshot"): _restore_persisted_layer(
            SimpleNamespace(con=column_model.con), layer, f"{persist}_{index}"
        )
        for index, (key, layer) in enumerate(layers.items())
    }
    schema = restored[replace(prefix, snapshot_id="snapshot")].schema()
    assert schema["sum_value"].is_unknown()
    assert schema["event_count"] == (dt.Decimal(38, 0) if persist == "current" else _BIGNUM)
    actual = compose_materialized_frame(design, column_model, persisted=restored)
    expected = compile_columns(design, column_model)
    assert column_model.con.to_pyarrow(actual.table.order_by("unit")).equals(
        column_model.con.to_pyarrow(expected.table.order_by("unit"))
    )


def test_build_anchor_layer_accepts_snapshot_relation_resolver(column_model):
    from terrine.compile.layers import build_layer

    design = _design()
    key = plan_layers(design, column_model).keys[0]

    anchor = build_layer(
        design,
        column_model,
        replace(key, snapshot_id="snapshot"),
        relation_resolver=column_model._relation,
    )

    rows = anchor.execute().to_dict("records")
    assert {row["unit"] for row in rows} == {"a", "b"}
    assert all(row["anchor_id"] == "fixed" for row in rows)


def test_snapshot_resolver_rebinding_keeps_filtered_catalog_sources(column_model):
    from copy import deepcopy

    from terrine.compile.layers import build_layer
    from terrine.model import Model
    from terrine.semantic.models import Catalog

    catalog_data = column_model.catalog.model_dump(mode="json")
    unused = deepcopy(catalog_data["sources"]["events"])
    unused["relation"]["name"] = "unused_missing_source"
    catalog_data["sources"]["unused"] = unused
    catalog = Catalog.model_validate(catalog_data)
    filtered = Model._bind(
        catalog,
        con=column_model.con,
        readiness=column_model.readiness,
        units=column_model.units,
        event_ids=column_model.event_ids,
        resolver=column_model._resolver,
        input_binding=column_model.input_binding,
        source_names=set(column_model.sources),
        engine=column_model._engine,
    )
    design = _design()
    key = plan_layers(design, filtered).keys[0]

    anchor = build_layer(
        design,
        filtered,
        replace(key, snapshot_id="snapshot"),
        relation_resolver=filtered._relation,
    )

    assert len(anchor.execute()) == 2


def test_runtime_shape_preflight_refuses_unsupported_shape_without_a_model():
    from types import SimpleNamespace
    from typing import cast

    from terrine.compile.materialize import preflight_materialization_shape
    from terrine.errors import TerrineError
    from terrine.spec.design import Design

    with pytest.raises(TerrineError) as error:
        preflight_materialization_shape(
            cast(Design, SimpleNamespace(shape="time_to_event", anchor=None))
        )
    assert error.value.code == "terrine.compile.runtime_shape_unsupported"


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


def test_layer_identity_ignores_source_nullability_lost_in_snapshot(column_model):
    from terrine.model import Model

    con = column_model.con
    con.raw_sql(
        "CREATE TABLE events_not_null (user_id VARCHAR NOT NULL, ts TIMESTAMPTZ NOT NULL, "
        "amount BIGINT NOT NULL, tier VARCHAR)"
    )
    con.raw_sql("INSERT INTO events_not_null SELECT * FROM events WHERE amount IS NOT NULL")
    from_not_null = Model._bind(
        column_model.catalog,
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
        event_ids=column_model.event_ids,
        resolver=lambda relation: (
            con.table("events_not_null")
            if getattr(relation, "name", None) == "events"
            else column_model._resolver(relation)
        ),
        input_binding=column_model.input_binding,
        source_names={"events"},
        engine=column_model._engine,
    )

    assert plan_layers(_design(), from_not_null).keys == plan_layers(_design(), column_model).keys
