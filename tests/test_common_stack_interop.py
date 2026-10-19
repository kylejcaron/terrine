from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from terrine.data import Budget, MaterializedFrame
from terrine.errors import TerrineError, thaw
from tests.fixtures.interop import interop_frame


@pytest.fixture(params=[False, True], ids=["typed_rows", "empty"])
def common_frame(request, tmp_path):
    con = ibis.duckdb.connect(str(tmp_path / "dataset.duckdb"))
    try:
        yield interop_frame(con, empty=request.param, full=False)
    finally:
        con.disconnect()


def test_local_arrow_preserves_exact_temporal_values_and_semantic_metadata(common_frame):
    materialized = common_frame.materialize(Budget(max_rows=3))
    arrow = materialized.to_arrow().sort_by("unit")
    assert arrow.schema.field("amount").type == pa.decimal128(38, 2)
    assert arrow.schema.field("t0").type == pa.timestamp("us", tz="UTC")
    restored = MaterializedFrame.from_arrow(arrow)
    assert restored.roles == materialized.roles
    assert restored.columns == materialized.columns
    assert restored.provenance == materialized.provenance
    if arrow.num_rows:
        rows = arrow.to_pylist()
        assert [row["amount"] for row in rows] == [
            Decimal("999999999999999999999999999999999999.98"),
            None,
            None,
        ]
        assert [row["t0"] for row in rows] == [datetime(2025, 1, 2, 12, tzinfo=UTC)] * 3
        assert arrow["note"].null_count == 3
        amount_meta = restored.columns["amount"]
        assert rows[1][amount_meta.reason_column] == "all_values_null"
    else:
        assert arrow.num_rows == 0
        assert arrow.schema.field("note").type == pa.string()
    decorated = replace(
        materialized,
        table=materialized.table.replace_schema_metadata(
            {b"consumer": b"kept", b"terrine.materialized_frame.v1": b"stale"}
        ),
    )
    transported = decorated.to_arrow()
    assert transported.schema.metadata[b"consumer"] == b"kept"
    assert MaterializedFrame.from_arrow(transported).to_metadata() == decorated.to_metadata()


@pytest.mark.parametrize("extra", ["pandas", "polars"])
def test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow(common_frame, extra):
    module = pytest.importorskip(extra)
    expected = common_frame.to_pyarrow(budget=Budget(max_rows=3)).sort_by("unit")
    converted = getattr(common_frame, f"to_{extra}")(budget=Budget(max_rows=3))
    if extra == "pandas":
        assert all(isinstance(dtype, module.ArrowDtype) for dtype in converted.dtypes)
        actual = pa.Table.from_pandas(converted, preserve_index=False)
    else:
        actual = converted.to_arrow()
    actual = actual.sort_by("unit")
    assert actual.schema.field("amount").type == expected.schema.field("amount").type
    assert actual.schema.field("t0").type == expected.schema.field("t0").type
    assert actual.to_pylist() == expected.to_pylist()
    assert actual["note"].null_count == expected["note"].null_count


def test_dataset_batches_and_parquet_preserve_typed_metadata(common_frame, tmp_path):
    budget = Budget(max_rows=3, max_output_bytes=1_000_000)
    expected = common_frame.to_pyarrow(budget=budget).sort_by("unit")
    expected_metadata = MaterializedFrame.from_arrow(expected)
    dataset = common_frame.stage("common_stack", budget=budget)
    batches = list(dataset.to_arrow_batches(batch_size=1))
    assert all(batch.num_rows <= 1 for batch in batches)
    if batches:
        streamed = pa.Table.from_batches(batches).sort_by("unit")
        assert streamed.to_pylist() == expected.to_pylist()
        assert MaterializedFrame.from_arrow(streamed).columns == expected_metadata.columns
    destination = tmp_path / "parquet"
    exported = dataset.export_parquet(destination=str(destination), budget=budget)
    assert exported.generation_id == dataset.manifest.generation.generation_id
    assert exported.schema == tuple((field.name, str(field.type)) for field in expected.schema)
    readback = pa.concat_tables(
        [pq.ParquetFile(destination / shard.path).read() for shard in exported.shards]
    ).sort_by("unit")
    assert readback.schema.equals(expected.schema, check_metadata=False)
    assert readback.to_pylist() == expected.to_pylist()
    restored = MaterializedFrame.from_arrow(readback)
    assert restored.roles == expected_metadata.roles
    assert restored.columns == expected_metadata.columns
    assert restored.provenance.design_hash == expected_metadata.provenance.design_hash
    assert restored.provenance.model_hash == expected_metadata.provenance.model_hash
    assert thaw(dataset.manifest.frame_metadata) == restored.to_metadata()


@pytest.mark.parametrize("extra", ["pandas", "polars"])
def test_missing_dataframe_extra_refuses_before_execution(native_inputs, monkeypatch, extra):
    from terrine.data import Coverage
    from terrine.model import Model
    from terrine.spec import Design, EventAnchor

    catalog, con = native_inputs
    frame = Design("user", EventAnchor("session")).build(
        Model.from_catalog(
            catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-01-31")}
        )
    )
    con.con.execute("DROP TABLE events")
    original = __import__("terrine.frame", fromlist=["import_module"]).import_module

    def unavailable(name):
        if name == extra:
            raise ImportError("optional dataframe unavailable")
        return original(name)

    monkeypatch.setattr("terrine.frame.import_module", unavailable)
    with pytest.raises(TerrineError) as error:
        getattr(frame, f"to_{extra}")()
    assert error.value.code == "terrine.convert.missing_dependency"
    assert error.value.context["extra"] == extra
