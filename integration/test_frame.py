from datetime import datetime

import ibis
import pyarrow as pa
import pytest

import terrine as tr
from terrine.spec import Design, EventAnchor
from tests.fixtures.readme import readme_design, readme_tables


@pytest.fixture
def shuffled_model(tmp_path):
    tables = readme_tables()
    orders = tables["orders"].append_column("event_id", pa.array(["z", "b", "c", "d", "e"]))
    orders = pa.concat_tables(
        [orders, orders.slice(0, 1).set_column(3, "event_id", pa.array(["a"]))]
    )
    con = ibis.duckdb.connect(str(tmp_path / "ordering.duckdb"))
    con.create_table(
        "customers",
        pa.table(
            {
                "customer_id": ["carol", "bob", "alice"],
                "segment": [10, 2, 10],
                "ages": [[], [2], [10, 20]],
            }
        ),
    )
    con.create_table("orders", orders.take([3, 5, 4, 1, 2, 0]))
    con.create_table(
        "churn",
        pa.table(
            {
                "customer_id": ["alice", "bob"],
                "ordered_at": [datetime(2025, 1, 24), datetime(2025, 1, 20)],
            }
        ),
    )
    model = tr.Model.from_tables(
        con,
        units={
            "customer": tr.UnitSource("customers", "customer_id", attributes=("segment", "ages"))
        },
        events={
            "orders": tr.Events(
                "orders",
                time="ordered_at",
                key="customer_id",
                metrics={"orders": tr.count(), "spend": tr.sum("amount")},
                complete=("2025-01-01", "2025-03-20"),
            ),
            "churn": tr.Events(
                "churn",
                time="ordered_at",
                key="customer_id",
                complete=("2025-01-01", "2025-03-20"),
            ),
        },
        event_ids={"orders": "event_id"},
    )
    try:
        yield model
    finally:
        con.disconnect()


def test_rows_are_ordered_by_identity_columns(shuffled_model):
    frame = readme_design().build(shuffled_model)
    data = frame.to_pandas()
    identity = data[["unit", "t0", "anchor_id"]]
    assert identity.equals(identity.sort_values(list(identity.columns)).reset_index(drop=True))
    assert data["unit"].tolist() == ["alice", "alice", "bob", "bob", "carol", "carol"]
    assert data["t0"].dt.tz is not None

    tied = Design("customer", EventAnchor("orders", occurrence="each")).build(shuffled_model)
    keys = tied.to_pandas()[["unit", "t0", "anchor_id"]]
    assert keys.duplicated(["unit", "t0"]).any()
    assert keys.equals(keys.sort_values(list(keys.columns)).reset_index(drop=True))


@pytest.mark.parametrize("reader", ["frame", "dataset"])
def test_shape_named_features_keep_identity_order(shuffled_model, reader):
    frame = Design(
        "customer",
        EventAnchor("orders", occurrence="each"),
        features={
            "time_bin": tr.agg("orders", window=("-30d", "0s")),
            **{
                name: tr.asof("ages")
                for name in ("age", "event_period", "period_index", "trial_id", "sequence_position")
            },
        },
    ).build(shuffled_model)
    result = (
        frame if reader == "frame" else frame.stage("ordering", budget=tr.Budget(strict_scan=False))
    )
    table = result.to_pyarrow()
    identities = [
        (row["unit"], row["t0"], row["anchor_id"])
        for row in table.select(["unit", "t0", "anchor_id"]).to_pylist()
    ]
    assert identities == sorted(identities)
    assert len({(unit, t0) for unit, t0, _ in identities}) < len(identities)
    assert table["unit"].to_pylist() == ["alice"] * 5 + ["bob"]
    for name in ("age", "event_period", "period_index", "trial_id", "sequence_position"):
        assert table[name].to_pylist() == [[10, 20]] * 5 + [[2]]


def test_life_table_rows_are_ordered_by_group_and_bin(shuffled_model):
    frame = tr.recipes.life_table(
        start_event="orders",
        end_event="churn",
        bin="7d",
        group_by={"segment": tr.asof("segment")},
    ).build(shuffled_model)
    data = frame.to_pandas()
    keys = data[["segment", "time_bin"]]
    assert set(data["segment"]) == {2, 10}
    assert data["time_bin"].nunique() > 1
    assert keys.equals(keys.sort_values(list(keys.columns)).reset_index(drop=True))


def test_sequence_rows_are_ordered_by_position(shuffled_model):
    frame = tr.recipes.event_sequence(
        event="orders",
        horizon="30d",
        max_events=10,
    ).build(shuffled_model)
    table = frame.to_pyarrow(budget=tr.Budget(max_rows=100, max_output_bytes=1_000_000))
    rows = table.to_pylist()
    anchors = {}
    for row in rows:
        anchors.setdefault((row["unit"], row["t0"], row["anchor_id"]), []).append(row)
    assert list(anchors) == sorted(anchors)
    assert any(sum(row["is_event"] for row in group) > 1 for group in anchors.values())
    for group in anchors.values():
        positions = [row["sequence_position"] for row in group if row["is_event"]]
        assert positions == sorted(positions)
        assert group[-1]["sequence_position"] is None
        assert group[-1]["is_event"] is False


def test_materialized_frame_dataframes_match_frame_dataframes(shuffled_model):
    frame = readme_design().build(shuffled_model)
    materialized = frame.materialize()

    assert materialized.to_pandas(values_only=True).equals(frame.to_pandas(values_only=True))
    pytest.importorskip("polars")
    materialized_polars = materialized.to_polars(values_only=True)
    frame_polars = frame.to_polars(values_only=True)
    assert materialized_polars.columns == frame_polars.columns
    assert materialized_polars.to_dicts() == frame_polars.to_dicts()
