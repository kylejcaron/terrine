from __future__ import annotations

from datetime import UTC, datetime

import ibis
import pytest
import yaml

import terrine as tr
from terrine import recipes
from terrine.model import Model
from terrine.spec import (
    CohortSpec,
    Design,
    EventAnchor,
    FixedDate,
    Treatment,
    agg,
    asof,
    event_time,
)
from terrine.spec.trajectory import Trajectory
from tests.semantic.test_from_tables import WINDOW, create_tables, rich_model


def _rebound(model: Model) -> Model:
    return Model.from_catalog(
        yaml.safe_load(model.to_catalog()),
        con=model.con,
        units=model.units,
        readiness=model.readiness,
        event_ids=model.event_ids,
    )


def _table(model, design):
    frame = design.build(model)
    return model.con.to_pyarrow(frame.expr.order_by("unit"))


def test_to_catalog_round_trip_builds_equal_dataset():
    con = ibis.duckdb.connect()
    try:
        create_tables(con)
        model = rich_model(con)
        rebound = _rebound(model)
        design = Design(
            "customer",
            FixedDate("2025-02-01"),
            features={
                "refunds": agg("refunds", window=WINDOW),
                "large_paid": agg("large_paid", window=WINDOW),
                "refund_rate": agg("refund_rate", window=WINDOW),
                "first_refund": event_time(
                    "orders", window=WINDOW, label_property="status", equals="refunded"
                ),
                "plan_at_t0": asof("plan", before="0s"),
                "plan_change_at_t0": asof("plan_change", before="0s"),
            },
            treatment=Treatment("plan_change"),
        )
        table = _table(model, design)
        assert table.equals(_table(rebound, design))
        rows = {row["unit"]: row for row in table.to_pylist()}
        assert (rows["alice"]["refunds"], rows["bob"]["refunds"]) == (1, 1)
        assert (rows["alice"]["large_paid"], rows["bob"]["large_paid"]) == (1, 1)
        assert rows["alice"]["refund_rate"] == rows["bob"]["refund_rate"] == 1 / 3
        assert (rows["alice"]["plan_at_t0"], rows["alice"]["plan_change_at_t0"]) == (
            "basic",
            "basic",
        )
        assert rows["alice"]["treatment"] == "pro"
        assert rows["bob"]["plan_at_t0__reason"] == "missing_history"
        assert rows["bob"]["plan_at_t0__complete"] is False

        early = Design(
            "customer",
            FixedDate("2023-12-15"),
            features={
                "plan_at_t0": asof("plan", before="0s"),
                "plan_change_at_t0": asof("plan_change", before="0s"),
            },
        )
        early_table = _table(model, early)
        assert early_table.equals(_table(rebound, early))
        assert set(early_table.column("plan_at_t0__reason").to_pylist()) == {"incomplete_coverage"}
        assert set(early_table.column("plan_change_at_t0__reason").to_pylist()) == {
            "incomplete_coverage"
        }
    finally:
        con.disconnect()


def test_from_tables_ltv_cumulative_age_zero_includes_origin_event():
    con = ibis.duckdb.connect()
    try:
        create_tables(con)
        model = rich_model(con)
        low_level = Design(
            "customer",
            EventAnchor("orders", occurrence="first"),
            shape=CohortSpec("orders", Trajectory("spend", "7d", periods=2, cumulative=True)),
        )
        design = recipes.ltv(
            "customer", start_event="orders", metric="spend", period="7d", periods=2
        )
        assert design == low_level
        assert design.spec_hash == low_level.spec_hash
        rows = model.con.to_pyarrow(design.build(model).expr.order_by("unit", "age")).to_pylist()
        rebound = _rebound(model)
        assert (
            rows
            == model.con.to_pyarrow(
                low_level.build(rebound).expr.order_by("unit", "age")
            ).to_pylist()
        )
        by_unit_age = {(row["unit"], row["age"]): row["cumulative_value"] for row in rows}
        # alice: Jan 5 (30) origin; Jan 10 (60) in age 0; Jan 15 (15) in age 1.
        # bob: Jan 6 (80) origin; Jan 12 (45) in age 0; Jan 20 (10) beyond age 1.
        assert by_unit_age == {
            ("alice", 0): 90.0,
            ("alice", 1): 105.0,
            ("bob", 0): 125.0,
            ("bob", 1): 125.0,
        }
    finally:
        con.disconnect()


@pytest.fixture
def mixed_events():
    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE customers AS SELECT * FROM (VALUES ('a'), ('b')) c(id);
        CREATE TABLE events AS
        SELECT id, ts::TIMESTAMPTZ AS ts, kind, status, amount::BIGINT AS amount, tier
        FROM (VALUES
            ('a', '2025-01-01 00:00:00+00', 'signup', NULL, 0, 'excluded'),
            ('a', '2025-01-03 00:00:00+00', 'purchase', 'paid', 10, 'basic'),
            ('a', '2025-01-04 00:00:00+00', 'purchase', 'paid', 20, 'pro'),
            ('a', '2025-01-05 00:00:00+00', 'signup', NULL, 1000, 'excluded'),
            ('b', '2025-01-02 00:00:00+00', 'signup', NULL, 0, 'excluded'),
            ('b', '2025-01-05 00:00:00+00', 'purchase', 'paid', 30, 'basic')
        ) e(id, ts, kind, status, amount, tier);
        CREATE TABLE signups AS SELECT * FROM events WHERE kind = 'signup';
        CREATE TABLE purchases AS SELECT * FROM events WHERE kind = 'purchase';
    """)

    def bind(*, separated=False, kind="purchase"):
        return tr.Model.from_tables(
            con,
            units={"customer": tr.UnitSource("customers", "id")},
            events={
                "signup": tr.Events(
                    "signups" if separated else "events",
                    time="ts",
                    key="id",
                    complete=("2025-01-01", "2025-01-10"),
                    where=None if separated else {"kind": "signup"},
                ),
                "purchase": tr.Events(
                    "purchases" if separated else "events",
                    time="ts",
                    key="id",
                    complete=("2025-01-01", "2025-01-10"),
                    labels=("tier",),
                    metrics={"n": tr.count(), "spend": tr.sum("amount")},
                    where=None if separated else {"kind": kind, "status": "paid"},
                ),
            },
        )

    try:
        yield con, bind
    finally:
        con.disconnect()


def test_filtered_events_split_one_table_into_event_kinds(mixed_events):
    con, bind = mixed_events
    events = con.to_pyarrow(con.table("events"))
    model = tr.Model.from_tables(
        units={"customer": tr.UnitSource(con.to_pyarrow(con.table("customers")), "id")},
        events={
            kind: tr.Events(
                events,
                time="ts",
                key="id",
                where={"kind": kind},
                complete=("2025-01-01", "2025-01-10"),
            )
            for kind in ("signup", "purchase")
        },
    )
    try:
        design = recipes.survival(start_event="signup", end_event="purchase")
        filtered = design.build(model).materialize().table.sort_by("unit")
        separated = design.build(bind(separated=True)).materialize().table.sort_by("unit")
        assert filtered.equals(separated)
        assert filtered["duration"].cast("int64").to_pylist() == [
            2 * 86400_000000,
            3 * 86400_000000,
        ]
        assert filtered["event"].to_pylist() == [True, True]
    finally:
        model.con.disconnect()


def test_filtered_events_apply_to_metrics_selectors_anchors_and_label_asof(mixed_events):
    _, bind = mixed_events
    design = Design(
        "customer",
        tr.EventAnchor("purchase", occurrence="first"),
        outcomes={
            "n": agg("n", window=("0s", "5d")),
            "spend": agg("spend", window=("0s", "5d")),
            "last": event_time("purchase", window=("0s", "5d"), occurrence="last"),
        },
    )
    filtered = design.build(bind()).materialize().table.sort_by("unit")
    separated = design.build(bind(separated=True)).materialize().table.sort_by("unit")
    assert filtered.equals(separated)
    rows = filtered.to_pylist()
    assert [(r["t0"].day, r["n"], r["spend"]) for r in rows] == [(3, 2, 30), (5, 1, 30)]
    assert [r["last"] for r in rows] == [
        datetime(2025, 1, 4, tzinfo=UTC),
        datetime(2025, 1, 5, tzinfo=UTC),
    ]
    label_design = Design("customer", FixedDate("2025-01-06"), features={"tier": asof("tier")})
    filtered_labels = label_design.build(bind()).materialize().table.sort_by("unit")
    separated_labels = label_design.build(bind(separated=True)).materialize().table.sort_by("unit")
    assert filtered_labels.equals(separated_labels)
    assert filtered_labels["tier"].to_pylist() == ["pro", "basic"]


def test_undeclared_filter_column_reaches_daily_and_prefix_metrics(mixed_events):
    _, bind = mixed_events
    design = recipes.time_series(
        every="1d",
        start="2025-01-04",
        end="2025-01-06",
        features={
            "daily": agg("spend", window=("-1d", "0s")),
            "prefix": agg("spend", window=("-3d", "0s")),
            "count": agg("n", window=("-3d", "0s")),
        },
    )
    order = [("unit", "ascending"), ("t0", "ascending")]
    filtered = design.build(bind()).materialize().table.sort_by(order)
    separated = design.build(bind(separated=True)).materialize().table.sort_by(order)
    assert filtered.equals(separated)
    assert filtered["daily"].to_pylist() == [10, 20, 0, 0, 0, 30]
    assert filtered["prefix"].to_pylist() == [10, 30, 30, 0, 0, 30]


def test_changing_a_source_filter_changes_feature_identity(mixed_events):
    from terrine.dataset import TimeSplit
    from terrine.errors import TerrineError
    from terrine.frame import _feature_definition_hash

    _, bind = mixed_events
    original, changed = bind(), bind(kind="signup")
    design = recipes.time_series(
        every="1d",
        start="2025-01-04",
        end="2025-01-06",
        features={
            "spend": agg("spend", window=("-1d", "0s")),
            "tier": asof("tier", before="0s"),
            "last": event_time("purchase", window=("-3d", "0s")),
        },
        outcomes={"label": agg("n", window=("0s", "1d"))},
    )
    for feature in ("spend", "tier", "last"):
        assert _feature_definition_hash(design, (feature,), original) != (
            _feature_definition_hash(design, (feature,), changed)
        )
    frame = design.build(original).training_set(
        label="label",
        features=("spend",),
        split=TimeSplit(train_until="2025-01-05", validation_until="2025-01-06"),
    )
    fitted = frame.fit_winsorization("spend", lower="0", upper="1")
    with pytest.raises(TerrineError) as error:
        design.build(changed).winsorize(fitted)
    assert error.value.code == "terrine.transform.winsorization_identity_mismatch"
