import builtins
import copy
import pickle
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime
from typing import Any, cast

import ibis
import pyarrow as pa
import pytest
import yaml

import terrine as tr
from terrine.data import Events, History, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import Design, EventAnchor, FixedDate, Treatment, agg, asof, event_time
from tests.fixtures.readme import readme_design, readme_model, readme_tables

UNITS = {"customer": UnitSource("customers", "customer_id")}
COMPLETE = ("2025-01-01", "2025-01-31")
HISTORY_COMPLETE = ("2024-01-01", "2025-03-20")
AXES: dict[str, dict[str, Any]] = {
    "valid_from": {"valid_from": "starts", "valid_to": "ends"},
    "changed_at": {"changed_at": "changed_at"},
}
WINDOW = ("-31d", "0s")


def create_tables(con) -> None:
    """Two customers; six January orders (qty 2 and 10 bound a range); alice's plan history."""
    con.raw_sql(
        "CREATE TABLE customers AS SELECT * FROM (VALUES ('alice'), ('bob')) c(customer_id)"
    )
    con.raw_sql("CREATE TABLE accounts AS SELECT * FROM (VALUES ('acme')) a(account_id)")
    con.raw_sql("""
        CREATE TABLE orders AS SELECT customer_id, ordered_at::TIMESTAMPTZ AS ordered_at,
            status::VARCHAR AS status, amount::DOUBLE AS amount, flag::BOOLEAN AS flag,
            qty::INTEGER AS qty, ordered_at::DATE AS day
        FROM (VALUES
            ('alice', '2025-01-05 00:00:00+00', 'refunded', 30, true, 2),
            ('alice', '2025-01-10 00:00:00+00', 'paid', 60, false, 10),
            ('alice', '2025-01-15 00:00:00+00', 'chargeback', 15, true, 1),
            ('bob', '2025-01-06 00:00:00+00', 'refunded', 80, false, 11),
            ('bob', '2025-01-12 00:00:00+00', 'paid', 45, true, 0),
            ('bob', '2025-01-20 00:00:00+00', 'paid', 10, false, 20)
        ) o(customer_id, ordered_at, status, amount, flag, qty)
    """)
    con.raw_sql("""
        CREATE TABLE plans AS SELECT customer_id, plan::VARCHAR AS plan,
            starts::TIMESTAMPTZ AS starts, ends::TIMESTAMPTZ AS ends,
            starts::TIMESTAMPTZ AS changed_at
        FROM (VALUES
            ('alice', 'basic', '2025-01-01 00:00:00+00', '2025-02-01 00:00:00+00'),
            ('alice', 'pro', '2025-02-01 00:00:00+00', NULL)
        ) p(customer_id, plan, starts, ends)
    """)


def orders(**changes: Any) -> Events:
    arguments: dict[str, Any] = {
        "table": "orders",
        "time": "ordered_at",
        "key": "customer_id",
        "complete": COMPLETE,
        **changes,
    }
    return Events(arguments.pop("table"), **arguments)


def plan(form: str, **changes: Any) -> History:
    arguments: dict[str, Any] = {
        "key": "customer_id",
        "column": "plan",
        "complete": HISTORY_COMPLETE,
    }
    return History("plans", **(arguments | AXES[form] | changes))


def rich_model(con) -> Model:
    """Filtered counts, a ratio, labels and both history forms on one model."""
    return Model.from_tables(
        con,
        units=UNITS,
        events={
            "orders": orders(
                labels=("status", "amount"),
                metrics={
                    "orders": tr.count(),
                    "refunds": tr.count(where={"status": "refunded"}),
                    "large_paid": tr.count(where={"status__ne": "refunded", "amount__gt": 20}),
                    "spend": tr.sum("amount"),
                    "refund_rate": tr.ratio("refunds", "orders"),
                },
            )
        },
        history={"plan": plan("valid_from"), "plan_change": plan("changed_at")},
    )


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        create_tables(connection)
        yield connection
    finally:
        connection.disconnect()


def _snapshot(model, anchor, **columns):
    frame = Design("customer", FixedDate(anchor), **columns).build(model)
    table = frame.to_pyarrow().to_pydict()
    return {name: dict(zip(table["unit"], values, strict=True)) for name, values in table.items()}


def test_events_descriptor_normalizes_and_detaches_inputs():
    metrics = {"n": tr.count()}
    labels = ["status"]
    events = Events(
        "orders",
        time="ordered_at",
        key="customer_id",
        metrics=metrics,
        labels=labels,
        complete=COMPLETE,
    )
    metrics["changed"] = tr.count()
    labels.append("changed")
    assert dict(events.metrics) == {"n": tr.count()}
    assert events.labels == ("status",)
    assert events.unit is None
    assert events.complete == (date(2025, 1, 1), date(2025, 1, 31))
    with pytest.raises(TypeError):
        events.metrics["n"] = tr.count()  # ty: ignore[invalid-assignment]
    with pytest.raises(FrozenInstanceError):
        events.table = "changed"  # ty: ignore[invalid-assignment]


@pytest.mark.parametrize(
    ("changes", "reason", "field"),
    [
        ({"table": ""}, "empty_name", "table"),
        ({"table": pa.table({})}, "empty_table", "table"),
        ({"time": ""}, "empty_name", "time"),
        ({"key": ""}, "empty_name", "key"),
        ({"unit": ""}, "empty_name", "unit"),
        ({"metrics": {"n": "count"}}, "wrong_type", "metrics"),
        ({"metrics": {"": tr.count()}}, "wrong_type", "metrics"),
        ({"labels": "status"}, "wrong_type", "labels"),
        ({"complete": (datetime(2025, 1, 1), date(2025, 1, 31))}, "invalid_date", "complete"),
    ],
)
def test_events_descriptor_rejects_invalid_fields(changes, reason, field):
    with pytest.raises(TerrineError) as error:
        orders(**changes)
    assert error.value.code == "terrine.model.invalid_binding"
    assert error.value.context == {"reason": reason, "field": field}


def test_where_counts_match_hand_oracles(con):
    cases = {
        "equals": ({"status": "refunded"}, 2),
        "in": ({"status": ["refunded", "chargeback"]}, 3),
        "not_equals": ({"status__ne": "refunded"}, 4),
        "gt": ({"amount__gt": 50}, 2),
        "between": ({"amount__between": (10, 50)}, 4),
        "between_bounds": ({"qty__between": [2, 10]}, 2),
        "between_text_bounds": ({"qty__between": ("2", "10")}, 2),
        "conjunction": ({"status": "refunded", "amount__gt": 20}, 2),
    }
    model = Model.from_tables(
        con,
        units=UNITS,
        events={
            "orders": orders(
                labels=("status", "amount", "qty"),
                metrics={name: tr.count(where=where) for name, (where, _) in cases.items()},
            )
        },
    )
    frame = Design(
        "customer",
        FixedDate("2025-02-01"),
        features={name: agg(name, window=WINDOW) for name in cases},
    ).build(model)
    table = frame.to_pyarrow()
    assert {name: builtins.sum(table.column(name).to_pylist()) for name in cases} == {
        name: expected for name, (_, expected) in cases.items()
    }


@pytest.mark.parametrize(
    ("key", "bad"),
    [
        ("status__like", "x"),
        ("amount__between", (1,)),
        ("qty__between", {2, 10}),
        ("qty__between", (10, 2)),
        ("qty__between", (10.5, 2)),
        ("status__in", []),
        ("status__in", "refunded"),
        ("a__b__gt", 1),
    ],
)
def test_filter_grammar_errors_are_coded(key, bad):
    with pytest.raises(TerrineError) as error:
        tr.count(where={key: bad})
    assert error.value.code == "terrine.model.invalid_binding"
    assert error.value.context["reason"] == "invalid_filter"
    assert error.value.context["key"] == key


def test_label_names_may_not_contain_double_underscore():
    with pytest.raises(TerrineError) as error:
        orders(labels=("amount__gt",))
    assert error.value.code == "terrine.model.invalid_binding"
    assert error.value.context == {"reason": "reserved_name", "kind": "label", "name": "amount__gt"}


def test_null_filter_literal_refuses_at_build_not_zero(con):
    model = Model.from_tables(
        con,
        units=UNITS,
        events={
            "orders": orders(labels=("status",), metrics={"n": tr.count(where={"status": None})})
        },
    )
    with pytest.raises(TerrineError) as error:
        _snapshot(model, "2025-02-01", features={"n": agg("n", window=WINDOW)})
    assert error.value.code == "terrine.filter.unknown"


def test_undeclared_label_and_ratio_operand_refuse_missing_reference(con):
    with pytest.raises(TerrineError) as error:
        Model.from_tables(
            con,
            units=UNITS,
            events={
                "orders": orders(
                    labels=("status",), metrics={"n": tr.count(where={"region": "eu"})}
                )
            },
        )
    assert error.value.code == "terrine.semantic.missing_reference"
    assert error.value.context["kind"] == "label"
    assert error.value.context["name"] == "region"
    assert error.value.context["available"] == ("status",)
    with pytest.raises(TerrineError) as error:
        Model.from_tables(
            con,
            units=UNITS,
            events={"orders": orders(metrics={"n": tr.count(), "r": tr.ratio("n", "missing")})},
        )
    assert error.value.code == "terrine.semantic.missing_reference"
    assert error.value.context["kind"] == "metric"
    assert error.value.context["available"] == ("n",)


@pytest.mark.parametrize(
    ("kind", "declarations", "owners"),
    [
        (
            "metric",
            {
                "events": {
                    "orders": {"metrics": {"n": tr.count()}},
                    "sessions": {"metrics": {"n": tr.count()}},
                }
            },
            ("events:orders", "events:sessions"),
        ),
        (
            "label",
            {"events": {"orders": {"labels": ("status",)}, "sessions": {"labels": ("status",)}}},
            ("labels:orders", "labels:sessions"),
        ),
        (
            "event",
            {"events": {"plan": {}}, "history": {"plan": plan("changed_at")}},
            ("events:plan", "history:plan"),
        ),
        (
            "label",
            {"events": {"orders": {"labels": ("sessions",)}, "sessions": {}}},
            ("events:sessions", "labels:orders"),
        ),
        (
            "event",
            {
                "events": {
                    "orders": {"metrics": {"spend": tr.sum("amount")}, "labels": ("orders_amount",)}
                }
            },
            ("events:orders.amount", "labels:orders"),
        ),
    ],
)
def test_name_collisions_refuse_with_owners(kind, declarations, owners, con):
    events = {name: orders(**changes) for name, changes in declarations["events"].items()}
    with pytest.raises(TerrineError) as error:
        Model.from_tables(con, units=UNITS, events=events, history=declarations.get("history"))
    assert error.value.code == "terrine.model.invalid_binding"
    assert error.value.context["reason"] == "name_collision"
    assert error.value.context["kind"] == kind
    assert error.value.context["owners"] == owners
    for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
        assert restored.code == error.value.code
        assert restored.context == error.value.context


def test_label_dtype_inference_and_timestamp_refusal(con):
    labels = ("status", "flag", "qty", "amount", "day")
    model = Model.from_tables(con, units=UNITS, events={"orders": orders(labels=labels)})
    assert {name: model.catalog.properties[name].dtype for name in labels} == {
        "status": "string",
        "flag": "bool",
        "qty": "int",
        "amount": "float",
        "day": "date",
    }
    with pytest.raises(TerrineError) as error:
        Model.from_tables(con, units=UNITS, events={"orders": orders(labels=("ordered_at",))})
    assert error.value.code == "terrine.model.invalid_binding"
    assert error.value.context["reason"] == "unsupported_column_type"
    assert error.value.context["source"] == "orders"
    assert error.value.context["column"] == "ordered_at"


def test_events_without_metrics_declares_only_the_event(con):
    model = Model.from_tables(con, units=UNITS, events={"orders": orders()})
    assert set(model.metrics) == set()
    assert set(model.catalog.events) == {"orders"}
    assert Design("customer", EventAnchor("orders")).validate(model) is None


def test_two_unit_sources_require_unit_on_events(con):
    units = {"customer": UNITS["customer"], "account": UnitSource("accounts", "account_id")}
    with pytest.raises(TerrineError) as error:
        Model.from_tables(con, units=units, events={"orders": orders()})
    assert error.value.code == "terrine.model.ambiguous_unit"
    assert error.value.context == {
        "owner": "events:orders",
        "choices": ("account", "customer"),
        "parameter": "unit",
    }
    with pytest.raises(TerrineError) as error:
        Model.from_tables(con, units=units, events={"orders": orders(unit="shop")})
    assert error.value.code == "terrine.semantic.missing_reference"
    assert error.value.context["kind"] == "unit"
    assert error.value.context["available"] == ("account", "customer")
    model = Model.from_tables(con, units=units, events={"orders": orders(unit="customer")})
    assert model.metrics == {} and model.sources["orders"].units == {"customer": "customer_id"}


@pytest.mark.parametrize("form", ["valid_from", "changed_at"])
def test_history_asof_and_treatment_rules(form, con):
    model = Model.from_tables(
        con, units=UNITS, events={"orders": orders()}, history={"plan": plan(form)}
    )
    at_t0 = _snapshot(
        model,
        "2025-02-01",
        features={"plan_at_t0": asof("plan", before="0s")},
        treatment=Treatment("plan"),
    )
    assert at_t0["plan_at_t0"] == {"alice": "basic", "bob": None}
    assert at_t0["plan_at_t0__reason"]["bob"] == "missing_history"
    assert at_t0["treatment"]["alice"] == "pro"
    early = _snapshot(model, "2023-12-15", features={"plan_at_t0": asof("plan", before="0s")})
    assert set(early["plan_at_t0__reason"].values()) == {"incomplete_coverage"}


def test_history_time_axis_and_completeness_are_coded(con):
    axes: list[dict[str, Any]] = [
        {"valid_from": "starts", "changed_at": "changed_at"},
        {},
        {"changed_at": "c", "valid_to": "e"},
    ]
    for axis in axes:
        with pytest.raises(TerrineError) as error:
            History("plans", key="customer_id", column="plan", **axis)
        assert error.value.code == "terrine.model.invalid_binding"
        assert error.value.context["reason"] == "history_time_axis"
    with pytest.raises(TerrineError) as error:
        Model.from_tables(
            con,
            units=UNITS,
            events={"orders": orders()},
            history={"plan": plan("changed_at", complete=None)},
        )
    assert error.value.code == "terrine.model.missing_event_completeness"
    assert error.value.context["source"] == "plan"


def test_label_asof_carries_the_last_event_value(con):
    model = Model.from_tables(con, units=UNITS, events={"orders": orders(labels=("status",))})
    rows = _snapshot(model, "2025-01-11", features={"status_at_t0": asof("status", before="0s")})
    assert rows["status_at_t0"] == {"alice": "paid", "bob": "refunded"}


def test_event_time_with_label_equals_works_on_from_tables_model(con):
    model = Model.from_tables(con, units=UNITS, events={"orders": orders(labels=("status",))})
    selector = event_time("orders", window=WINDOW, label_property="status", equals="refunded")
    rows = _snapshot(model, "2025-02-01", features={"first_refund": selector})
    assert rows["first_refund"] == {
        "alice": datetime(2025, 1, 5, tzinfo=UTC),
        "bob": datetime(2025, 1, 6, tzinfo=UTC),
    }


def test_to_catalog_round_trip_has_equal_model_hash(con):
    model = rich_model(con)
    restored = Model.from_catalog(
        yaml.safe_load(model.to_catalog()),
        con=model.con,
        units=model.units,
        readiness=model.readiness,
        event_ids=model.event_ids,
    )
    assert restored.model_hash == model.model_hash
    assert set(model.catalog.dimensions) == {"plan"}
    assert model.catalog.sources["plan_change"].dimensions == ()


README_ROWS = {
    ("alice", "2025-02-01"): (2, 75.0, 25.0, None),
    ("bob", "2025-02-01"): (1, 80.0, 0.0, None),
    ("carol", "2025-02-01"): (0, 0.0, 0.0, None),
    ("alice", "2025-03-01"): (1, 25.0, None, "incomplete_coverage"),
    ("bob", "2025-03-01"): (0, 0.0, None, "incomplete_coverage"),
    ("carol", "2025-03-01"): (0, 0.0, None, "incomplete_coverage"),
}


def _readme_rows(model: Model) -> dict[tuple[str, str], tuple[Any, ...]]:
    table = readme_design().build(model).to_pyarrow().to_pydict()
    return {
        (unit, t0.date().isoformat()): (orders_30d, spend_30d, spend_next_30d, reason)
        for unit, t0, orders_30d, spend_30d, spend_next_30d, reason in zip(
            table["unit"],
            table["t0"],
            table["orders_30d"],
            table["spend_30d"],
            table["spend_next_30d"],
            table["spend_next_30d__reason"],
            strict=True,
        )
    }


def _in_memory(kind: str, table: pa.Table) -> Any:
    if kind == "arrow":
        return table
    if kind == "pandas":
        return table.to_pandas()
    return pytest.importorskip("polars").from_arrow(table)


def _readme_events(table: Any) -> dict[str, Events]:
    return {
        "orders": Events(
            table,
            time="ordered_at",
            key="customer_id",
            metrics={"orders": tr.count(), "spend": tr.sum("amount")},
            complete=("2025-01-01", "2025-03-20"),
        )
    }


def _in_memory_model(customers: Any, orders_table: Any) -> Model:
    return Model.from_tables(
        units={"customer": UnitSource(customers, "customer_id")},
        events=_readme_events(orders_table),
    )


def test_readme_model_builds_the_readme_rows(readme_con):
    assert _readme_rows(readme_model(readme_con)) == README_ROWS


@pytest.mark.parametrize("kind", ["arrow", "pandas", "polars"])
def test_in_memory_tables_build_the_readme_dataset(kind):
    tables = {name: _in_memory(kind, table) for name, table in readme_tables().items()}
    model = _in_memory_model(tables["customers"], tables["orders"])
    assert {"customer", "orders"} <= set(model.con.list_tables())
    assert model.units["customer"].table == "customer"
    catalog = yaml.safe_load(model.to_catalog())
    assert catalog["sources"]["orders"]["relation"] == {"kind": "table", "name": "orders"}
    assert _readme_rows(model) == README_ROWS


@pytest.mark.parametrize("kind", ["pandas", "polars"])
def test_descriptors_store_in_memory_tables_as_arrow(kind):
    frame = _in_memory(kind, readme_tables()["orders"])
    if kind == "pandas":
        frame = frame.set_axis([f"row{index}" for index in range(5)])
    for descriptor in (
        _readme_events(frame)["orders"],
        History(frame, key="customer_id", column="amount", changed_at="ordered_at"),
        UnitSource(frame, "customer_id"),
    ):
        assert isinstance(descriptor.table, pa.Table)
        assert set(descriptor.table.column_names) == {"customer_id", "ordered_at", "amount"}
        assert descriptor.table.num_rows == 5


def test_aware_timestamp_column_matches_naive_utc():
    customers = readme_tables()["customers"].to_pandas()
    naive = readme_tables()["orders"].to_pandas()
    aware = naive.assign(
        ordered_at=naive["ordered_at"].dt.tz_localize("UTC").dt.tz_convert("US/Eastern")
    )
    assert _readme_rows(_in_memory_model(customers, aware)) == README_ROWS
    assert _readme_rows(_in_memory_model(customers, naive)) == README_ROWS


@pytest.mark.parametrize(
    ("reason", "build", "context"),
    [
        (
            "in_memory_input_with_connection",
            lambda con: Model.from_tables(
                con,
                units={"customer": UnitSource(readme_tables()["customers"], "customer_id")},
                events=_readme_events(readme_tables()["orders"]),
            ),
            {"inputs": ("events:orders", "units:customer")},
        ),
        (
            "in_memory_input_with_connection",
            lambda con: Model.from_catalog(
                readme_model(con).catalog,
                con=con,
                units={"customer": UnitSource(readme_tables()["customers"], "customer_id")},
                readiness=readme_model(con).readiness,
            ),
            {"inputs": ("units:customer",)},
        ),
        (
            "connection_required",
            lambda con: Model.from_tables(
                units=UNITS, events=_readme_events(readme_tables()["orders"])
            ),
            {"tables": ("units:customer",)},
        ),
        (
            "unsupported_table_input",
            lambda con: UnitSource(con.table("customers"), "customer_id"),
            {"field": "table"},
        ),
        (
            "unsupported_table_input",
            lambda con: _readme_events({"customer_id": ["alice"]}),
            {"field": "table"},
        ),
        (
            "name_collision",
            lambda con: Model.from_tables(
                units={"orders": UnitSource(readme_tables()["customers"], "customer_id")},
                events=_readme_events(readme_tables()["orders"]),
            ),
            {"kind": "table", "name": "orders", "owners": ("events:orders", "units:orders")},
        ),
        (
            "name_collision",
            lambda con: Model.from_tables(
                units={"Orders": UnitSource(readme_tables()["customers"], "customer_id")},
                events=_readme_events(readme_tables()["orders"]),
            ),
            {"kind": "table", "owners": ("events:orders", "units:Orders")},
        ),
        (
            "wrong_type",
            lambda con: Model.from_tables(
                units={"customer": UnitSource(readme_tables()["customers"], "customer_id")},
                events=_readme_events(readme_tables()["orders"]),
                event_ids=cast(Any, 5),
            ),
            {"field": "event_ids"},
        ),
        (
            "wrong_type",
            lambda con: Model.from_tables(
                units=cast(Any, 5), events=_readme_events(readme_tables()["orders"])
            ),
            {"field": "units"},
        ),
    ],
)
def test_in_memory_refusals(reason, build, context, readme_con):
    with pytest.raises(TerrineError) as error:
        build(readme_con)
    assert error.value.code == "terrine.model.invalid_binding"
    assert error.value.context["reason"] == reason
    for key, value in context.items():
        assert error.value.context[key] == value
    for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
        assert restored.context == error.value.context


def test_missing_duckdb_extra_refuses_before_any_connection(monkeypatch):
    import terrine.model as model_module

    original = model_module.import_module

    def unavailable(name):
        if name == "duckdb":
            raise ImportError("optional backend unavailable")
        return original(name)

    def connected(*args, **kwargs):
        raise AssertionError("a connection was opened")

    monkeypatch.setattr(model_module, "import_module", unavailable)
    monkeypatch.setattr(ibis.duckdb, "connect", connected)
    with pytest.raises(TerrineError) as error:
        readme_model()
    assert error.value.code == "terrine.convert.missing_dependency"
    assert error.value.context["extra"] == "duckdb"
    assert "route" in error.value.context


def test_pandas_nullable_and_categorical_columns_survive_conversion():
    pd = pytest.importorskip("pandas")
    customers = pd.DataFrame({"customer_id": pd.Series(["alice", "bob"], dtype="string")})
    orders = pd.DataFrame(
        {
            "customer_id": ["alice", "alice", "bob", "bob"],
            "ordered_at": pd.to_datetime(["2025-01-03", "2025-01-04", "2025-01-05", "2025-01-06"]),
            "qty": pd.Series([1, None, 3, 4], dtype="Int64"),
            "status": pd.Categorical(["paid", "refunded", "paid", "paid"]),
            "flag": pd.Series([True, None, False, True], dtype="boolean"),
        }
    )
    model = Model.from_tables(
        units={"customer": UnitSource(customers, "customer_id")},
        events={
            "orders": Events(
                orders,
                time="ordered_at",
                key="customer_id",
                labels=("status", "flag", "qty"),
                metrics={
                    "n": tr.count(),
                    "qty": tr.sum("qty"),
                    "paid": tr.count(where={"status": "paid"}),
                },
                complete=COMPLETE,
            )
        },
    )
    assert {name: prop.dtype for name, prop in model.catalog.properties.items()} == {
        "status": "string",
        "flag": "bool",
        "qty": "int",
    }
    rows = _snapshot(
        model,
        "2025-01-10",
        features={
            "n": agg("n", window=("-9d", "0s")),
            "qty": agg("qty", window=("-9d", "0s")),
            "paid": agg("paid", window=("-9d", "0s")),
            "last_status": asof("status", before="0s"),
            "last_flag": asof("flag", before="0s"),
            "last_qty": asof("qty", before="0s"),
        },
    )
    assert rows["n"] == {"alice": 2, "bob": 2}
    assert rows["qty"] == {"alice": 1, "bob": 7}
    assert rows["paid"] == {"alice": 1, "bob": 2}
    assert rows["last_status"] == {"alice": "refunded", "bob": "paid"}
    assert rows["last_flag"] == {"alice": None, "bob": True}
    assert rows["last_flag__reason"] == {"alice": "all_values_null", "bob": None}
    assert rows["last_qty"] == {"alice": None, "bob": 4}
