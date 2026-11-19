"""The declared population survives conversion to a native Catalog."""

import copy
import pickle
import sys
from pathlib import Path

import ibis
import pytest

import terrine as tr
from terrine.adapters.shared import to_catalog
from terrine.semantic.io import dump_catalog

shared = pytest.importorskip("metric_definitions")

_SHOP = Path(sys.prefix) / "share/metric-definitions-core/examples/shop"
_METRIC = "revenue_per_customer"


@pytest.fixture
def duckdb_con():
    con = ibis.duckdb.connect()
    try:
        yield con
    finally:
        con.disconnect()


@pytest.mark.parametrize(
    ("units", "reason"),
    [
        ({}, "missing_roster"),
        (
            {"customer": tr.UnitSource("paying_customers", "customer_id")},
            "roster_identity_mismatch",
        ),
        ({"customer": tr.UnitSource("customers", "other_id")}, "roster_identity_mismatch"),
    ],
    ids=["missing", "other_table", "other_key"],
)
def test_declared_population_refuses_wrong_roster_without_resolving_rows(duckdb_con, units, reason):
    parsed = shared.load(_SHOP / "metrics.yaml")
    catalog = to_catalog(parsed, dialect="duckdb", units=frozenset(units), metrics=[_METRIC])
    # Identical rows are not proof of identical roster declarations.
    duckdb_con.raw_sql("CREATE TABLE customers AS SELECT 'A' customer_id, 'A' other_id")
    duckdb_con.raw_sql("CREATE TABLE paying_customers AS FROM customers")
    # No orders table: the population failure must precede fact resolution.
    for bind in (
        lambda: tr.Model.from_shared_metrics(
            parsed, con=duckdb_con, units=units, readiness={}, metrics=[_METRIC]
        ),
        lambda: tr.Model.from_catalog(
            dump_catalog(catalog), con=duckdb_con, units=units, readiness={}
        ),
    ):
        with pytest.raises(tr.TerrineError) as caught:
            bind()
        expected = {
            "metric": _METRIC,
            "target": "terrine",
            "population": "eligible_customers",
            "relation": "customers",
            "key": "customer_id",
            "unit": "customer",
            "reason": reason,
        }
        for error in (
            caught.value,
            copy.deepcopy(caught.value),
            pickle.loads(pickle.dumps(caught.value)),
        ):
            assert error.code == "terrine.semantic.population_binding"
            assert error.context == expected
