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

_SHOP = Path(sys.prefix) / "share/metric-definitions/examples/shop"
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


def test_oversized_formula_literal_has_the_native_catalog_refusal(duckdb_con):
    literal = 10**400
    shared_catalog = shared.Catalog.model_validate(
        {
            "version": 1,
            "sources": {},
            "measures": {},
            "metrics": {"oversized": {"formula": f"1 + {literal}"}},
        }
    )
    native_catalog = {
        "schema_version": 4,
        "day_boundary": "UTC",
        "dialect": "duckdb",
        "sources": {},
        "events": {},
        "properties": {},
        "dimensions": {},
        "measures": {},
        "metrics": {
            "oversized": {
                "kind": "formula",
                "unit": "customer",
                "formula": {
                    "kind": "binary",
                    "operator": "+",
                    "left": {"kind": "literal", "value": 1},
                    "right": {"kind": "literal", "value": literal},
                },
            }
        },
    }
    units = {"customer": tr.UnitSource("customers", "customer_id")}
    with pytest.raises(tr.TerrineError) as native:
        tr.Model.from_catalog(native_catalog, con=duckdb_con, units=units, readiness={})
    with pytest.raises(tr.TerrineError) as translated:
        tr.Model.from_shared_metrics(shared_catalog, con=duckdb_con, units=units, readiness={})
    assert translated.value.code == native.value.code == "terrine.catalog.invalid_definition"
    assert translated.value.context == native.value.context
    assert native.value.context["issues"][0]["kind"] == "invalid_formula_literal"
