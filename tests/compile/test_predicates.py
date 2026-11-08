import ibis
import pytest

import terrine as tr
from terrine.semantic.predicates import lower_predicate
from terrine.spec import Expr, asof


@pytest.fixture(scope="module")
def table():
    con = ibis.duckdb.connect()
    try:
        yield con.sql("""
            SELECT amount::BIGINT AS amount, status::VARCHAR AS status
            FROM (VALUES (5, 'paid'), (10, 'refunded'), (NULL, NULL)) t(amount, status)
        """)
    finally:
        con.disconnect()


@pytest.mark.parametrize(
    ("where", "predicate"),
    [
        ({"status": "paid"}, lambda value: tr.eq(value, "paid")),
        ({"status__ne": "paid"}, lambda value: tr.ne(value, "paid")),
        ({"amount__gt": 10}, lambda value: tr.gt(value, 10)),
        ({"amount__gte": 10}, lambda value: tr.gte(value, 10)),
        ({"amount__lt": 10}, lambda value: tr.lt(value, 10)),
        ({"amount__lte": "9.5"}, lambda value: tr.lte(value, "9.5")),
        ({"status": ["paid", None]}, lambda value: tr.isin(value, ["paid", None])),
        ({"status__in": {"refunded", "paid"}}, lambda value: tr.isin(value, {"paid", "refunded"})),
        ({"amount__between": (5, 10)}, lambda value: tr.between(value, 5, 10)),
    ],
)
def test_where_dict_and_predicate_compile_identically(table, where, predicate):
    (filt,) = tr.count(where=where).filters
    column = table[filt.property]
    from_where = lower_predicate(filt, {filt.property: column}.__getitem__)
    expression = asof(filt.property)
    by_expression: dict[Expr, ibis.Value] = {expression: column}
    from_design = lower_predicate(predicate(expression), by_expression.__getitem__)
    assert from_where.equals(from_design)
