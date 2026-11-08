"""Native and exact arithmetic of additive metric windows through the compiler."""

from __future__ import annotations


import ibis

from tests.fixtures.readme import readme_design, readme_model


def test_native_readme_sql_has_no_decimal_cast_and_is_smaller(readme_con):
    sql = {
        exact: ibis.to_sql(readme_design().build(readme_model(readme_con, exact=exact)).expr)
        for exact in (False, True)
    }
    assert "DECIMAL(38, 9)" not in sql[False]
    assert "DECIMAL(38, 9)" in sql[True]
    assert len(sql[False]) < len(sql[True])
