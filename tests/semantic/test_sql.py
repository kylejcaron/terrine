import pytest

from terrine.errors import TerrineError
from terrine.semantic.sql import admit_read_query


@pytest.mark.parametrize(
    ("sql", "dialect"),
    [
        ("DELETE FROM events", "duckdb"),
        ("SELECT * FROM events; DROP TABLE events", "duckdb"),
        ("SELECT * FROM events FOR UPDATE", "duckdb"),
        ("SELECT 1 INTO result", "duckdb"),
        ("WITH removed AS (DELETE FROM events RETURNING *) SELECT * FROM removed", "postgres"),
        ("SELECT nextval('sequence')", "duckdb"),
        ("SELECT * FROM custom.abs(1)", "duckdb"),
    ],
)
def test_sql_admission_rejects_non_read_only_or_ambiguous_sql(sql, dialect):
    with pytest.raises(TerrineError) as error:
        admit_read_query(sql, dialect, dialect)
    assert error.value.code == "terrine.sql.invalid_read_query"


def test_sql_admission_accepts_single_read_query_without_rewriting():
    sql = "SELECT user_id, COALESCE(SUM(amount), 0) AS amount FROM events GROUP BY user_id"
    assert admit_read_query(sql, "duckdb", "duckdb") == sql


def test_sql_admission_rejects_dialect_mismatch():
    with pytest.raises(ValueError):
        admit_read_query("SELECT 1", "postgres", "duckdb")
