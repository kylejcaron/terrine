import copy
import pickle
import sys

import pytest

from terrine.errors import TerrineError
from terrine.semantic.sql import _verified, admit_read_query

# About thirty nesting levels parse within the interpreter's default budget;
# two hundred cannot, from any caller.
_DEEP = "SELECT " + "(" * 30 + "1" + ")" * 30 + " AS value"
_TOO_DEEP = "SELECT " + "(" * 200 + "1" + ")" * 200 + " AS value"


def _from_depth(remaining: int, action):
    """Call ``action`` from a stack ``remaining`` frames below the recursion limit."""
    depth = 0
    frame = sys._getframe()
    while frame is not None:
        depth += 1
        frame = frame.f_back

    def descend(count: int):
        return descend(count - 1) if count else action()

    return descend(sys.getrecursionlimit() - depth - remaining)


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


def test_sql_admission_depth_does_not_depend_on_the_caller_stack():
    """Parsing recurses per nesting level; a caller near the recursion limit must
    not turn a readable statement into a refusal, and the limit itself is untouched."""
    limit = sys.getrecursionlimit()
    assert _from_depth(120, lambda: admit_read_query(_DEEP, "duckdb", "duckdb")) == _DEEP
    assert sys.getrecursionlimit() == limit


def test_sql_too_deep_to_verify_is_a_coded_refusal():
    with pytest.raises(TerrineError) as caught:
        admit_read_query(_TOO_DEEP, "duckdb", "duckdb")
    error = caught.value
    assert error.code == "terrine.sql.invalid_read_query"
    assert error.context["reason"] == "too_deep_to_verify"
    assert isinstance(error.__cause__, RecursionError)
    with pytest.raises(TypeError):
        error.context["reason"] = "changed"  # ty: ignore[invalid-assignment]
    for copied in (copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
        assert copied.code == error.code
        assert dict(copied.context) == dict(error.context)


def test_verification_step_propagates_other_outcomes_unchanged():
    assert _verified(lambda: _verified(lambda: 1) + 1) == 2

    def fail():
        raise ValueError("not a depth problem")

    with pytest.raises(ValueError, match="not a depth problem"):
        _verified(fail)

    def recurse():
        return recurse()

    with pytest.raises(TerrineError) as caught:
        _verified(lambda: _verified(recurse))
    assert caught.value.context["reason"] == "too_deep_to_verify"
