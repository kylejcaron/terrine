import ibis
import pyarrow as pa
import pytest

from terrine.compile.prune import prune_columns


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


def test_pruning_keeps_rows_and_schema_and_drops_unread_columns(con):
    """Projections, joins, aggregates, unions and shared views keep their rows, and
    the statement no longer carries a column nothing reads."""
    t = con.create_table(
        "t", pa.table({"k": [1, 1, 2, None], "x": [1.0, 2.0, 3.0, 4.0], "s": ["a", "b", "c", "d"]})
    )
    staged = t.mutate(y=t.x * 2, unread=ibis.literal("not a number").cast("int64"))
    shared = staged.view()
    totals = shared.group_by("k").aggregate(total=shared.y.sum(), rows=shared.count())
    joined = staged.left_join(totals, staged.k == totals.k).select(
        staged.k, staged.s, total=totals.total, rows=totals.rows
    )
    stacked = joined.union(joined.mutate(rows=joined.rows + 1), distinct=False)
    query = stacked.order_by(["s", "rows"]).select("s", "total", "rows")
    pruned = prune_columns(query)
    assert pruned.schema() == query.schema()
    assert len(ibis.to_sql(pruned)) < len(ibis.to_sql(query))
    assert con.to_pyarrow(pruned).equals(con.to_pyarrow(query))
    # A NULL key joins no total.
    expected: list[tuple[str, float | None, int | None]] = [("a", 6.0, 2), ("a", 6.0, 3)]
    expected += [("b", 6.0, 2), ("b", 6.0, 3), ("c", 6.0, 1), ("c", 6.0, 2)]
    expected += [("d", None, None), ("d", None, None)]
    assert [tuple(row.values()) for row in con.to_pyarrow(pruned).to_pylist()] == expected
