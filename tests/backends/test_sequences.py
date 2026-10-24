import ibis
import pytest

from terrine.backends.sequences import integer_sequence


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake"])
@pytest.mark.parametrize("stop", [1, 9, 10, 11, 123])
def test_sequence_holds_every_integer_below_stop_once(dialect, stop):
    con = ibis.duckdb.connect()
    sequence = integer_sequence(stop, "ordinal", dialect=dialect)
    assert sequence.schema()["ordinal"].is_int64()
    rows = con.to_pyarrow(sequence.order_by("ordinal")).column("ordinal").to_pylist()
    assert rows == list(range(stop))
