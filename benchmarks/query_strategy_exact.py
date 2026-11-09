"""Throwaway exact arithmetic checks: local summaries need not lose exactness."""

import json
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import ibis
import pyarrow as pa

import terrine as tr


def run():
    con = ibis.duckdb.connect(threads=1)
    try:
        instants = [datetime(2025, 1, day, tzinfo=UTC) for day in (13, 13, 14)]
        events = con.create_table(
            "exact_inputs",
            pa.table(
                {
                    "unit": ["a"] * 3,
                    "instant": instants,
                    "value": [6e-10, 6e-10, 4e-10],
                }
            ),
        )
        quantized = events.mutate(q=events.value.cast("decimal(38, 9)"))
        daily = quantized.group_by(quantized.instant).aggregate(
            total=quantized.q.sum(),
            below=((quantized.value != 0) & (quantized.q == 0)).cast("int64").sum(),
        )
        exact = (
            daily.aggregate(total=daily.total.sum(), below=daily.below.sum())
            .to_pyarrow()
            .to_pylist()[0]
        )
        assert exact["total"] == Decimal("0.000000002")
        assert exact["below"] == 1
        roster = con.create_table("roster", pa.table({"unit": ["a"]}))
        model = tr.Model.from_tables(
            units={"unit": tr.UnitSource(roster.to_pyarrow(), "unit")},
            events={
                "events": tr.Events(
                    events.to_pyarrow(),
                    time="instant",
                    key="unit",
                    metrics={"total": tr.sum("value", exact=True)},
                    complete=("2025-01-01", "2025-01-15"),
                )
            },
        )
        frame = tr.Design(
            "unit",
            tr.FixedDate("2025-01-15"),
            features={"total": tr.agg("total", window=("-2d", "0s"))},
        ).build(model)
        current = frame.to_pyarrow().to_pylist()[0]
        assert abs(current["total"] - float(exact["total"])) <= 1e-24
        assert current[frame.columns["total"].reason_column] == "below_resolution"
        wide = con.create_table(
            "wide_inputs",
            pa.table({"day": [1, 2], "value": pa.array([2**53 + 1, 2], type=pa.int64())}),
        )
        by_day = wide.group_by(wide.day).aggregate(total=wide.value.cast("decimal(38, 0)").sum())
        wide_total = by_day.total.sum().execute()
        assert wide_total == Decimal(2**53 + 3)
        decimals = con.create_table(
            "decimal_inputs",
            pa.table(
                {
                    "day": [1, 2],
                    "value": pa.array(
                        [Decimal("100000000000000000000.000000001"), Decimal("0.000000002")],
                        type=pa.decimal128(38, 9),
                    ),
                }
            ),
        )
        by_day = decimals.group_by(decimals.day).aggregate(total=decimals.value.sum())
        decimal_total = by_day.total.sum().execute()
        assert decimal_total == Decimal("100000000000000000000.000000003")
        return {
            "quantized_total": str(exact["total"]),
            "discarded_nonzero_terms": exact["below"],
            "current_exact_float_matches": True,
            "wide_integer_total": str(wide_total),
            "declared_decimal_total": str(decimal_total),
            "limits": (
                "DuckDB known answers only; not a general exact reducer or overflow certification. "
                "Quantization is per input, before grouping."
            ),
        }
    finally:
        con.disconnect()


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2))
    Path(sys.argv[1]).write_text(json.dumps(result, indent=2))
