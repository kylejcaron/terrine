"""Exact retention and training-only winsor limits reused by a scoring definition."""

from __future__ import annotations

import tempfile
from decimal import Decimal
from pathlib import Path

import ibis

from examples.ml_staging import catalog
from examples.retention_metrics import main as retention_main
from terrine import Budget, Coverage, Design, Model, Schedule, UnitSource, agg
from terrine.dataset.splits import TimeSplit
from terrine.spec import DatasetDefinition, Winsorization


def main() -> None:
    retention_main()
    with tempfile.TemporaryDirectory(prefix="terrine-winsorization-") as directory:
        root = Path(directory)
        con = ibis.duckdb.connect(str(root / "dataset.duckdb"))
        try:
            con.raw_sql("CREATE TABLE users AS SELECT 1 AS user_id")
            con.raw_sql(
                "CREATE TABLE events AS SELECT ts::TIMESTAMP AS ts, 1 AS user_id, "
                "amount::BIGINT AS amount, 'known' AS segment FROM (VALUES "
                "('2025-01-02 12:00:00',0),('2025-01-03 12:00:00',10),"
                "('2025-01-04 12:00:00',20),('2025-01-05 12:00:00',1000),"
                "('2025-01-06 12:00:00',2000)) AS points(ts,amount)"
            )
            con.raw_sql("CREATE TABLE labels AS SELECT ts,user_id,amount FROM events")
            model = Model.from_catalog(
                catalog(),
                con=con,
                units={"user": UnitSource("users", "user_id")},
                readiness={
                    name: Coverage("2025-01-01", "2025-01-12") for name in ("events", "labels")
                },
            )
            design = Design(
                "user",
                Schedule("1d", "2025-01-03", "2025-01-08"),
                features={"past_amount": agg("event_total", window=("-1d", "0s"))},
                outcomes={"label": agg("label_total", window=("0s", "1h"))},
            )
            budget = Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)
            training = design.build(model).training_set(
                label="label",
                split=TimeSplit(train_until="2025-01-06", validation_until="2025-01-07"),
            )
            fitted = training.fit_winsorization(
                "past_amount", lower="0.25", upper="0.75", budget=budget
            )
            assert (fitted.lower, fitted.upper) == (Decimal(5), Decimal(15))
            transform_path = root / "limits.json"
            transform_path.write_bytes(fitted.to_json())
            frozen = Winsorization.from_json(transform_path.read_bytes())
            dataset = training.winsorize(frozen).stage("training", budget=budget)
            assert dataset.manifest.definition.transforms == (frozen,)
            con.raw_sql("INSERT INTO events VALUES ('2025-01-09 12:00:00',1,5000,'known')")
            scoring = design.snapshot(
                model, as_of="2025-01-10", features=("past_amount",), eligibility={}
            ).winsorize(frozen)
            scored_dataset = scoring.stage("scoring", budget=budget)
            definition = DatasetDefinition.from_json(
                scored_dataset.manifest.definition.canonical_json()
            )
            result = definition.build(model).materialize(budget)
            assert result.table["past_amount"].to_pylist() == [Decimal(15)]
            print(
                {
                    "training_limits": [str(frozen.lower), str(frozen.upper)],
                    "scoring": [str(value) for value in result.table["past_amount"].to_pylist()],
                }
            )
        finally:
            con.disconnect()


if __name__ == "__main__":
    main()
