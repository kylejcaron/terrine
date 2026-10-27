"""Opt in to read-only use of an unverified Ibis backend and inspect its stamp.

The Ibis SQLite backend ships with the base dependency, so no extra is needed:

    uv run python examples/unverified_backend.py

DuckDB, PostgreSQL, BigQuery and Snowflake are verified engines. Every other
genuine Ibis backend is refused by default, before any source schema is read.
``allow_unverified=True`` admits that one Model for read-only use under the
portable expression policy and prints ``UnverifiedBackendWarning`` once. An
operation the backend cannot represent exactly is refused with its code; it is
never answered approximately. A caller who accepts that may filter the warning
(``warnings.filterwarnings("ignore", category=UnverifiedBackendWarning)``); the
result's provenance still records ``engine_status="unverified"``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import ibis
import pyarrow as pa

from terrine import Design, Events, FixedDate, Model, TerrineError, UnitSource, agg
from terrine.data import MaterializedFrame


def main() -> None:
    con = ibis.sqlite.connect(":memory:")
    try:
        con.create_table("customers", pa.table({"id": ["active", "quiet"]}))
        con.create_table(
            "orders",
            pa.table(
                {
                    "customer_id": ["active", "active"],
                    "occurred_at": [
                        datetime(2025, 1, 2, tzinfo=UTC),
                        datetime(2025, 1, 10, tzinfo=UTC),
                    ],
                    "amount": [7, 13],
                }
            ),
        )
        binding = {
            "units": {"customer": UnitSource("customers", "id")},
            "events": {
                "orders": Events(
                    "orders",
                    "occurred_at",
                    {"customer": "customer_id"},
                    values=("amount",),
                    complete=("2025-01-01", "2025-01-31"),
                )
            },
        }

        # Without consent the backend is refused before its schema is read.
        try:
            Model.from_tables(con, **binding)
        except TerrineError as error:
            assert error.code == "terrine.backend.capability_unavailable"
            assert error.context["backend"] == "sqlite"
            assert error.context["operation"] == "query"
            assert error.context["status"] == "unverified"
            print("Refused by default:", error.code, "->", error.context["route"])
        else:
            raise AssertionError("an unregistered backend must not be admitted by default")

        # Consent is explicit and belongs to this Model; the warning is printed.
        model = Model.from_tables(con, allow_unverified=True, **binding)
        # Three distinct names: the executing backend, the expression policy
        # and the SQL dialect that parses admitted SQL. The real dialect, never
        # "generic", is what a Catalog declares.
        assert model.catalog.dialect == "sqlite"
        assert model.sources["orders"].dialect == "generic"
        print("Expression policy:", model.sources["orders"].dialect)
        print("SQL dialect:", model.catalog.dialect)

        # A count needs exact decimal arithmetic. SQLite stores decimals by
        # affinity, so the portable policy refuses this operation with its code
        # instead of returning rounded state; a backend with exact decimals
        # materializes the same Design and stamps the result unverified.
        counts = Design(
            "customer",
            FixedDate("2025-01-05"),
            features={"orders_before": agg("orders_count", window=("-7d", "0d"))},
        )
        try:
            counted = counts.build(model).materialize()
        except TerrineError as error:
            assert error.code == "terrine.backend.capability_unavailable"
            assert error.context["operation"] not in {"query", "dialect_admission"}
            print(
                "Count refused:",
                error.code,
                f"operation={error.context['operation']!r}",
                f"backend={error.context['backend']!r}",
            )
        else:
            stamp = counted.provenance
            print(
                "Count materialized:",
                counted.table.select(["unit", "orders_before"]).to_pylist(),
                (stamp.engine_name, stamp.dialect, stamp.engine_status),
            )

        # A real read through the admitted backend: the roster at t0, with the
        # unverified stamp and no snapshot identity.
        roster = Design("customer", FixedDate("2025-01-05")).build(model).materialize()
        stamp = roster.provenance
        assert sorted(roster.table["unit"].to_pylist()) == ["active", "quiet"]
        assert (stamp.engine_name, stamp.dialect, stamp.engine_status) == (
            "sqlite",
            "generic",
            "unverified",
        )
        assert stamp.execution_snapshot.snapshot_id is None
        assert stamp.execution_snapshot.replay_equivalence == "unverified"
        print("Roster rows:", sorted(roster.table["unit"].to_pylist()))
        print(
            "Stamp:",
            f"engine_name={stamp.engine_name!r}",
            f"dialect={stamp.dialect!r}",
            f"engine_status={stamp.engine_status!r}",
            f"snapshot_id={stamp.execution_snapshot.snapshot_id!r}",
        )

        # The stamp survives Arrow; a result cannot become verified later.
        restored = MaterializedFrame.from_arrow(roster.to_arrow())
        assert restored.provenance == stamp
        assert restored.table.equals(roster.table)

        # Consent is not connection-wide: a second Model needs it again.
        try:
            Model.from_tables(con, **binding)
        except TerrineError as error:
            assert error.context["operation"] == "query"
            print("A second Model without consent is refused again.")
        else:
            raise AssertionError("consent must not outlive the Model that gave it")
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
