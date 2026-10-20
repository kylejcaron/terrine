"""One native typed dataset shared by local interoperability checks."""

from datetime import UTC, datetime

from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg, asof


def interop_frame(con, *, empty=False, full=True):
    con.raw_sql("""
        CREATE TABLE events (
            user_id VARCHAR, ts TIMESTAMPTZ, amount DECIMAL(38, 2), score DOUBLE,
            note VARCHAR
        )
    """)
    con.raw_sql("""
        INSERT INTO events VALUES
        ('a', '2025-01-02 11:59:59+00', 999999999999999999999999999999999999.98, 0.1, NULL),
        ('a', '2025-01-02 12:00:00+00', 0.01, 0.2, NULL),
        ('b', '2025-01-02 11:59:59+00', NULL, NULL, NULL)
    """)
    con.raw_sql("CREATE TABLE users AS SELECT * FROM (VALUES ('a'), ('b'), ('c')) AS u(id)")
    con.raw_sql("""
        CREATE TABLE accounts AS SELECT user_id, tier, starts::TIMESTAMPTZ AS starts,
            ends::TIMESTAMPTZ AS ends
        FROM (VALUES
            ('a', 'old', '2025-01-01 00:00:00+00', '2025-01-02 12:00:00+00'),
            ('a', 'new', '2025-01-02 12:00:00+00', NULL),
            ('b', NULL, '2025-01-01 00:00:00+00', NULL),
            ('c', 'empty', '2025-01-01 00:00:00+00', NULL)
        ) AS d(user_id, tier, starts, ends)
    """)
    if empty:
        con.raw_sql("DELETE FROM users")
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["amount", "score"],
                    "properties": ["amount_value", "note", "tier"],
                    "dimensions": ["accounts"],
                }
            },
            "facts": {name: {"source": "events", "column": name} for name in ("amount", "score")},
            "properties": {
                "amount_value": {
                    "source": "events",
                    "column": "amount",
                    "dtype": "float",
                    "as_of": "event_time",
                },
                "note": {
                    "source": "events",
                    "column": "note",
                    "dtype": "string",
                    "as_of": "event_time",
                },
                "tier": {
                    "source": "events",
                    "column": "tier",
                    "dtype": "string",
                    "as_of": "event_time",
                    "dimension": "accounts",
                },
            },
            "dimensions": {
                "accounts": {
                    "relation": {"kind": "table", "name": "accounts"},
                    "source_entity": "user",
                    "entity_column": "user_id",
                    "properties": ["tier"],
                    "validity": {"valid_from": "starts", "valid_to": "ends"},
                }
            },
            "measures": {
                name: {"fact": name, "aggregation": "sum"} for name in ("amount", "score")
            },
            "metrics": {
                name: {"kind": "mean", "entity": "user", "measure": name}
                for name in ("amount", "score")
            },
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-02")},
    )
    return interop_design(full=full).build(model)


def interop_design(*, full=True):
    features = (
        {
            "amount": agg("amount", window=("-1s", "0s")),
            "score": agg("score", window=("-1s", "0s")),
            "tier": asof("tier", before="0s"),
            "note": asof("note", before="0s"),
        }
        if full
        else {"amount": asof("amount_value", before="0s"), "note": asof("note", before="0s")}
    )
    return Design(
        "user",
        FixedDate(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        features=features,
        outcomes={"future": agg("amount", window=("0s", "2d"))} if full else {},
    )
