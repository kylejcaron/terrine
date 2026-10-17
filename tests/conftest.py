import pytest


@pytest.fixture
def native_inputs():
    from datetime import UTC, datetime

    import ibis
    import pyarrow as pa

    from terrine.semantic.models import Catalog

    con = ibis.duckdb.connect()
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["a", "b"],
                "ts": [datetime(2025, 1, 3, tzinfo=UTC)] * 2,
                "event_id": ["a1", "b1"],
                "alternate_event_id": ["a2", "b2"],
            }
        ),
    )
    con.create_table("labels", con.table("events"))
    con.create_table("users", pa.table({"user_id": ["a", "b"], "alt_id": ["aa", "bb"]}))
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                name: {
                    "relation": {"kind": "table", "name": name},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": [fact],
                    "properties": [],
                    "dimensions": [],
                }
                for name, fact in (("events", "session"), ("labels", "churn"))
            },
            "facts": {
                "session": {"source": "events", "column": None},
                "churn": {"source": "labels", "column": None},
            },
            "properties": {},
            "dimensions": {},
            "measures": {
                "sessions_count": {"fact": "session", "aggregation": "count"},
                "churn_count": {"fact": "churn", "aggregation": "count"},
            },
            "metrics": {
                "sessions": {"kind": "mean", "entity": "user", "measure": "sessions_count"},
                "churned": {"kind": "conversion", "entity": "user", "measure": "churn_count"},
            },
        }
    )
    try:
        yield catalog, con
    finally:
        con.disconnect()
