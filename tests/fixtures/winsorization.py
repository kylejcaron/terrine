from terrine import Coverage, Model, UnitSource
from terrine.semantic.models import Catalog
from terrine.spec import Design, Schedule, agg, asof


def bind_model(con, *, label_end="2025-01-12", scoring=False):
    catalog = Catalog.model_validate(
        {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["amount"],
                    "properties": ["category"],
                    "dimensions": [],
                },
                "labels": {
                    "relation": {"kind": "table", "name": "labels"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["label"],
                    "properties": [],
                    "dimensions": [],
                },
            },
            "events": {
                "amount": {"source": "events", "column": "amount"},
                "label": {"source": "labels", "column": None},
            },
            "properties": {
                "category": {
                    "source": "events",
                    "column": "category",
                    "dtype": "string",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {
                "total": {"event": "amount", "aggregation": "sum"},
                "labels": {"event": "label", "aggregation": "count"},
            },
            "metrics": {
                "total": {"kind": "mean", "unit": "user", "measure": "total"},
                "labels": {"kind": "conversion", "unit": "user", "measure": "labels"},
            },
        }
    )
    readiness = {
        "events": Coverage("2025-01-01", "2025-01-12"),
        "labels": Coverage("2025-01-01", label_end),
    }
    if scoring:
        definitions = catalog.model_dump(mode="python")
        for section, name in (
            ("sources", "labels"),
            ("events", "label"),
            ("measures", "labels"),
            ("metrics", "labels"),
        ):
            del definitions[section][name]
        catalog = Catalog.model_validate(definitions)
        del readiness["labels"]
    return Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness=readiness,
    )


def create_model(con):
    con.raw_sql("CREATE TABLE users AS SELECT 'a' AS user_id")
    con.raw_sql(
        "CREATE TABLE events AS SELECT 'a' AS user_id, ts::TIMESTAMPTZ AS ts, "
        "amount::BIGINT AS amount, 'known' AS category FROM (VALUES "
        "('2025-01-02 12:00:00+00',0),('2025-01-03 12:00:00+00',10),"
        "('2025-01-04 12:00:00+00',20),('2025-01-05 12:00:00+00',1000),"
        "('2025-01-06 12:00:00+00',2000)) AS points(ts,amount)"
    )
    con.raw_sql("CREATE TABLE labels AS SELECT user_id, ts FROM events")
    return bind_model(con)


def design():
    return Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-07"),
        features={
            "value": agg("total", window=("-1d", "0s")),
            "category": asof("category", before="0s"),
        },
        outcomes={"label": agg("labels", window=("0s", "1h"))},
    )
