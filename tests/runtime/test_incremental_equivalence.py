from dataclasses import replace

import pytest

from terrine.compile.columns import compile_columns
from terrine.compile.layers import measure_identity, plan_layers
from terrine.compile.materialize import compose_materialized_frame
from terrine.data import Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg, gt
from terrine.temporal.daily import _sparse_group, resolve_measure
from terrine.temporal.prefix import sparse_prefix

pytest_plugins = ("tests.compile.test_columns",)


def _same_snapshot_prefixes(design, model):
    measures = {
        measure_identity(measure): measure
        for metric in model.metrics.values()
        for measure in metric.measures
    }
    persisted = {}
    for key in plan_layers(design, model).keys:
        source = model.sources[key.source]
        events = normalize_events(
            model._relation(source.relation), source, source.entities[design.unit]
        )
        if key.measure is None:
            raise AssertionError("planned temporal layer has no measure identity")
        measure = resolve_measure(events, measures[key.measure], source=source)
        grouped = _sparse_group(
            events, measure, {"_timestamp": events._timestamp}, "_unit", source=source
        )
        persisted[replace(key, snapshot_id="pinned-snapshot")] = sparse_prefix(
            grouped, measure, resolution="event"
        )
    return persisted


def _float_model(column_model):
    con = column_model.con
    con.raw_sql("ALTER TABLE events ALTER amount TYPE DOUBLE")
    return Model.from_catalog(
        Catalog.model_validate(column_model.catalog.model_dump(mode="python")),
        con=con,
        readiness=column_model.readiness,
        units=column_model.units,
    )


def test_same_snapshot_incremental_compilation_matches_clean_values_and_eligibility(column_model):
    model = _float_model(column_model)
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
        eligibility={"positive": gt(agg("total", window=("-1d", "0s")), 0.0)},
    )
    clean = compile_columns(design, model)
    incremental = compose_materialized_frame(
        design, model, persisted=_same_snapshot_prefixes(design, model)
    )
    clean.validate(execute=model.con.to_pyarrow)
    incremental.validate(execute=model.con.to_pyarrow)
    assert model.con.to_pyarrow(clean.audit_table.order_by("unit")).equals(
        model.con.to_pyarrow(incremental.audit_table.order_by("unit"))
    )
    assert model.con.to_pyarrow(clean.table.order_by("unit")).equals(
        model.con.to_pyarrow(incremental.table.order_by("unit"))
    )
    assert clean.columns == incremental.columns


def test_historical_1e16_insert_has_identical_float64_refusal(column_model):
    model = _float_model(column_model)
    model = Model.from_catalog(
        model.catalog,
        con=model.con,
        readiness={"events": Coverage("2024-12-31", "2025-01-02")},
        units=model.units,
    )
    model.con.raw_sql(
        "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-01 12:00:00+00', 1e16, 'old')"
    )
    model.con.raw_sql(
        "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-02 11:59:57+00', 1.0, 'old')"
    )
    model.con.raw_sql(
        "INSERT INTO events VALUES ('a', TIMESTAMPTZ '2025-01-02 11:59:58+00', -1e16, 'old')"
    )
    model.con.raw_sql(
        "UPDATE events SET amount=1.0 WHERE user_id='a' AND ts = TIMESTAMPTZ "
        "'2025-01-02 11:59:59+00'"
    )
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-2d", "0s"))},
    )
    clean = compile_columns(design, model)
    incremental = compose_materialized_frame(
        design, model, persisted=_same_snapshot_prefixes(design, model)
    )
    refusals = []
    for compiled in (clean, incremental):
        with pytest.raises(TerrineError) as error:
            compiled.validate(execute=model.con.to_pyarrow)
            model.con.to_pyarrow(compiled.audit_table)
        refusals.append((error.value.code, error.value.context))
    assert refusals[0] == refusals[1]
    assert refusals[0][0] == "terrine.temporal.float64_precision"
