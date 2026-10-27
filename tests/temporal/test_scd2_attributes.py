from datetime import UTC, datetime

import ibis
import pytest

from terrine.errors import TerrineError
from terrine.semantic.models import (
    BoundDimension,
    BoundSource,
    BoundValidity,
    PropertyBinding,
    TableRef,
)
from terrine.temporal.attributes import (
    _enriched_property_columns,
    attribute_asof,
    enrich_events,
    validate_history,
)


def _dimension(*, validity=None, key="account_id"):
    return BoundDimension(
        name="account_tier",
        relation=TableRef(kind="table", name="tier_history"),
        source_entity="account",
        entity_column=key,
        validity=validity,
        properties={
            "tier": PropertyBinding("tier", "tier_value", "string", "pre_exposure", "account_tier")
        },
    )


def _source(dimension):
    return BoundSource(
        name="events",
        relation=TableRef(kind="table", name="events"),
        timestamp_column="occurred_at",
        entities={"user": "user_id", "account": "account_id_event"},
        facts={},
        properties={},
        dimensions=(dimension,),
        dialect="duckdb",
        day_boundary="UTC",
    )


@pytest.fixture
def connection():
    con = ibis.duckdb.connect()
    try:
        yield con
    finally:
        con.disconnect()


def test_strict_asof_excludes_version_start_and_distinguishes_matched_null(connection):
    t0 = datetime(2025, 3, 1, tzinfo=UTC)
    points = ibis.memtable({"point_id": [1, 2], "account": ["a", "missing"]}).mutate(
        cutoff=ibis.literal(t0)
    )
    history = ibis.memtable(
        {
            "account_id": ["a", "a"],
            "starts": [datetime(2025, 1, 1, tzinfo=UTC), t0],
            "ends": [t0, None],
            "tier_value": [None, "gold"],
        }
    )
    history = connection.create_table("tier_history", history)
    dimension = _dimension(validity=BoundValidity("starts", "ends", None))
    execute = connection.to_pyarrow
    validate_history(history, dimension, execute=execute)
    result = connection.to_pyarrow(
        attribute_asof(
            points, history, dimension, "tier", unit_column="account", cutoff_column="cutoff"
        )
    ).to_pylist()
    assert result == [
        {
            "point_id": 1,
            "account": "a",
            "cutoff": t0,
            "__terrine_attribute_value": None,
            "__terrine_attribute_found": True,
        },
        {
            "point_id": 2,
            "account": "missing",
            "cutoff": t0,
            "__terrine_attribute_value": None,
            "__terrine_attribute_found": False,
        },
    ]


def test_event_enrichment_uses_declared_account_key_and_event_time(connection):
    t1, t2 = datetime(2025, 3, 1, tzinfo=UTC), datetime(2025, 3, 2, tzinfo=UTC)
    events = ibis.memtable(
        {
            "event_id": [10, 11],
            "_timestamp": [t1, t2],
            "_unit": ["u", "u"],
            "account_id_event": ["a", "a"],
            "user_id": ["u", "u"],
        }
    )
    tier = ibis.memtable(
        {
            "account_id": ["a", "a"],
            "starts": [datetime(2025, 1, 1, tzinfo=UTC), t2],
            "ends": [t2, None],
            "tier_value": ["gold", "silver"],
        }
    )
    dimension = _dimension(validity=BoundValidity("starts", "ends", None))
    validate_history(tier, dimension, execute=connection.to_pyarrow)
    enriched = enrich_events(events, _source(dimension), {"account_tier": tier})
    aliases = _enriched_property_columns((dimension,), events.columns)
    value_col, found_col = aliases[("account_tier", "tier")]
    rows = connection.to_pyarrow(enriched).to_pylist()
    assert [
        (row["event_id"], row["_unit"], row["user_id"], row[value_col], row[found_col])
        for row in rows
    ] == [(10, "u", "u", "gold", True), (11, "u", "u", "silver", True)]
    assert len(rows) == 2


def test_event_enrichment_refuses_missing_selected_entity_column():
    events = ibis.memtable({"_timestamp": [datetime(2025, 3, 1, tzinfo=UTC)], "_unit": ["u"]})
    tier = ibis.memtable({"account_id": ["a"], "tier_value": ["gold"]})
    with pytest.raises(TerrineError) as error:
        enrich_events(events, _source(_dimension()), {"account_tier": tier})
    assert error.value.code == "terrine.temporal.missing_join_key"


def test_validator_refuses_overlapping_ranges_with_bounded_executor(connection):
    history = ibis.memtable(
        {
            "account_id": ["a", "a"],
            "starts": [datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 2, 1, tzinfo=UTC)],
            "ends": [datetime(2025, 3, 1, tzinfo=UTC), None],
            "tier_value": ["silver", "gold"],
        }
    )
    dimension = _dimension(validity=BoundValidity("starts", "ends", None))

    def execute(query):
        result = connection.to_pyarrow(query)
        assert result.num_rows == 1
        return result

    with pytest.raises(TerrineError) as error:
        validate_history(history, dimension, execute=execute)
    assert error.value.code == "terrine.temporal.overlapping_history"


def test_validator_detects_nested_overlap_across_prior_ranges(connection):
    history = ibis.memtable(
        {
            "account_id": ["a", "a", "a"],
            "starts": [
                datetime(2025, 1, 1, tzinfo=UTC),
                datetime(2025, 2, 1, tzinfo=UTC),
                datetime(2025, 3, 15, tzinfo=UTC),
            ],
            "ends": [
                datetime(2025, 4, 1, tzinfo=UTC),
                datetime(2025, 3, 1, tzinfo=UTC),
                None,
            ],
            "tier_value": ["one", "two", "three"],
        }
    )
    dimension = _dimension(validity=BoundValidity("starts", "ends", None))
    with pytest.raises(TerrineError) as error:
        validate_history(history, dimension, execute=connection.to_pyarrow)
    assert error.value.code == "terrine.temporal.overlapping_history"


def test_asof_key_type_comparison_ignores_nullability():
    points = ibis.memtable(
        {"account": ["a"], "cutoff": [datetime(2025, 3, 1, tzinfo=UTC)]},
        schema={"account": "!string", "cutoff": "timestamp('UTC')"},
    )
    history = ibis.memtable(
        {"account_id": ["a"], "tier_value": ["gold"]},
        schema={"account_id": "string", "tier_value": "string"},
    )
    result = attribute_asof(
        points, history, _dimension(), "tier", unit_column="account", cutoff_column="cutoff"
    )
    assert "__terrine_attribute_found" in result.columns


def test_static_dimension_validator_rejects_duplicate_entity_key(connection):
    dimension = _dimension()
    history = ibis.memtable({"account_id": ["a", "a"], "tier_value": ["silver", "gold"]})
    with pytest.raises(TerrineError) as error:
        validate_history(history, dimension, execute=connection.to_pyarrow)
    assert error.value.code == "terrine.temporal.static_key_multiplicity"


def test_validator_rejects_multiple_valid_from_only_rows_for_entity(connection):
    history = ibis.memtable(
        {
            "account_id": ["a", "a"],
            "starts": [datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 2, 1, tzinfo=UTC)],
            "tier_value": ["silver", "gold"],
        }
    )
    dimension = _dimension(validity=BoundValidity("starts", None, None))

    with pytest.raises(TerrineError) as error:
        validate_history(history, dimension, execute=connection.to_pyarrow)
    assert error.value.code == "terrine.temporal.overlapping_history"


def test_static_physical_count_alias_preserves_unique_attributes(connection):
    dimension = _dimension(key="__terrine_n")
    history = ibis.memtable({"__terrine_n": ["a", "b"], "tier_value": ["silver", "gold"]})
    points = ibis.memtable(
        {"account": ["a", "b"], "cutoff": [datetime(2025, 3, 1, tzinfo=UTC)] * 2}
    )
    validate_history(history, dimension, execute=connection.to_pyarrow)
    result = connection.to_pyarrow(
        attribute_asof(
            points, history, dimension, "tier", unit_column="account", cutoff_column="cutoff"
        ).order_by("account")
    ).to_pylist()
    assert [(row["account"], row["__terrine_attribute_value"]) for row in result] == [
        ("a", "silver"),
        ("b", "gold"),
    ]


def test_temporal_duplicate_group_count_alias_avoids_history_column(connection):
    history = ibis.memtable(
        {
            "account_id": ["a", "a"],
            "__terrine_n": [datetime(2025, 1, 1, tzinfo=UTC)] * 2,
            "tier_value": ["silver", "gold"],
        }
    )
    dimension = _dimension(validity=BoundValidity("__terrine_n", None, None))

    with pytest.raises(TerrineError) as error:
        validate_history(history, dimension, execute=connection.to_pyarrow)
    assert error.value.code == "terrine.temporal.duplicate_history"


def test_validity_declared_on_one_column_for_both_ends(connection):
    """A validity whose ``valid_from`` and ``valid_to`` name the same column is
    permitted by the catalog; every non-empty row is malformed (an end at or
    before its start) and refuses by code, while an empty history validates."""
    dimension = _dimension(validity=BoundValidity("ts", "ts", None))
    history = ibis.memtable(
        {
            "account_id": ["a"],
            "ts": [datetime(2025, 1, 1, tzinfo=UTC)],
            "tier_value": ["gold"],
        }
    )
    with pytest.raises(TerrineError) as error:
        validate_history(history, dimension, execute=connection.to_pyarrow)
    assert error.value.code == "terrine.temporal.malformed_history"
    empty = history.filter(history.account_id.isnull())
    validate_history(empty, dimension, execute=connection.to_pyarrow)


def test_enrichment_aliases_are_deterministic_and_avoid_casefold_collisions():
    dimension = _dimension()
    first = _enriched_property_columns((dimension,), ("ID",))
    second = _enriched_property_columns((dimension,), ("id",))
    assert first == second
    value, found = first[("account_tier", "tier")]
    assert value.casefold() != "id" and found.casefold() != "id"


def test_asof_refuses_fixed_output_alias_collision():
    points = ibis.memtable(
        {
            "account": ["a"],
            "cutoff": [datetime(2025, 3, 1, tzinfo=UTC)],
            "__terrine_attribute_found": [True],
        }
    )
    history = ibis.memtable({"account_id": ["a"], "tier_value": ["gold"]})
    with pytest.raises(TerrineError) as error:
        attribute_asof(
            points, history, _dimension(), "tier", unit_column="account", cutoff_column="cutoff"
        )
    assert error.value.code == "terrine.temporal.alias_collision"


def test_missing_validity_column_has_shared_schema_refusal():
    instant = datetime(2025, 3, 1, tzinfo=UTC)
    events = ibis.memtable({"_timestamp": [instant], "_unit": ["u"], "account_id_event": ["a"]})
    points = ibis.memtable({"account": ["a"], "cutoff": [instant]})
    history = ibis.memtable({"account_id": ["a"], "starts": [instant], "tier_value": ["gold"]})
    dimension = _dimension(validity=BoundValidity("starts", "ends", None))
    lookups = (
        lambda: enrich_events(events, _source(dimension), {dimension.name: history}),
        lambda: attribute_asof(
            points, history, dimension, "tier", unit_column="account", cutoff_column="cutoff"
        ),
    )
    for lookup in lookups:
        with pytest.raises(TerrineError) as error:
            lookup()
        assert error.value.code == "terrine.semantic.schema_mismatch"
        assert error.value.context["column"] == "ends"


@pytest.mark.parametrize("count", [[], [None], [-1], [0.5], [0, 0], [True]])
def test_history_validation_refuses_malformed_aggregate_counts(connection, count):
    import pyarrow as pa

    history = connection.sql("SELECT NULL::VARCHAR AS account_id, 'gold' AS tier_value")
    with pytest.raises(TerrineError) as error:
        validate_history(
            history, _dimension(), execute=lambda _: pa.table({"__terrine_count": count})
        )
    assert error.value.code == "terrine.temporal.malformed_history"


def test_strict_history_preserves_nanosecond_changes_and_wide_utc_cutoffs(connection):
    history = connection.sql("""
        SELECT 'a' AS account_id, ts::TIMESTAMP_NS AS changed, tier AS tier_value
        FROM (VALUES
            ('1969-12-31 23:59:59.999999999', 'negative'),
            ('2025-01-02 11:59:59.999999999', 'before'),
            ('2025-01-02 12:00:00.000000000', 'at'),
            ('2025-01-02 12:00:00.000000001', 'after')
        ) AS h(ts, tier)
    """)
    points = connection.sql("""
        SELECT request, 'a' AS account, ts::TIMESTAMPTZ AS cutoff
        FROM (VALUES
            (1, '1600-01-01'), (2, '1970-01-01'),
            (3, '2025-01-02 12:00:00+00'), (4, '2500-01-01'), (5, NULL)
        ) AS p(request, ts)
    """)
    result = attribute_asof(
        points,
        history,
        _dimension(validity=BoundValidity(None, None, "changed")),
        "tier",
        unit_column="account",
        cutoff_column="cutoff",
    )
    rows = connection.to_pyarrow(result.order_by("request")).to_pylist()
    assert [(r["__terrine_attribute_value"], r["__terrine_attribute_found"]) for r in rows] == [
        (None, False),
        ("negative", True),
        ("before", True),
        ("after", True),
        (None, False),
    ]
