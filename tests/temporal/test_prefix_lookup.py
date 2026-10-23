from __future__ import annotations

import ibis
import pytest


@pytest.mark.parametrize("strategy", ["native", "union_window"])
def test_strict_lookup_preserves_equal_cutoffs_null_states_and_duplicate_points(strategy):
    from terrine.temporal.lookup import lookup_prefix

    con = ibis.duckdb.connect()
    try:
        prefix = con.sql("""
            SELECT unit AS _unit, ts::TIMESTAMPTZ AS _timestamp,
                   total::BIGNUM AS sum_value, events::BIGNUM AS event_count,
                   nonnulls::BIGNUM AS nonnull_count
            FROM (VALUES
                ('a', '2025-01-01 12:00:00+00', NULL, 1, 0),
                ('a', '2025-01-01 13:00:00+00', '9007199254740993', 2, 1)
            ) AS states(unit, ts, total, events, nonnulls)
        """)
        points = con.sql("""
            SELECT request, unit, cutoff::TIMESTAMPTZ AS cutoff
            FROM (VALUES
                (1, 'a', '2025-01-01 12:00:00+00'),
                (2, 'a', '2025-01-01 12:00:00.000001+00'),
                (2, 'a', '2025-01-01 12:00:00.000001+00'),
                (3, 'a', '2025-01-01 13:00:00+00'),
                (4, 'a', '2025-01-01 13:00:00.000001+00'),
                (5, 'b', '2025-01-01 14:00:00+00')
            ) AS requests(request, unit, cutoff)
        """)
        result = lookup_prefix(
            points, prefix, unit_column="unit", timestamp_column="cutoff", strategy=strategy
        )
        table = result.table
        selected = table.select(
            "request",
            total=table[result.columns["sum_value"]].cast("string"),
            events=table[result.columns["event_count"]].cast("string"),
            found=table[result.matched_column],
        ).order_by("request")
        assert con.to_pyarrow(selected).to_pylist() == [
            {"request": 1, "total": "0", "events": "0", "found": False},
            {"request": 2, "total": None, "events": "1", "found": True},
            {"request": 2, "total": None, "events": "1", "found": True},
            {"request": 3, "total": None, "events": "1", "found": True},
            {"request": 4, "total": "9007199254740993", "events": "2", "found": True},
            {"request": 5, "total": "0", "events": "0", "found": False},
        ]
        later_points = table.mutate(later=table.cutoff + ibis.interval(hours=1))
        later = lookup_prefix(
            later_points,
            prefix,
            unit_column="unit",
            timestamp_column="later",
            strategy=strategy,
        )
        selected = later.table.select(
            "request",
            earlier=later.table[result.columns["event_count"]].cast("string"),
            later=later.table[later.columns["event_count"]].cast("string"),
        ).order_by("request")
        assert con.to_pyarrow(selected).to_pylist() == [
            {"request": 1, "earlier": "0", "later": "1"},
            {"request": 2, "earlier": "1", "later": "2"},
            {"request": 2, "earlier": "1", "later": "2"},
            {"request": 3, "earlier": "1", "later": "2"},
            {"request": 4, "earlier": "2", "later": "2"},
            {"request": 5, "earlier": "0", "later": "0"},
        ]
    finally:
        con.disconnect()


@pytest.mark.parametrize("strategy", ["native", "union_window"])
def test_lookup_aliases_are_collision_safe_and_keep_state_row_coherent(strategy):
    from terrine.temporal.lookup import lookup_prefix

    con = ibis.duckdb.connect()
    try:
        prefix = con.sql("""
            SELECT unit AS _unit, ts::TIMESTAMPTZ AS _timestamp,
                   total::BIGNUM AS sum_value, events::BIGNUM AS event_count,
                   nonnulls::BIGNUM AS nonnull_count,
                   label::VARCHAR AS state_label,
                   flagged::BOOLEAN AS flagged,
                   scale::INTEGER AS value_scale
            FROM (VALUES
                ('a', '2025-01-01 12:00:00+00', NULL, 1, 0, 'old', false, 3),
                ('a', '2025-01-01 13:00:00+00', '9', 2, 1, 'new', true, 7)
            ) AS states(unit, ts, total, events, nonnulls, label, flagged, scale)
        """)
        points = con.sql("""
            SELECT request, unit, cutoff::TIMESTAMPTZ AS cutoff,
                   'left'::VARCHAR AS __terrine_state_unit,
                   17::INTEGER AS __TERRINE_LOOKUP_0
            FROM (VALUES
                (1, 'a', '2025-01-01 12:00:00.000001+00'),
                (2, 'a', '2025-01-01 13:00:00.000001+00'),
                (3, 'b', '2025-01-01 14:00:00+00')
            ) AS requests(request, unit, cutoff)
        """)
        result = lookup_prefix(
            points,
            prefix,
            unit_column="unit",
            timestamp_column="cutoff",
            strategy=strategy,
        )
        table = result.table
        selected = table.select(
            "request",
            "__terrine_state_unit",
            "__TERRINE_LOOKUP_0",
            total=table[result.columns["sum_value"]].cast("string"),
            count=table[result.columns["event_count"]].cast("string"),
            label=table[result.columns["state_label"]],
            flagged=table[result.columns["flagged"]],
            scale=table[result.columns["value_scale"]],
            found=table[result.matched_column],
        ).order_by("request")
        assert con.to_pyarrow(selected).to_pylist() == [
            {
                "request": 1,
                "__terrine_state_unit": "left",
                "__TERRINE_LOOKUP_0": 17,
                "total": None,
                "count": "1",
                "label": "old",
                "flagged": False,
                "scale": 3,
                "found": True,
            },
            {
                "request": 2,
                "__terrine_state_unit": "left",
                "__TERRINE_LOOKUP_0": 17,
                "total": "9",
                "count": "2",
                "label": "new",
                "flagged": True,
                "scale": 7,
                "found": True,
            },
            {
                "request": 3,
                "__terrine_state_unit": "left",
                "__TERRINE_LOOKUP_0": 17,
                "total": "0",
                "count": "0",
                "label": None,
                "flagged": False,
                "scale": None,
                "found": False,
            },
        ]
    finally:
        con.disconnect()


@pytest.mark.parametrize("strategy", ["native", "union_window"])
def test_mixed_nanosecond_state_and_wide_utc_requests_keep_strict_order(strategy):
    from terrine.temporal.lookup import lookup_prefix

    con = ibis.duckdb.connect()
    try:
        prefix = con.sql("""
            SELECT 'a' AS _unit, ts::TIMESTAMP_NS AS _timestamp, n::BIGINT AS event_count,
                   'state-' || n::VARCHAR AS label
            FROM (VALUES
                ('1969-12-31 23:59:59.999999999', 1),
                ('2025-01-02 11:59:59.999999999', 2),
                ('2025-01-02 12:00:00.000000000', 3),
                ('2025-01-02 12:00:00.000000001', 4)
            ) AS s(ts, n)
        """)
        points = con.sql("""
            SELECT request, 'a' AS unit, ts::TIMESTAMPTZ AS cutoff
            FROM (VALUES
                (1, '1600-01-01'), (2, '1970-01-01'),
                (3, '2025-01-02 12:00:00+00'), (4, '2500-01-01'), (5, NULL)
            ) AS p(request, ts)
        """)
        result = lookup_prefix(
            points, prefix, unit_column="unit", timestamp_column="cutoff", strategy=strategy
        )
        table = result.table
        rows = con.to_pyarrow(
            table.select(
                "request",
                n=table[result.columns["event_count"]],
                label=table[result.columns["label"]],
                found=table[result.matched_column],
            ).order_by("request")
        ).to_pylist()
        assert rows == [
            {"request": 1, "n": 0, "label": None, "found": False},
            {"request": 2, "n": 1, "label": "state-1", "found": True},
            {"request": 3, "n": 2, "label": "state-2", "found": True},
            {"request": 4, "n": 4, "label": "state-4", "found": True},
            {"request": 5, "n": 0, "label": None, "found": False},
        ]
    finally:
        con.disconnect()


@pytest.mark.parametrize("dtype", ["TIMESTAMP_S", "TIMESTAMP_MS"])
def test_coarse_timestamp_ordering_refuses_without_lossless_native_extraction(dtype):
    from terrine.errors import TerrineError
    from terrine.temporal.lookup import lookup_prefix

    con = ibis.duckdb.connect()
    try:
        prefix = con.sql("SELECT 'a' AS _unit, TIMESTAMP '2025-01-01' AS _timestamp, 1 AS n")
        points = con.sql(f"SELECT 'a' AS unit, '2500-01-01'::{dtype} AS cutoff")
        with pytest.raises(TerrineError) as error:
            lookup_prefix(points, prefix, unit_column="unit", timestamp_column="cutoff")
        assert error.value.code == "terrine.temporal.timestamp_order_unsupported"
    finally:
        con.disconnect()


@pytest.mark.parametrize("strategy", ["native", "union_window"])
def test_infinite_timestamp_keys_keep_strict_order_and_null_requests(strategy):
    from terrine.temporal.lookup import lookup_prefix

    con = ibis.duckdb.connect()
    try:
        prefix = con.sql("""
            SELECT 'a' AS _unit, ts::TIMESTAMP_NS AS _timestamp, n
            FROM (VALUES ('-infinity', 1), ('1969-12-31 23:59:59.999999999', 2),
                         ('infinity', 3)) AS s(ts, n)
        """)
        points = con.sql("""
            SELECT request, 'a' AS unit, ts::TIMESTAMPTZ AS cutoff
            FROM (VALUES (1, '-infinity'), (2, '1970-01-01'), (3, 'infinity'), (4, NULL))
            AS p(request, ts)
        """)
        result = lookup_prefix(
            points, prefix, unit_column="unit", timestamp_column="cutoff", strategy=strategy
        )
        table = result.table
        rows = con.to_pyarrow(
            table.select("request", n=table[result.columns["n"]]).order_by("request")
        ).to_pylist()
        assert [row["n"] for row in rows] == [0, 2, 2, 0]
    finally:
        con.disconnect()
