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
def test_lookup_aliases_are_collision_safe_and_keep_certificate_row_coherent(strategy):
    from terrine.temporal.lookup import lookup_prefix

    con = ibis.duckdb.connect()
    try:
        prefix = con.sql("""
            SELECT unit AS _unit, ts::TIMESTAMPTZ AS _timestamp,
                   total::BIGNUM AS sum_value, events::BIGNUM AS event_count,
                   nonnulls::BIGNUM AS nonnull_count,
                   cert::VARCHAR AS float64_certificate_id,
                   failed::BOOLEAN AS float64_nonfinite_state,
                   scale::INTEGER AS value_scale
            FROM (VALUES
                ('a', '2025-01-01 12:00:00+00', NULL, 1, 0, 'old', false, 3),
                ('a', '2025-01-01 13:00:00+00', '9', 2, 1, 'new', true, 7)
            ) AS states(unit, ts, total, events, nonnulls, cert, failed, scale)
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
            certificate=table[result.columns["float64_certificate_id"]],
            failed=table[result.columns["float64_nonfinite_state"]],
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
                "certificate": "old",
                "failed": False,
                "scale": 3,
                "found": True,
            },
            {
                "request": 2,
                "__terrine_state_unit": "left",
                "__TERRINE_LOOKUP_0": 17,
                "total": "9",
                "count": "2",
                "certificate": "new",
                "failed": True,
                "scale": 7,
                "found": True,
            },
            {
                "request": 3,
                "__terrine_state_unit": "left",
                "__TERRINE_LOOKUP_0": 17,
                "total": "0",
                "count": "0",
                "certificate": None,
                "failed": False,
                "scale": None,
                "found": False,
            },
        ]
    finally:
        con.disconnect()
