from datetime import UTC, datetime
from decimal import Decimal

import ibis
import pyarrow as pa
import pytest

from terrine.compile.anchors import AnchorTable
from terrine.compile.columns import CompiledStudy
from terrine.compile.shapes.cohorts import compile_cohorts
from terrine.data import ColumnMeta, Roles
from terrine.errors import TerrineError
from terrine.spec.trajectory import CohortGrouping

_INTEGER = pa.int64()


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


def _cohorts(
    con,
    values,
    *,
    dtype=_INTEGER,
    statistic="mean",
    units=None,
    ages=None,
    times=None,
    complete=None,
    mature=None,
    bucket="7d",
    origin=datetime(2025, 1, 1, tzinfo=UTC),
):
    count = len(values)
    table = con.create_table(
        "trajectory",
        pa.table(
            {
                "unit": units or [str(index) for index in range(count)],
                "age": ages or [0] * count,
                "cohort_time": times or [datetime(2025, 1, 1, tzinfo=UTC)] * count,
                "period_value": pa.array(values, type=dtype),
                "period_value__complete": complete or [True] * count,
                "period_value_mature": mature or [True] * count,
                "period_value__reason": pa.array(
                    [None if value is not None else "incomplete_observation" for value in values],
                    type=pa.string(),
                ),
            }
        ),
        overwrite=True,
    )
    study = CompiledStudy(
        table=table,
        audit_table=table,
        roles=Roles("unit", "unit", "cohort_time", None, (), ("period_value",)),
        columns={
            "period_value": ColumnMeta(
                None,
                "outcome",
                None,
                None,
                "period_value__complete",
                "period_value__reason",
            )
        },
        feature_cutoffs={},
        eligibility_cutoffs={},
        strategy={},
        _anchors=AnchorTable(table, ()),
        _checks=(),
    )
    result = compile_cohorts(
        study,
        grouping=CohortGrouping(bucket=bucket, bucket_origin=origin, statistic=statistic),
        value_column="period_value",
    )
    result.validate(execute=con.to_pyarrow)
    return result, con.to_pyarrow(result.table).to_pylist()


def test_cohort_grouping_bucket_statistic_and_denominators_match_hand_calculation(con):
    _, rows = _cohorts(
        con,
        [10, 0, 15, 0, 15, 0, 0, None, None],
        units=["a", "b"] * 3 + ["c"] * 3,
        ages=[0, 0, 1, 1, 2, 2, 0, 1, 2],
        times=[datetime(2025, 1, 1, tzinfo=UTC)] * 6 + [datetime(2025, 1, 15, tzinfo=UTC)] * 3,
        complete=[True] * 7 + [False, False],
        mature=[True] * 7 + [False, False],
    )
    assert [
        (row["bucket_index"], row["age"], row["value"], row["at_risk"], row["observable"])
        for row in sorted(rows, key=lambda row: (row["bucket_index"], row["age"]))
    ] == [
        (0, 0, 5.0, 2, 2),
        (0, 1, 7.5, 2, 2),
        (0, 2, 7.5, 2, 2),
        (2, 0, 0.0, 1, 1),
        (2, 1, None, None, None),
        (2, 2, None, None, None),
    ]


def test_bucket_index_respects_subsecond_origin_boundaries(con):
    _, rows = _cohorts(
        con,
        [0],
        times=[datetime(2025, 1, 15, tzinfo=UTC)],
        origin=datetime(2025, 1, 1, microsecond=500_000, tzinfo=UTC),
    )
    assert rows[0]["bucket_index"] == 1


def test_bucket_index_normalizes_timestamp_remainders_across_unix_epoch(con):
    _, rows = _cohorts(
        con,
        [0],
        times=[datetime(1970, 1, 1, microsecond=900_000, tzinfo=UTC)],
        origin=datetime(1969, 12, 31, 23, 59, 59, 100_000, tzinfo=UTC),
        bucket="1s",
    )
    assert rows[0]["bucket_index"] == 1


def test_zero_partial_and_unavailable_cohort_denominators(con):
    _, rows = _cohorts(
        con,
        [0, None, None],
        units=["young"] * 3,
        ages=[0, 1, 2],
        complete=[True, False, False],
        mature=[True, True, False],
    )
    by_age = {row["age"]: row for row in rows}
    assert [
        (by_age[age]["value"], by_age[age]["at_risk"], by_age[age]["observable"]) for age in (0, 1)
    ] == [
        (0.0, 1, 1),
        (None, 1, 0),
    ]
    assert (by_age[2]["at_risk"], by_age[2]["observable"]) == (None, None)
    for name in ("at_risk", "observable"):
        assert [by_age[age][f"{name}__complete"] for age in (0, 1, 2)] == [True, True, False]
        assert by_age[2][f"{name}__reason"] == "administrative_cutoff"


def test_exact_integer_values_are_summed_before_cohort_mean_conversion(con):
    _, rows = _cohorts(con, [9007199254740993, -9007199254740992])
    assert rows[0]["value"] == 0.5


def test_cohort_mean_preserves_fractional_decimal_and_float_values(con):
    for dtype in (pa.decimal128(18, 2), pa.float64()):
        values = [Decimal("0.25")] * 2 if pa.types.is_decimal(dtype) else [0.25] * 2
        _, rows = _cohorts(con, values, dtype=dtype)
        assert rows[0]["value"] == 0.25


def test_high_scale_decimal_cohort_mean_does_not_overflow_intermediate_sum(con):
    _, rows = _cohorts(con, [Decimal(90_000_000)] * 2, dtype=pa.decimal128(38, 30))
    assert rows[0]["value"] == 90_000_000


def test_high_scale_decimal_cohort_total_accumulates_cancellation_exactly(con):
    _, rows = _cohorts(
        con,
        [Decimal(90_000_000), Decimal(90_000_000), Decimal(-90_000_000)],
        dtype=pa.decimal128(38, 30),
        statistic="total",
    )
    assert rows[0]["value"] == 90_000_000


def test_high_scale_decimal_cohort_total_refuses_unrepresentable_result(con):
    with pytest.raises(TerrineError) as error:
        _cohorts(
            con,
            [Decimal(90_000_000)] * 2,
            dtype=pa.decimal128(38, 30),
            statistic="total",
        )
    assert error.value.code == "terrine.temporal.exact_overflow"


def test_float_cohort_mean_of_huge_values_avoids_overflow(con):
    result, rows = _cohorts(con, [1e308, 1e308], dtype=pa.float64())
    assert rows[0]["value"] == 1e308
    meta = result.columns["value"]
    assert meta.numeric_policy is None


def test_large_zero_float_cohort_mean_remains_available(con):
    _, rows = _cohorts(con, [0.0] * 2050, dtype=pa.float64())
    assert rows[0]["value"] == 0.0
    assert rows[0]["value__complete"] is True


def test_float_cohort_total_overflow_refuses(con):
    with pytest.raises(TerrineError) as error:
        _cohorts(con, [1e308, 1e308], dtype=pa.float64(), statistic="total")
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == "bound_overflow"
