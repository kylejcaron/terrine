"""Tiny executable counterparts of the library hand-off guide."""

from dataclasses import replace

import ibis
import pytest

from terrine.export import preflight_export, preflight_warehouse

pytestmark = pytest.mark.handoffs


def test_doubleml_repeated_rows_keep_cluster_identity(handoff_frame, library):
    doubleml = library("doubleml")
    data = replace(
        handoff_frame,
        table=handoff_frame.table.take([i // 2 for i in range(24)]),
        roles=replace(handoff_frame.roles, cluster_unit="unit"),
    )
    checked = preflight_export(data)
    roles = data.roles
    consumer = doubleml.DoubleMLData(
        checked.table.to_pandas(),
        y_col=roles.outcomes[0],
        d_cols=roles.treatment,
        x_cols=list(roles.features),
        cluster_cols=roles.cluster_unit,
    )
    assert consumer.n_obs == 24
    assert consumer.cluster_cols == ["unit"]
    assert consumer.cluster_vars[:, 0].tolist() == [i // 2 for i in range(24)]
    assert consumer.x.shape == (24, 1)


def test_dowhy_one_row_per_unit_identifies_declared_graph(handoff_frame, library):
    dowhy = library("dowhy")
    checked = preflight_export(handoff_frame, consumer="dowhy")
    roles = handoff_frame.roles
    model = dowhy.CausalModel(
        data=checked.table.select([roles.treatment, *roles.features, *roles.outcomes]).to_pandas(),
        treatment=roles.treatment,
        outcome=roles.outcomes[0],
        common_causes=list(roles.features),
    )
    identified = model.identify_effect()
    assert identified.treatment_variable == ["treatment"]
    assert identified.outcome_variable == ["y"]


def test_duckreg_consumes_same_snapshot_and_compressed_statistics(handoff_frame, library):
    duckreg = library("duckreg")
    con = ibis.duckdb.connect()
    try:
        # Seed source rows into the warehouse; no dataset rows are collected below.
        table = con.create_table("materialized_frame", handoff_frame.table)
        with preflight_warehouse(
            table,
            roles=handoff_frame.roles,
            columns=handoff_frame.columns,
            provenance=handoff_frame.provenance,
        ) as checked:
            con.create_view("checked_frame", checked.table)
            model = duckreg.DuckRegression(
                db_name=None,
                connection=con,
                table_name="checked_frame",
                formula="y ~ treatment + x",
                cluster_col="unit",
                seed=42,
                n_bootstraps=0,
            )
            model.fit()
            assert model.df_compressed["count"].sum() == 12
            assert len(model.df_compressed) == 4
            assert set(model.df_compressed.columns) == {
                "treatment",
                "x",
                "count",
                "sum_y",
                "sum_y_sq",
                "mean_y",
            }
            assert con.table("materialized_frame").count().execute() == 12
    finally:
        con.disconnect()
