from __future__ import annotations

import ibis

from terrine.data import Budget, Coverage, UnitSource
from terrine.model import Model
from terrine.spec import Design, EventAnchor, agg, gt


def test_scoring_today_is_resolved_once_at_catalog_day_boundary(
    native_inputs, tmp_path, monkeypatch
):
    catalog, source_con = native_inputs
    con = ibis.duckdb.connect(str(tmp_path / "scoring.duckdb"))
    for name in ("events", "labels", "users"):
        con.create_table(name, source_con.table(name).execute())
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
    )
    design = Design(
        "user",
        EventAnchor("session"),
        features={"past_sessions": agg("sessions", window=("-1d", "0d"))},
        outcomes={"churn": agg("churned", window=("0d", "1d"))},
    )
    from datetime import UTC
    from datetime import datetime as OriginalDateTime

    class FrozenDateTime(OriginalDateTime):
        calls = 0

        @classmethod
        def now(cls, tz=None):
            cls.calls += 1
            return OriginalDateTime(2025, 1, 5, 23, 45, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr("terrine.spec.design.datetime", FrozenDateTime)
    eligibility = {"active": gt(agg("sessions", window=("-1d", "0d")), 0)}

    scoring = design.snapshot(
        model,
        as_of="today",
        eligibility=eligibility,
        features=("past_sessions",),
    )
    assert FrozenDateTime.calls == 1

    assert scoring.roles.outcomes == ()
    assert scoring.roles.features == ("past_sessions",)
    result = scoring.materialize()
    view = result.provenance.dataset_view
    assert view is not None
    assert view.kind == "scoring"
    assert view.selected_features == ("past_sessions",)
    assert view.selected_labels == ()
    assert view.resolved_as_of is not None
    assert view.resolved_as_of.isoformat() == "2025-01-05T00:00:00+00:00"
    assert result.roles.outcomes == ()
    assert isinstance(design.anchor, EventAnchor)
    assert "churn" in design.outcomes
    staged = scoring.stage("scoring", budget=Budget(strict_scan=False))
    assert staged.manifest.recipe.view["kind"] == "scoring"
    assert staged.manifest.recipe.view["features"] == ("past_sessions",)
    assert staged.manifest.recipe.view["as_of"] == view.resolved_as_of.isoformat()
    assert staged.manifest.recipe.view["eligibility"]["active"]
    con.disconnect()
