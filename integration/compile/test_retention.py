import pytest

from terrine import Design, retention
from terrine.spec import EventAnchor, EventPanel, Schedule
from tests.compile.test_retention import retention_model as retention_model  # noqa: F401


@pytest.mark.parametrize("shape", ["schedule", "event_panel"])
def test_retention_composes_with_bounded_repeated_shapes(retention_model, shape):
    from terrine.compile.dispatch import compile_design

    expression = retention(
        "occurrence",
        label_property="event_name",
        target_event="target",
        thresholds=(1, 3),
        horizon="1d",
    )
    if shape == "schedule":
        design = Design(
            "user", Schedule("1d", "2025-01-02", "2025-01-03"), outcomes={"retained": expression}
        )
    else:
        design = EventPanel(
            Design("user", EventAnchor("occurrence", "first"), outcomes={"retained": expression}),
            periods=(0, 1),
            period_seconds=86400,
            reference_period=0,
        )
    result = compile_design(design, retention_model)
    result.validate(execute=retention_model.con.to_pyarrow)
    rows = retention_model.con.to_pyarrow(result.table).to_pylist()
    assert rows
    assert all(row["retained"] in (None, 0, 1) for row in rows)
