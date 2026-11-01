from types import SimpleNamespace
from typing import Any, cast

import pandas as pd
import pyarrow as pa

import terrine as tr
from terrine.data import MaterializedFrame, Roles
from terrine.errors import FrozenMap
from terrine.windows import Duration, Months, Window
from tests.fixtures.readme import readme_design, readme_model
from tests.test_metadata import _provenance


def test_model_and_frame_reprs_name_contents_without_addresses():
    model = readme_model()
    model_repr = repr(model)
    assert all(name in model_repr for name in ("customer", "orders", "spend"))

    frame_repr = repr(readme_design().build(model))
    assert all(name in frame_repr for name in ("unit", "spend_30d", "orders_30d"))
    assert "0x" not in frame_repr
    assert "columns=" in frame_repr


def test_training_frame_repr_reflects_selected_features_and_outcomes():
    frame = readme_design().build(readme_model())
    training = frame.training_set(
        label="spend_next_30d",
        features=("spend_30d",),
    )

    result = repr(training)
    assert "spend_30d" in result
    assert "orders_30d" not in result
    assert "outcomes=('spend_next_30d',)" in result


def test_model_repr_separates_event_names_from_event_labels():
    from datetime import datetime

    model = tr.Model.from_tables(
        units={"customer": tr.UnitSource(pa.table({"id": ["a"]}), "id")},
        events={
            "orders": tr.Events(
                pa.table(
                    {
                        "id": ["a"],
                        "time": [datetime(2025, 1, 1)],
                        "channel": ["web"],
                    }
                ),
                time="time",
                key="id",
                labels=("channel",),
                metrics={"orders": tr.count()},
                complete=("2025-01-01", "2025-01-02"),
            )
        },
    )

    result = repr(model)
    assert "events=('orders',)" in result
    assert "history=()" in result
    assert "channel" not in result.split("history=", 1)[1].split(",", 1)[0]


def test_dataset_repr_names_generation_and_column_count_without_addresses():
    from terrine.dataset.reference import Dataset

    dataset = cast(Any, Dataset.__new__(Dataset))
    dataset._manifest = SimpleNamespace(
        generation=SimpleNamespace(dataset_id="customer_spend", generation_id="g-1"),
        columns=(object(), object(), object()),
    )
    result = repr(dataset)
    assert "customer_spend" in result
    assert "g-1" in result
    assert "columns=3" in result
    assert "0x" not in result


def test_large_descriptor_and_materialized_frame_reprs_are_bounded():
    table = pd.DataFrame({"k": [str(i) for i in range(10_000)], "t": pd.Timestamp("2025-01-01")})
    event = tr.Events(table, time="t", key="k", complete=("2025-01-01", "2025-01-02"))
    assert len(repr(event)) < 400

    materialized = MaterializedFrame(
        pa.table({"unit": [str(i) for i in range(10_000)], "value": list(range(10_000))}),
        Roles("unit", "anchor", "t0", None, ("value",), ()),
        {},
        _provenance(),
    )
    result = repr(materialized)
    assert len(result) < 400
    assert "rows=10000" in result
    assert "value" in result
    assert "provenance" not in result


def test_frozen_map_window_and_duration_reprs_show_values():
    assert "incomplete_coverage" in repr(FrozenMap({"reason": "incomplete_coverage"}))
    assert repr(Window("-30d", "0s")) == "Window('-30d', '0s')"
    assert repr(Duration(604_800)) == "Duration('1w')"
    assert repr(Months(12)) == "Months(12)"
