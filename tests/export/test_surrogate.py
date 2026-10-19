from terrine.export import ArrowSurrogate, to_surrogate_arrow


def test_surrogate_arrow_round_trip_preserves_selected_columns_and_provenance(frame):
    exported = to_surrogate_arrow(frame, features=("x",), outcomes=("y",))
    restored = ArrowSurrogate.from_arrow(exported.to_arrow())

    assert restored.data.equals(exported.data)
    assert restored.metadata == exported.metadata
    assert {"id", "anchor", "t0", "x", "y", "complete", "reason"} <= set(
        restored.data.column_names
    )
    assert restored.metadata["feature_columns"] == ["x"]
    assert restored.metadata["outcome_columns"] == ["y"]
