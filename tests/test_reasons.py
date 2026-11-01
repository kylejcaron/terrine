from typing import cast

import pytest


def test_reason_registry_defines_stable_tokens_and_meanings():
    from terrine import reasons

    expected = {
        "administrative_cutoff",
        "all_values_null",
        "completeness_evidence_missing",
        "deadline_elapsed",
        "embargo",
        "exit",
        "group_missing",
        "horizon_unavailable",
        "incomplete_contribution",
        "incomplete_coverage",
        "incomplete_initial_history",
        "incomplete_label_history",
        "incomplete_observation",
        "incomplete_selector",
        "insufficient_followup",
        "known_null",
        "label_overlap",
        "label_unavailable",
        "missing_delayed_entry",
        "missing_history",
        "missing_predecessor",
        "missing_selector_origin",
        "missing_state_observation",
        "no_eligible_followup",
        "sequence_window_end_before_deadline",
        "no_observations",
        "null_comparison",
        "reason_unavailable",
        "source_coverage",
        "split_mismatch",
        "state_value_null",
        "unit_observation",
        "unknown",
        "zero_denominator",
    }
    assert set(reasons.REASONS) == expected
    assert reasons.__all__ == ["REASONS"]
    for token, meaning in reasons.REASONS.items():
        assert getattr(reasons, token.upper()) == token
        assert meaning.strip() and "\n" not in meaning
    with pytest.raises(TypeError):
        cast("dict[str, str]", reasons.REASONS)["invented"] = "Unavailable."
