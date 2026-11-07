"""Stable reasons for unavailable or reduced values, censoring and split exclusions.

Use the registry to interpret completeness metadata before choosing an imputation policy.
A reason beside a null value says why it is unavailable; ``below_resolution`` is the
one reason published beside a value, describing what that value omits.
Integrity-check keys and refusal-context reasons are separate vocabularies.

Example: REASONS["incomplete_coverage"]
"""

from collections.abc import Mapping
from types import MappingProxyType

__all__ = ["REASONS"]

ADMINISTRATIVE_CUTOFF = "administrative_cutoff"
ALL_VALUES_NULL = "all_values_null"
BELOW_RESOLUTION = "below_resolution"
COMPLETENESS_EVIDENCE_MISSING = "completeness_evidence_missing"
DEADLINE_ELAPSED = "deadline_elapsed"
EMBARGO = "embargo"
EXIT = "exit"
GROUP_MISSING = "group_missing"
HORIZON_UNAVAILABLE = "horizon_unavailable"
INCOMPLETE_CONTRIBUTION = "incomplete_contribution"
INCOMPLETE_COVERAGE = "incomplete_coverage"
INCOMPLETE_INITIAL_HISTORY = "incomplete_initial_history"
INCOMPLETE_LABEL_HISTORY = "incomplete_label_history"
INCOMPLETE_OBSERVATION = "incomplete_observation"
INCOMPLETE_SELECTOR = "incomplete_selector"
INSUFFICIENT_FOLLOWUP = "insufficient_followup"
KNOWN_NULL = "known_null"
LABEL_OVERLAP = "label_overlap"
LABEL_UNAVAILABLE = "label_unavailable"
MISSING_DELAYED_ENTRY = "missing_delayed_entry"
MISSING_HISTORY = "missing_history"
MISSING_PREDECESSOR = "missing_predecessor"
MISSING_SELECTOR_ORIGIN = "missing_selector_origin"
MISSING_STATE_OBSERVATION = "missing_state_observation"
NO_ELIGIBLE_FOLLOWUP = "no_eligible_followup"
NO_OBSERVATIONS = "no_observations"
NULL_COMPARISON = "null_comparison"
REASON_UNAVAILABLE = "reason_unavailable"
SEQUENCE_WINDOW_END_BEFORE_DEADLINE = "sequence_window_end_before_deadline"
SOURCE_COVERAGE = "source_coverage"
SPLIT_MISMATCH = "split_mismatch"
STATE_VALUE_NULL = "state_value_null"
UNIT_OBSERVATION = "unit_observation"
UNKNOWN = "unknown"
ZERO_DENOMINATOR = "zero_denominator"

REASONS: Mapping[str, str] = MappingProxyType(
    {
        ADMINISTRATIVE_CUTOFF: "Follow-up or a contribution ends at the administrative cutoff.",
        ALL_VALUES_NULL: "Events or an attribute observation exist, but their values are all null.",
        BELOW_RESOLUTION: (
            "An exact float sum discarded at least one nonzero contribution below its"
            " 1e-9 resolution; the published value is the sum of the retained contributions."
        ),
        COMPLETENESS_EVIDENCE_MISSING: "The outcome has no completeness evidence.",
        DEADLINE_ELAPSED: "The next funnel step did not occur before its deadline.",
        EMBARGO: "The row origin falls within the embargo between temporal splits.",
        EXIT: "Follow-up ends at the declared exit event.",
        GROUP_MISSING: "The row has no group key for group-preserving split assignment.",
        HORIZON_UNAVAILABLE: "The retention horizon cannot be determined for this anchor.",
        INCOMPLETE_CONTRIBUTION: "A cumulative value includes an unavailable period contribution.",
        INCOMPLETE_COVERAGE: "Declared source coverage does not contain the required history.",
        INCOMPLETE_INITIAL_HISTORY: "Follow-up history is incomplete at the survival origin.",
        INCOMPLETE_LABEL_HISTORY: "Required event labels have unresolved dimension history.",
        INCOMPLETE_OBSERVATION: "The required history lies outside the unit's observation bounds.",
        INCOMPLETE_SELECTOR: "The selector used as the temporal origin is incomplete.",
        INSUFFICIENT_FOLLOWUP: "The retention anchor has no eligible observable follow-up.",
        KNOWN_NULL: "The event property is observed and its value is explicitly null.",
        LABEL_OVERLAP: "Label support crosses the boundary of its temporal split.",
        LABEL_UNAVAILABLE: "The row has no available label support for split assignment.",
        MISSING_DELAYED_ENTRY: "No qualifying delayed-entry event was found.",
        MISSING_HISTORY: "No usable attribute or dimension history exists at the requested time.",
        MISSING_PREDECESSOR: "The event has no predecessor within its session.",
        MISSING_SELECTOR_ORIGIN: "The event selected as the temporal origin is absent.",
        MISSING_STATE_OBSERVATION: "No state observation exists at the scheduled point.",
        NO_ELIGIBLE_FOLLOWUP: "Follow-up ends no later than its eligible start.",
        NO_OBSERVATIONS: "The window contains no observations for this aggregation.",
        NULL_COMPARISON: "A predicate comparison cannot be resolved because an operand is null.",
        REASON_UNAVAILABLE: "An unavailable outcome has no recorded reason.",
        SEQUENCE_WINDOW_END_BEFORE_DEADLINE: "The sequence ends before the funnel deadline.",
        SOURCE_COVERAGE: "Sequence coverage is incomplete, or source coverage censors follow-up.",
        SPLIT_MISMATCH: "The temporal split differs from the group-preserving split assignment.",
        STATE_VALUE_NULL: "A state observation exists but its state value is null.",
        UNIT_OBSERVATION: "The sequence window is outside the unit's observation bounds.",
        UNKNOWN: "A life-table terminal record has no recorded censoring or incompleteness reason.",
        ZERO_DENOMINATOR: "A ratio's denominator is known to be zero.",
    }
)
