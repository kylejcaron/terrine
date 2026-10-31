# Task 7 report

## Implemented

- Removed timestamp-tie rejection from `EventTime` selector identity checks while retaining null unit/timestamp and configured event-ID uniqueness checks.
- Kept the ambiguity refusal for first/nth `EventAnchor` selection and `EventSequence`, using the existing `terrine.compile.anchor_integrity` code and `check_key="tied_event_timestamps"`. Its context now includes `event`, and the message gives the `event_ids={event: "<id column>"}` route through `Model.from_tables` or `Model.from_catalog`.
- Replaced the selector rejection test with coverage that `occurrence="last"` and `occurrence="nth", n=2` both return the shared timestamp for tied rows and mark both results complete. Added anchor refusal coverage and extended the sequence refusal assertion with its event name.
- Regenerated `docs/reference/refusals.md` and added an unreleased API changelog entry.

## RED evidence

- `uv run pytest tests/compile/test_columns.py::test_event_time_selector_admits_tied_timestamps -q` failed before implementation with `terrine.compile.anchor_integrity`, `check_key='tied_event_timestamps'`.
- `uv run pytest tests/compile/test_anchors.py::test_first_event_anchor_refuses_tied_timestamps_without_event_ids tests/compile/test_event_sequence.py::test_timestamp_identity_refuses_ties_without_configured_id -q` failed before implementation because both errors lacked the required `event` context key.

## GREEN evidence

- `make check TESTS="tests/compile"` — passed: ruff, vulture, pydoclint, tach, ty, and **305 passed** tests.
- `eval "$(uv run python -m tests.integration.postgres_scratch start)" && make test-postgres` — passed: 79 PostgreSQL probes and 9 generic-dialect conformance tests passed. Scratch server stopped with `uv run python -m tests.integration.postgres_scratch stop`.
- `uv run python tests/documentation.py` — completed: documented 175 codes from 270 declarations.
- A separate direct focused post-change test run timed out after 300 seconds waiting for shared agent-slots. The full compile selection above includes the three affected tests.

## Files changed

- `terrine/compile/columns.py`
- `terrine/compile/anchors.py`
- `terrine/compile/shapes/event_sequence.py`
- `tests/compile/test_columns.py`
- `tests/compile/test_anchors.py`
- `tests/compile/test_event_sequence.py`
- `docs/reference/refusals.md`
- `CHANGELOG.md`

## Deviations and self-review

No scope deviations. Tests use the existing compact fixtures; selector assertions derive the expected instant from the fixture data. First/nth anchor and sequence identity checks remain scoped to only places where occurrence order affects the result. The `event_name` argument on the sequence identity helper defaults to an empty string solely for the existing internal configured-identity test call; production sequence compilation always supplies the declared event name.

## Concerns

The first `make test-postgres` attempt was run without carrying the scratch server's exported environment into the same shell and failed because PostgreSQL settings were missing. The required combined start-and-test invocation then passed. The focused test command's first post-change attempt encountered a transient format-string error; after escaping the literal mapping braces, coverage passed in the compile-wide check. The additional targeted rerun later timed out after 300 seconds waiting for external shared agent-slot contention.
