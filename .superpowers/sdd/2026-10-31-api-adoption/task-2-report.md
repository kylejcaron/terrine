# Task 2 report: duration strings; refuse bare integers

## Implemented

- Added public `terrine.windows.duration(value: str | Literal[0], *, field: str) -> int` admission and `seconds_text(n: int) -> str` conversion helpers, retaining `seconds(value: str | int, *, field: str) -> int` for already-normalized integer metadata. Public non-zero integer durations and booleans now refuse with `terrine.window.invalid_duration` and `reason="bare_integer"`; bare integer zero remains accepted.
- Changed duration-bearing public constructors, helpers, and recipe signatures to string inputs (plus literal zero), and switched public inputs to `duration`.
- Updated saved-record decoders and internal cloning/reconstruction sites to serialize normalized seconds with `seconds_text`; preserved integer seconds in normalized internal records/metadata, including `AttributeAsOf` and `FeatureCutoff`.
- Added refusal, conversion, compatibility, saved-document round-trip, and Arrow-metadata tests; updated callers/tests that supplied integer durations. Updated guide text, regenerated the refusal reference, and added the unreleased changelog entry.

## RED / GREEN evidence

- RED: `uv run pytest tests/test_windows.py::test_bare_integer_durations_refuse_except_zero tests/test_windows.py::test_saved_integer_second_documents_still_decode -q` — the bare-integer test failed because the expected refusal was not raised; saved-document compatibility test passed.
- GREEN: focused duration tests — `uv run pytest tests/test_windows.py tests/spec/test_recipes.py tests/compile/test_nonadditive.py::test_nonzero_feature_cutoff_survives_build_and_arrow_metadata -q` — 86 passed.
- GREEN: `uv run pytest tests -q` — 1978 passed, 2623 warnings (33.80s).
- GREEN: `make check TESTS="tests/test_windows.py tests/spec tests/agent tests/compile/test_event_sequence.py tests/test_metadata.py"` — all lint, vulture, docstring, dependency, type-check and selected-test gates passed; selected pytest tier reported 455 passed, 277 warnings (5.34s).
- GREEN: `uv run python tests/documentation.py` — documented 175 codes from 268 declarations; `uv run --group docs python -m tests.documentation --build` completed successfully.
- GREEN: `for e in examples/*.py; do uv run python "$e"; done` — all example scripts completed successfully.

## Files changed (47)

- Product/API/docs: `CHANGELOG.md`, `docs/guides/design-specs.md`, `docs/guides/temporal-engine.md`, `docs/reference/refusals.md`, `tach.toml`, `terrine/windows.py`, `terrine/data.py`, `terrine/recipes.py`, `terrine/spec/anchors.py`, `terrine/spec/describe.py`, `terrine/spec/design.py`, `terrine/spec/expressions.py`, `terrine/spec/io.py`, `terrine/spec/panels.py`, `terrine/spec/sequence.py`, `terrine/spec/state_panel.py`, `terrine/spec/trajectory.py`, `terrine/spec/validate.py`, `terrine/temporal/retention.py`, `terrine/temporal/sequence.py`, `terrine/compile/dispatch.py`, `terrine/compile/layers.py`, `terrine/compile/shapes/_observations.py`, `terrine/compile/shapes/calendar_comparison.py`, `terrine/compile/shapes/event_sequence.py`, `terrine/compile/shapes/ltv.py`, `terrine/compile/shapes/state_panel.py`, `terrine/compile/shapes/time_to_event.py`.
- Tests: `integration/temporal/test_window_quotient_composition.py`, `tests/agent/test_plan_authorization.py`, `tests/agent/test_schemas.py`, `tests/compile/test_event_sequence.py`, `tests/compile/test_ltv.py`, `tests/compile/test_nonadditive.py`, `tests/export/conftest.py`, `tests/fixtures/baseline.py`, `tests/fixtures/test_baseline.py`, `tests/spec/test_design_io.py`, `tests/spec/test_panels.py`, `tests/spec/test_recipes.py`, `tests/spec/test_retention.py`, `tests/spec/test_sequence.py`, `tests/spec/test_validate.py`, `tests/temporal/test_range_scaling.py`, `tests/temporal/test_window_aggregates.py`, `tests/test_metadata.py`, `tests/test_windows.py`.

## Deviations from brief

- Updated `tach.toml` to allow the compile core and dispatch modules to depend on `terrine.windows`: compiler reconstruction paths need the canonical `seconds_text` formatter, and the dependency checker explicitly models these module boundaries.
- Updated additional integer-duration callers revealed by the full suite and corrected the internal `AttributeAsOf` decoder to retain its normalized integer seconds, rather than converting it to a public duration string.
- Adjusted integration tests that directly construct public `Window` objects to use equivalent duration strings. Tests intentionally asserting refusal of invalid integer input use explicit `Any` casts so static typing continues to check the new public contract without weakening production annotations.

## Self-review and concerns

- Reviewed public admission versus normalized metadata paths: only user-facing duration paths reject nonzero integers; serialization remains integer-second compatible, and internal `seconds(...)` validation continues to accept normalized seconds.
- Coverage includes bool rejection, bare zero acceptance, prescribed recipe duration fields, decoder/clone paths, nonzero feature cutoff metadata round-trip, and examples.
- No outstanding concerns. Full suite emitted existing deprecation and warning output; all checks completed successfully.
