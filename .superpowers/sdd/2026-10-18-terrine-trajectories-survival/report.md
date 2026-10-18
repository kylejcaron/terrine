# Trajectories and survival implementation report

## Task 1 — first-occurrence temporal aggregate

- Original commit: `0505e71 feat: add first occurrence temporal aggregate`.
- Red: `uv run pytest tests/temporal/test_first_occurrence.py -q` failed because `terrine.temporal.first` did not exist.
- Green before review: focused temporal tests passed (4 tests); formatting, Ruff, ty, and tach passed.
- Completed review fixes in commits `c914efc` and `6f8f0fa`. Focused temporal tests pass (8 tests); the four findings from job `8464` were fixed, commented on, and closed. Follow-up review `8475` found collision and null-bound cases; both were fixed with regressions, and follow-up review `8482` passed. Jobs `8475` and `8482` were commented on and closed.

## Task 2 — review remediation

- Added and retained regressions for all eight findings: generated-name collision, exact cohort mean, outcome numeric policy, feature/treatment support metadata, cutoff error certificates, cumulative float tolerance, signed bucket arithmetic across the Unix epoch, and `TYPE_CHECKING` public exports.
- Cumulative floating-point error now includes input certificates plus a conservative reduction-rounding bound, is checked against the final-result tolerance, and the deferred check is registered on the compiled study.
- Commits: `a85d8d3`, `adbff22`, and `87fc7d0` (cumulative certification). Reviews `8507` and `8509` passed; job `8493` was commented with final dispositions and closed.
- Focused `uv run pytest tests/compile/test_ltv.py tests/compile/test_cohorts.py -q`: 14 passed. `uv run ruff format terrine tests examples`, `uv run ruff check terrine tests examples`, `uv run ty check terrine tests`, and `uv run tach check` passed.
- Task 3 is implemented in `2b907b6 feat: add time to event and Kaplan Meier`. Added the serializable `TimeToEvent` shape, single-entry design restrictions, lazy event/exit/entry lookup over coverage and unit bounds, prevalent-event exclusion, cutoff handling, incomplete-history reason, and an Arrow-only Kaplan–Meier estimator. Elapsed timestamps are converted relative to each row's `t0` with integer Arrow arithmetic; KM handles event/censor ties, time-zero events, delayed entry and Greenwood variance.
- Original Task 3 tests: `uv run pytest tests/compile/test_time_to_event.py tests/estimate/test_survival.py tests/spec/test_design_io.py -q` — 22 passed. Review job `8532` found ten valid issues; the fixes are in progress. Focused regression tests now pass (25 tests), as do formatting, `ruff check terrine tests examples`, `ty check terrine tests`, and `tach check`.
- The review fixes cover same-source event-ID projection, timestamp precision widening, delayed-entry observation bounds, prior-event lookup before coverage, reversed follow-up exclusion, Arrow scalar timestamp handling, generated-name collision guards, metadata/support preservation, persistent Greenwood unavailability after survival reaches zero, and an O(N log N) risk-set sweep. Commit and roborev-fix disposition for these fixes remain pending.

## Disposition

- Tasks 1–3 are implemented; Task 3 review closure remains pending.
- Task 4 (earlier-only backtests, surrogate Arrow export, integration/example workflow) remains.
