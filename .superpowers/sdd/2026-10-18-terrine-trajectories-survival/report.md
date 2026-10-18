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
- Task 3 (time-to-event and Kaplan–Meier) remains unstarted. The referenced plan `docs/superpowers/plans/2026-10-18-terrine-trajectories-survival.md` is not present in this checkout; the directory lists only plans dated 2026-10-16.

## Disposition

- Task 2 review remediation is complete; Task 3 remains unstarted because its referenced implementation plan is unavailable in this checkout.
