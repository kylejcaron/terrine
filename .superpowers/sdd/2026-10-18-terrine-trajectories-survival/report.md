# Trajectories and survival implementation report

## Task 1 — first-occurrence temporal aggregate

- Original commit: `0505e71 feat: add first occurrence temporal aggregate`.
- Red: `uv run pytest tests/temporal/test_first_occurrence.py -q` failed because `terrine.temporal.first` did not exist.
- Green before review: focused temporal tests passed (4 tests); formatting, Ruff, ty, and tach passed.
- Completed review fixes in commits `c914efc` and `6f8f0fa`. Focused temporal tests pass (8 tests); the four findings from job `8464` were fixed, commented on, and closed. Follow-up review `8475` found collision and null-bound cases; both were fixed with regressions, and follow-up review `8482` passed. Jobs `8475` and `8482` were commented on and closed.

## Task 2 — review remediation in progress

- Added and retained regressions covering trajectory name collision, feature support metadata, float certificate cutoff, exact cohort mean, repeated anchors/missing periods, fresh snapshots, bucket boundaries, and cohort denominators.
- Fixed generated trajectory output-name reservation, support-column projection for features/treatment, outcome-only numeric policy selection, cutoff nulling of the period error certificate, epoch-safe signed bucket normalization, and restored `Budget`, `Coverage`, and `UnitSource` under `TYPE_CHECKING`.
- Focused `uv run pytest tests/compile/test_ltv.py tests/compile/test_cohorts.py -q`: 12 passed. `uv run ruff format terrine tests examples` and `uv run ruff check terrine tests examples`, `uv run ty check terrine tests`, `uv run tach check` passed after formatting fixes.
- Still outstanding: cumulative floating-point error/tolerance certification (including compile checks), an explicit epoch-crossing regression, commit/review cycles and job `8493` disposition/closure. Task 3 has not started.

## Disposition

- Task 2 review remediation and Task 3 are incomplete; job `8493` remains open.
