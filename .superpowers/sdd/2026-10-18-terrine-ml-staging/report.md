# ML staging implementation report

## Commits

- `4ced1f3` — `feat: define reproducible dataset manifests` (review 8772 findings fixed and closed in follow-up).
- `ef16f51` — `fix: preserve dataset recipe metadata on reopen` (review 8774 passed).
- `53b6f9c` — `feat: add deterministic leakage-safe splits` (review 8776 findings fixed and closed in follow-up).
- `0e3b809` — `fix: preserve split support and boundary semantics` (review 8780 finding fixed and closed in follow-up).
- `77dd22a` — `fix: defer mixed split boundary ordering until bind` (review 8782 passed and closed).
- `07f1858` — `feat: support owner-bound immutable recipe references`.
- `71306c4` — `feat: wire lazy training views and label completeness`.
- `7759942` — `fix: clean recipe temp files after write failures`.
- `bb50224` — `fix: collect duplicate test module names safely`.
- `22e1580` — `fix: harden resumable dataset exports` (review 8850 findings fixed; shared refusal codes moved to `terrine.data` to avoid the forbidden dataset-to-compiler dependency).
- `7a7fc93` — `fix: preserve scoring snapshot identity` (review 8851 findings fixed).

## Acceptance coverage

- Task 3 implementation remains partial: `Frame.stage` and `Dataset.from_manifest` produce generation-pinned, checksum-verified Dataset references from a committed publication, with bounded Arrow reads and strict output/row limits. It stages directly through `DuckDBBackend`; it does not use the runtime's `SavedPlanRecipe`/serialized execution path or persist/replay a stage recipe. A backend retry compares incoming bounded Arrow content with the existing generation.
- Task 4 implementation remains partial: Parquet export consumes batches with one active writer, records shard checksums and export metadata, enforces budget limits, locks destinations, and recovers verified checkpoints while discarding unfinished shards. It does not provide staging's missing runtime integration. No framework-specific adapters were added.
- Task 5 remains unimplemented.

## Verification

- `uv run ruff format terrine tests examples` — PASS.
- `uv run ruff check terrine tests examples` — PASS.
- `uv run ty check terrine tests` — PASS.
- `uv run tach check` — PASS.
- `uv run pytest -q tests/dataset/test_export.py` — PASS (2 passed; 59 warnings).
- `uv run pytest -q -n 8 tests/dataset tests/runtime tests/spec` — PASS (315 passed, 449 warnings).

## Review dispositions

- Reviews 8772/8774/8776/8780/8782 were closed; prior report records their fixes and dispositions.
- Review 8790 identified non-atomic writes and unnormalized recipe-reference failures; fixed with atomic publication, coded missing-object refusals, and regression tests; review closed.
- Review 8798 identified temporary-file leakage on I/O failure; fixed with cleanup covering write/flush/fsync/close and an injected-failure test; review closed.
- Review 8801 remains unreconciled and open.
- Review 8850: fixed destination concurrency, incomplete-shard recovery, initial checkpoint recovery, and refusal-code consistency; closed with `--commenter roborev-fix`.
- Review 8851: fixed relation descriptor serialization, timestamp precision and UTC normalization, anchor-independent feature fingerprints, and persisted parent-design identity; closed with `--commenter roborev-fix`.
- Review 8866 identified a stale recovery-test expectation after active-shard cleanup; replaced it with an interruption/rewrite regression and closed with `--commenter roborev-fix`.
- Review 8868 passed the scoring-identity commit.

## Remaining

- Task 3 still requires routing staging through `SavedPlanRecipe` and the serialized runtime execution path, persisting/replaying the stage recipe, broader ownership/reopen tests, and reconciliation of review 8801.
- Task 4 still requires extending resumability through the runtime staging/publication boundary; framework adapters remain optional and were not added.
- Task 5 remains unimplemented.
