# Contributing to terrine

Thank you for your interest in contributing.

If you do not have write access, fork the repository, create a branch in your
fork, and open a pull request into `main`. A pull request can merge only when CI
and the CLA check pass.

Development commands, test tiers and API rules are in the "Contributor
guidelines" section of [AGENTS.md](AGENTS.md).

## Development setup and checks

Install [uv](https://docs.astral.sh/uv/) and sync the checkout:

```sh
uv sync --group dev --group docs --all-extras
make check
```

`uv sync` installs the current checkout, not a released distribution.
`make check` runs static checks and the ordinary unit tier (`tests/`).
Use `make check TESTS="tests/test_windows.py"` for focused checks, or
`make test TESTS="tests/test_windows.py"` for tests alone.

Tests run in three tiers:

| Tier | Command | When it runs |
| --- | --- | --- |
| Unit | `uv run pytest tests` | Every local check and push |
| Integration | `uv run pytest -q integration` | Every push and before merges |
| Scale | `make test-scale` | Scheduled and on demand; deselected by ordinary runs |

The `lint`, `typecheck` and `fmt` targets run Ruff/Vulture/pydoclint/Tach, ty,
and Ruff formatting respectively over the package, `tests/` and `integration/`.
`make complexity` is informational, not part of the gate.
Keep tests at the cheapest boundary that protects consumer-visible behavior;
see [AGENTS.md](AGENTS.md#tests-and-validation) for the complete rules.

Check in-memory conversion and registration against descriptor Arrow tables
or registered relations in `tests/semantic/test_from_tables.py`. Keep the
named-table and one in-memory README study in `integration/test_model_from_tables.py`.
Batch exact cast inputs at the numeric boundary, but keep published value and
reason checks through window aggregation. Only batch quotient cases that share
physical operand types; keep a separate oracle for every case.

Documentation checks:

```sh
uv run python examples/first_dataset.py
uv run pytest -q tests/test_documentation.py
uv run python -m tests.documentation --build
```

## Releasing

Before tagging a release:

1. Confirm CI is green for the release commit. The release workflow requires the
   `ci-ok` job for every release tag. Non-development 1.x and later release
   candidates and final tags also require successful PostgreSQL and scale jobs
   on that commit. Dispatch both workflows against a ref containing the release
   commit, then wait for those jobs to succeed before tagging:

   ```sh
   gh workflow run warehouse-backends --ref <ref> -f backend_mode=postgres
   gh workflow run scale --ref <ref>
   ```

   If the CI push run was deferred or never ran in full, run
   `gh workflow run CI --ref <ref>` and wait for `ci-ok`. Every release also
   needs [BigQuery and Snowflake evidence](#cloud-evidence-and-waivers).
2. Run the stable-API compatibility check:

   ```sh
   uv run python scripts/api_check.py
   ```
3. Write the stable-surface snapshot for each release tag (`v0.Y.0`, a
   release candidate, or a final 1.x release), and include it in the tagged
   commit:

   ```sh
   uv run python scripts/api_check.py --snapshot tests/artifacts/surface/<version>.txt
   ```

   Keep earlier snapshots; CI compares against the newest version. Snapshot
   differences are informational during 0.x and enforced from `1.0.0rc1`.
   Confirm CI is green for the release commit after adding the snapshot, before
   tagging.
4. For a saved-format change, build a wheel and run the candidate upgrade lane
   before tagging:

   ```sh
   build_dir=$(mktemp -d)
   uv build --wheel --out-dir "$build_dir"
   wheel=$(printf '%s\n' "$build_dir"/*.whl)
   TERRINE_UPGRADE_CANDIDATE_WHEEL="$wheel" \
     make test-scale SCALE_TESTS="integration/test_release_upgrade.py"
   ```

   The lane installs the immediately preceding published writer from PyPI, by
   version order below the candidate (including alphas, betas and release
   candidates, excluding development builds before the saved-format promise).
   It stages data with that writer and with the candidate, then reopens both
   owners in place with the installed candidate wheel outside the checkout.
   Keep only published versions in `tests/artifacts/RELEASES` for the weekly
   source-checkout matrix; never add a local wheel path.

   The release workflow compares the candidate with the published predecessor's
   tag. It requires proof for any change beyond documentation and known release
   bookkeeping, or when it cannot classify the change. This includes changes to
   codecs, manifests, receipts, storage and replay schema, identity semantics,
   and corpus generation. The build records the predecessor and SHA-256 digests
   and verifies the checked wheel again before artifact upload. Missing
   predecessor evidence or a failed proof blocks publication. Neither the
   distribution smoke test nor an earlier weekly success clears the candidate.

5. If a saved format changed, add a generation to the artifact corpus:

   ```sh
   uv run python scripts/write_artifact_corpus.py --generation <name>
   ```

6. Tag the release as `v0.Y.0` for a 0.x release, or with its 1.x version after
   the 1.0 stability policy takes effect.
7. After publishing, append the released version to `tests/artifacts/RELEASES`
   so later upgrade checks include it.

### Cloud evidence and waivers

Check BigQuery and Snowflake separately before tagging. For each backend, the
latest executed job in `warehouse-backends` on the candidate SHA must be green
and complete in the seven days up to the original tag push, inclusive. A newer
failure blocks an older success. Jobs skipped by a PostgreSQL-only dispatch
don't count. A workflow retry or manual dispatch uses the original matching
push run's GitHub-recorded UTC instant; a missing tag push record blocks release.
Any paid run still requires approval; this check does not start one.

If the release owner considers a waiver, commit the complete candidate notes
as `.github/release-notes/<tag>.md` on that SHA. For each backend without
qualifying evidence, include one `## BigQuery waiver` or `## Snowflake waiver`
section. After the heading, add these nonempty Markdown list fields in order:
`Date` (UTC `YYYY-MM-DD`, no later than the tag), `Reason`, `Operations`,
`Evidence` (HTTPS link), `Qualification` (what is not verified for the release).
The file and its date are disclosures, not approval.

Before PyPI publication, the build summary shows the exact notes and an
approval comment binding the repository, workflow run and attempt, tag, SHA and
notes SHA-256. The release owner must review the notes and paste that exact
comment when approving the existing `pypi` environment. The workflow verifies
the owner's GitHub review before uploading to PyPI, then publishes the same
notes bytes on GitHub. A blank or mismatched approval fails; rerun all jobs to
request approval for a new attempt. A waiver does not change the backend's
[verification status](docs/guides/backend-compatibility.md#backend-verification-status).

## License and the CLA

terrine is licensed under the [Apache License 2.0](LICENSE).

Before your first pull request can be merged, you must sign the
[Contributor License Agreement](.github/CLA.md). Comment the following on your
PR and the CLA assistant records it:

```
I have read the CLA Document and I hereby sign the CLA
```

You sign once and it covers every future contribution.

**You keep the copyright in your work.** The CLA is a license, not an
assignment: you may reuse, sell or relicense your own contributions freely.
What it grants terrine's owner is the right to sublicense contributed code,
which is what makes commercial licensing and any future relicensing of the
project possible. Without it, a single unreachable contributor can permanently
block both.

Contributing on behalf of a company whose legal team requires its own
agreement? Email kyle.j.caron@gmail.com before opening a pull request.
