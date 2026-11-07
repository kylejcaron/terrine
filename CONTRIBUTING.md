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

Documentation checks:

```sh
uv run python examples/first_dataset.py
uv run pytest -q tests/test_documentation.py
uv run python -m tests.documentation --build
```

## Releasing

Before tagging a release:

1. Confirm CI is green for the release commit. The release workflow requires the
   `ci-ok` job for every release tag. Release candidates and final 1.x tags also
   require successful PostgreSQL and scale jobs on that commit. Dispatch both
   workflows against a ref containing the release commit, then wait for those
   jobs to succeed before tagging:

   ```sh
   gh workflow run warehouse-backends --ref <ref> -f backend_mode=postgres
   gh workflow run scale --ref <ref>
   ```

   BigQuery and Snowflake runs are advisory. If the CI push run was deferred or
   never ran in full, run `gh workflow run CI --ref <ref>` and wait for `ci-ok`.
2. Run the stable-API compatibility check:

   ```sh
   uv run python scripts/api_check.py
   ```

3. Run the release-upgrade test when `tests/artifacts/RELEASES` lists earlier 1.x
   releases:

   ```sh
   uv run pytest -n 0 -m scale integration/test_release_upgrade.py
   ```

   For the first 1.x release, `RELEASES` is empty and the test collects no
   cases. Validate the producer once against the locally built wheel instead:
   run `uv build`, add the wheel's absolute path as one line in
   `tests/artifacts/RELEASES`, run the command above, then empty `RELEASES`
   again.

4. If a saved format changed, add a generation to the artifact corpus:

   ```sh
   uv run python scripts/write_artifact_corpus.py --generation <name>
   ```

5. Tag the release as `v0.Y.0` for a 0.x release, or with its 1.x version after
   the 1.0 stability policy takes effect.
6. After publishing, append the released version to `tests/artifacts/RELEASES`
   so later upgrade checks include it.

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
