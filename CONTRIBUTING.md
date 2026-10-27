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
