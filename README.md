# terrine

Terrine is a Python package for typed analytical dataset workflows. Its native
contracts include exact elapsed windows, coded errors, independent synthetic
worlds, and baseline-treatment ATE estimation.

## Install and develop

Requires Python 3.13 or 3.14 and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --group dev --extra duckdb --extra pandas
make check
```

The ordinary suite excludes tests marked `slow` or `parameter_recovery`. Run a
focused selection with `make test TESTS="tests/test_windows.py"`. The `lint`,
`typecheck`, and `fmt` targets run Ruff/Vulture/pydoclint/Tach, ty, and Ruff
formatting respectively. `make complexity` reports complexity and is
informational, not part of the gate. `make check` runs static checks and the
ordinary test suite.

Explicit expensive tiers use `make test-slow TESTS=...` and
`make test-calibration TESTS=...`. Keep the ordinary feedback loop under
10 seconds on a warm local environment as an initial development budget,
not a per-test assertion. The foundation selection measured 0.94 seconds
(21 cases) on Python 3.13/macOS; investigate sustained regressions and
revisit the budget from measurements as capabilities grow.

## Current pure contracts

```python
from terrine import TerrineError, Window

window = Window("-7d", "0s")  # normalized elapsed seconds, half-open bounds
try:
    raise TerrineError(
        "Unsupported operation",
        code="terrine.unsupported",
        context={},
    )
except TerrineError as error:
    print(error.code)
```

Window endpoints represent elapsed durations, not calendar dates.

## Guides

- [Synthetic worlds and independent truth](docs/guides/synthetic-worlds.md)
- [Native baseline ATE estimation](docs/guides/baseline-estimation.md)
