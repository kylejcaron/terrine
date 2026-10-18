.PHONY: test test-slow test-calibration lint typecheck fmt check complexity

TESTS ?= tests

# Ordinary suite excludes explicitly marked slow and parameter-recovery tests.
test:
	uv run pytest $(TESTS)

test-slow:
	uv run pytest $(TESTS) -m "slow and not parameter_recovery"

test-calibration:
	uv run pytest -n auto $(TESTS) -m parameter_recovery

lint:
	uv run ruff check terrine tests
	uv run vulture terrine
	uv run pydoclint terrine
	uv run tach check

typecheck:
	uv run ty check terrine tests

fmt:
	uv run ruff format terrine tests
	uv run ruff check --fix terrine tests

check: lint typecheck test

# Informational only; complexity reports do not gate ordinary checks.
complexity:
	uv run complexipy terrine --ignore-complexity
