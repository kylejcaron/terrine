.PHONY: test test-scale lint typecheck fmt check complexity

TESTS ?= tests
SCALE_TESTS ?= integration

# Ordinary suite; scale checks are deselected by default.
test:
	uv run pytest $(TESTS)

# Scheduled scale tier: resource-bound scale, oracle and live-warehouse checks.
test-scale:
	uv run pytest $(SCALE_TESTS) -m scale

lint:
	uv run ruff check terrine tests integration
	uv run vulture terrine
	# Isolated: pydoclint's docstring-parser-fork and dagster's docstring-parser
	# install the same module and would overwrite each other.
	uv run --isolated --no-project --with pydoclint==0.11.1 pydoclint terrine
	uv run tach check

typecheck:
	uv run ty check terrine tests integration

fmt:
	uv run ruff format terrine tests integration
	uv run ruff check --fix terrine tests integration

check: lint typecheck test

# Informational only; complexity reports do not gate ordinary checks.
complexity:
	uv run complexipy terrine --ignore-complexity
