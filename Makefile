.PHONY: test test-scale test-postgres lint typecheck fmt check complexity

TESTS ?= tests
SCALE_TESTS ?= integration

# Ordinary suite; scale checks are deselected by default.
test:
	uv run pytest $(TESTS)

# Scheduled scale tier: resource-bound scale, oracle and live-warehouse checks.
test-scale:
	uv run pytest $(SCALE_TESTS) -m scale

# Live PostgreSQL probes; requires TERRINE_TEST_POSTGRES_* settings and fails
# without them. `uv run python -m tests.integration.postgres_scratch start` starts
# a throwaway local server and prints the settings. The extras provide the driver
# and the DuckDB reference backend.
test-postgres:
	uv run --extra duckdb --extra postgres pytest integration -m postgres

# Live BigQuery probes; requires TERRINE_TEST_BIGQUERY_PROJECT/_DATASET and
# credentials (GOOGLE_APPLICATION_CREDENTIALS, application-default, or
# TERRINE_TEST_BIGQUERY_ACCESS_TOKEN="$(gcloud auth print-access-token)"), and
# fails without them. Each probe creates and drops its own dataset.
test-bigquery:
	uv run --extra duckdb --extra bigquery pytest integration -m bigquery

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
