.PHONY: test test-scale test-postgres test-bigquery test-snowflake mutate lint typecheck fmt check complexity

TESTS ?= tests
SCALE_TESTS ?= integration
MUTATION_CORES ?= 2
MUTATION_SAMPLE ?= 1
MUTATION_SEED ?= 20261025
PYTEST_TIME ?=
PYTEST_WORKERS ?= $(if $(filter tests,$(TESTS)),auto,0)
WAREHOUSE_WORKERS ?= auto

# Ordinary suite; scale checks are deselected by default. Focused selections
# run in one process; the complete unit tier uses xdist.
test:
	$(PYTEST_TIME) uv run pytest -n $(PYTEST_WORKERS) $(TESTS)

# Local, on-demand campaigns; keep mutants/ until kill attribution is finished.
# MUTATION_TESTS names the module's own tests: the whole tree is too slow per
# mutant and needs repository files mutmut does not copy.
mutate:
	@test -n "$(MODULE)" -a -n "$(MUTATION_TESTS)" || \
		{ echo "usage: make mutate MODULE=terrine/x.py MUTATION_TESTS='tests/x.py ...'"; exit 2; }
	agent-slots -j $(MUTATION_CORES) -- uv run python scripts/mutate.py \
		--module "$(MODULE)" --cores $(MUTATION_CORES) \
		--sample $(MUTATION_SAMPLE) --seed $(MUTATION_SEED) $(MUTATION_TESTS)

# Scheduled scale tier: resource-bound scale, oracle and live-warehouse checks.
# Cases also marked `postgres` need the TERRINE_TEST_POSTGRES_* settings.
test-scale:
	$(PYTEST_TIME) uv run pytest $(SCALE_TESTS) -m scale

# Live PostgreSQL probes; requires TERRINE_TEST_POSTGRES_* settings and fails
# without them. `uv run python -m tests.integration.postgres_scratch start` starts
# a throwaway local server and prints the settings. The extras provide the driver
# and the DuckDB reference backend. The second run compiles the same conformance
# known answers through the portable base expression policy on the server.
# PostgreSQL cases of the scale tier run with it instead.
test-postgres:
	$(PYTEST_TIME) uv run --extra duckdb --extra postgres pytest integration -n $(WAREHOUSE_WORKERS) -m "postgres and not scale"
	$(PYTEST_TIME) uv run --extra duckdb --extra postgres pytest integration/conformance --generic-dialect -n $(WAREHOUSE_WORKERS) -m postgres

# Live BigQuery probes; requires TERRINE_TEST_BIGQUERY_PROJECT/_DATASET and
# credentials (GOOGLE_APPLICATION_CREDENTIALS, application-default, or
# TERRINE_TEST_BIGQUERY_ACCESS_TOKEN="$(gcloud auth print-access-token)"), and
# fails without them. Each probe creates and drops its own dataset.
test-bigquery:
	$(PYTEST_TIME) uv run --extra duckdb --extra bigquery pytest integration -n $(WAREHOUSE_WORKERS) -m bigquery

# Live Snowflake probes; requires TERRINE_TEST_SNOWFLAKE_{ACCOUNT,USER,PRIVATE_KEY,
# WAREHOUSE,DATABASE,SCHEMA} (key-pair service user) and fails without them. Each
# probe creates and drops its own schema in the test database.
test-snowflake:
	$(PYTEST_TIME) uv run --extra duckdb --extra snowflake pytest integration -n $(WAREHOUSE_WORKERS) -m snowflake

lint:
	uv run ruff check terrine tests integration benchmarks
	uv run vulture terrine
	# Isolated: pydoclint's docstring-parser-fork and dagster's docstring-parser
	# install the same module and would overwrite each other.
	uv run --isolated --no-project --with pydoclint==0.11.1 pydoclint terrine
	uv run tach check

typecheck:
	uv run ty check terrine tests integration benchmarks

fmt:
	uv run ruff format terrine tests integration benchmarks
	uv run ruff check --fix terrine tests integration benchmarks

check: lint typecheck test

# Informational only; complexity reports do not gate ordinary checks.
complexity:
	uv run complexipy terrine --ignore-complexity
