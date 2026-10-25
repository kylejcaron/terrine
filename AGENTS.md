<!-- BEGIN KATA (managed by `kata init --with-agents`) -->
Kata is the system of record for intent.

- Never `kata delete` or `kata purge` without explicit user authorization.

~~~dot
digraph kata {
  rankdir=TB; node [shape=box];

  arrive   [shape=diamond label="Work arrives"];
  search   [label="Search first:\nkata search \"<terms>\" --agent\nReuse an open issue or create one."];
  route    [shape=diamond label="Work it, or delegate it?"];

  subgraph cluster_work {
    label="";
    claim  [label="On claim or start, mark it tracked:\nkata meta set <ref> work.attention ok"];
    branch [label="If the work happens on a dedicated branch, stamp it once:\nkata meta set <ref> work.branch <branch>\nor bind at creation:\nkata create ... --meta work.branch=<branch> --idempotency-key <key>"];
    live   [label="Keep state current:\nkata meta set <ref> work.attention stuck|needs-human|ok\nkata meta set <ref> work.attention_msg \"<why>\"\nstuck = blocked; needs-human = input/review; ok = unblocked.\nRequest attention:\nkata notify <ref> --to <actor>[/<teammate>] --message <reason>"];
    claim -> branch -> live;
  }

  subgraph cluster_delegate {
    label="";
    fanout [label="Tracked children: --parent <ref>, --meta work.branch=<branch>,\n--idempotency-key <key>, --json; capture .issue.short_id.\nSubagents: distinct KATA_TEAMMATE and\nKATA_INBOX_USER=<actor>/<teammate>; keep the actor.\nRead requests: kata inbox --for <actor>[/<teammate>].\nAfter handling: kata notify <ref> --to <actor>[/<teammate>] --clear."];
    join   [label="Join with kata wait <refs> --until attention --any\nMatches needs-human or stuck; a close also completes the wait,\nand the reported reason distinguishes which. Use --timeout so a\nwrapper can tell timeout from satisfaction."];
    coord  [label="Read delegated work.*; never write it."];
    fanout -> join -> coord;
  }

  done     [shape=diamond label="Verified complete?"];
  close    [label="kata close <ref> --done\nwith a message and evidence"];
  review   [label="kata label add <ref> needs-review\nplus a comment on what remains"];
  park     [shape=diamond label="Park it?"];
  schedule [label="kata schedule <ref> <date-or-time>\nsets scheduled_on; clear with -"];
  someday  [label="kata meta set <ref> someday true --json-value\nclear with kata meta unset <ref> someday"];

  arrive -> search -> route;
  route -> claim   [label="work it"];
  route -> fanout  [label="delegate it"];
  route -> park    [label="record only"];
  live  -> done;
  coord -> done;
  done -> close    [label="yes"];
  done -> park     [label="no"];
  park -> schedule [label="start date known"];
  park -> someday  [label="start date unknown"];
  park -> review   [label="needs review"];
}
~~~

Parent links group work; they do not gate readiness, but a parent cannot close with open children.
Use --blocks <dependent> / --blocked-by <prerequisite> only for real prerequisites; they gate kata ready.
Use --related <ref> for context only. kata wait observes state without requiring a dependency edge.

One writer per key: only write your own work.*. Ignore work.* on closed issues; never write it there.
Before stopping, close completed work or update both work.attention and work.attention_msg for the handoff.

Schedule/someday defer work; deadlines don’t. kata deadline <ref> <date-or-time> sets deadline_on without changing readiness.
Reached schedules and deadlines use notify.* for the current owner, or the author when unowned; clear with kata notify <ref> --to <recipient> --clear.
<!-- END KATA -->

# Contributor guidelines

## Development commands

- `uv sync --extra duckdb --extra pandas --extra polars` installs the local
  reference backend and dataframe extras with development dependencies.
- `make check TESTS="<affected paths>"` runs static gates and focused tests;
  run it before each commit. `make fmt` formats source/tests; `uv build` builds
  local distributions.
- `make test-scale` runs the scheduled scale tier (`SCALE_TESTS` overrides the
  `integration/` default); `make complexity` reports without gating. The
  integration tier, the smoke script and examples run in CI on every push and
  before merges; the scale tier runs nightly and on demand.
- CI splits the integration tier into balanced groups from
  `integration/.test_durations`. Tests without a stored time still run (they
  are assigned the average); after large changes refresh it with
  `uv run pytest integration --store-durations --durations-path integration/.test_durations`.
  CI splits the unit tier into four groups balanced from `tests/.test_durations`.
  Run the full unit tier locally with `uv run pytest tests`; refresh its
  durations with `uv run pytest tests --store-durations --durations-path tests/.test_durations`.
- `make test-postgres` runs the live PostgreSQL probes (`@pytest.mark.postgres`,
  deselected by default); they fail, never skip, without
  `TERRINE_TEST_POSTGRES_*` settings. Start a throwaway local server with
  `eval "$(uv run python -m tests.integration.postgres_scratch start)"` and stop it
  with `uv run python -m tests.integration.postgres_scratch stop`. CI runs them
  on every push against a PostgreSQL 16 service container. Each probe owns a
  uniquely named schema; never share a warehouse namespace between tests.
- `make test-bigquery` runs the live BigQuery probes (`@pytest.mark.bigquery`,
  deselected by default); they fail, never skip, without
  `TERRINE_TEST_BIGQUERY_PROJECT`, `TERRINE_TEST_BIGQUERY_DATASET` and
  credentials. Locally,
  `TERRINE_TEST_BIGQUERY_ACCESS_TOKEN="$(gcloud auth print-access-token)"` is
  enough. Each probe creates and drops its own dataset; queries are billed, so
  keep fixtures tiny. CI runs them weekly and on dispatch, serialized.
- `make test-snowflake` runs the live Snowflake probes (`@pytest.mark.snowflake`,
  deselected by default); they fail, never skip, without
  `TERRINE_TEST_SNOWFLAKE_{ACCOUNT,USER,PRIVATE_KEY,WAREHOUSE,DATABASE,SCHEMA}`
  (a key-pair service user; `TERRINE_TEST_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` is
  optional). There are no local credentials: dispatch the `warehouse-backends`
  workflow with `backend_mode=snowflake`. Each probe creates and drops its own
  schema in the test database; warehouse time is billed, so keep fixtures tiny.
  CI runs them weekly and on dispatch, serialized.
- The ordinary suite lists unit tests slower than 5 s (well above the 2 s
  target, so contention does not trip it); a healthy run lists none. Fix any
  listed test.

## Tests and validation

- Keep the smallest permanent suite that protects meaningful consumer-visible
  behavior. Every added test must catch a plausible regression; extend or
  consolidate existing coverage before adding redundant tests. No test-count
  or blanket coverage-percentage targets. Delete a test as a duplicate only
  when its twin asserts everything it does; otherwise move it.
- Test each behavior at the lowest useful boundary; retain targeted cross-path
  and end-to-end checks where composition matters. Do not repeat every edge
  case at every layer. Prefer targeted parameterization over Cartesian matrices.
- Put each test in the cheapest tier that can show its behavior. Tiers are
  defined by when they run:
  - **Unit** (`tests/`, default): one rule at the function that owns it, a few
    rows of input, in-process unless the rule is process isolation itself.
    Unit tests MUST NOT use the network or a real warehouse. Typically
    milliseconds; under about 2 s alone. Runs on every local check and push.
  - **Integration** (`integration/`): composition across subsystems, processes
    or real libraries, such as Model→snapshot→stage→publish→read-back, crash
    recovery, orchestrators and library hand-offs. One end-to-end test per
    workflow, typically under 30 s on the 2-CPU CI runner. A unit-boundary test
    that cannot meet the unit budget (for example fixed compile overhead) also
    moves here. Runs on every push.
  - **Scale** (`@pytest.mark.scale`, under `integration/`): resource-bound
    scale, independent-oracle and owner-approved live-warehouse checks.
    Deselected by default; runs nightly and on demand. Keep them few.
  - A unit test that drives a full pipeline to check one narrow rule is
    misplaced: rewrite it at the owning function.
- Use tiny deterministic fixtures and bounded property-based cases. Avoid sleeps
  and dependence on the real clock; use controlled clocks or synchronization.
  Source-text assertions, forwarding-only checks and mock echoes are not useful
  regression tests. Use throwaway checks for one-off verification instead.
- When a test is slow on tiny inputs, profile it: fixed per-run overhead in
  terrine costs users too. Cut test work before tuning workers or threads, and
  compare in-suite with `-n 0` timings before blaming contention. Do not use
  wall-clock assertions as performance gates.
- Batch independent numeric cases into one query while keeping a separate oracle
  for each case. Module-scoped result fixtures must be immutable and release
  their connections before returning. Keep compiler/execution compositions in
  the matching `integration/` module; retain cheap admission and expression
  rules in `tests/`. Both tiers share the worker DuckDB resource fixture.
- Plan acceptance lists and suggested test names specify required behaviors,
  not a quota of test functions. Each acceptance check names the owning
  function and tier. Consolidate without losing acceptance coverage, and update
  any affected command selectors.
- Tests MUST be independent under `pytest-xdist`. A test MUST NOT rely on
  execution order or state created by another test. Modules that mutate a
  shared filesystem path MUST use `pytest.mark.xdist_group("<name>")`.
  Sockets, lock files, lease names and scratch paths come from `tmp_path` or
  include the xdist worker id; never a fixed path under `/tmp`.
- Tests assert consumer-visible behavior: stable codes, numerical contracts,
  tolerances, invariants, and cross-path parity. They MUST NOT pin permitted
  message wording, private structure, internal wiring, or exact bits where a
  tolerance defines the contract.
- Run focused validation of changed behavior and affected contracts at coherent
  milestones, not after every edit. Use documented commands when available;
  do not copy validation commands from sibling projects before they exist here.

## Limitations

- A refusal MUST cover only the variant its reason applies to and MUST name a
  route forward when one exists.
- Document a limitation only when it is inherent to the source, the available
  construction, or statistical validity. Missing implementation belongs in
  the issue tracker, not in the limitations page.

## Public API and refusals

- Keep an explicit, reviewed `__all__` for public package facades. A symbol is
  not public merely because another internal module imports it or a plan names
  it. New exports need a concrete user workflow; avoid aliases and duplicate
  entry points.
- Keep the everyday workflow centered on Model, Design, Frame and Dataset.
  Supporting constructors should earn top-level exposure through that workflow.
  Advanced estimation, orchestration and telemetry stay in focused modules;
  specialized shapes must not expose their implementation machinery through
  the ordinary dataset API.
- Derived metadata and execution internals may be inspectable without becoming
  required user-authored inputs. Ordinary dataset creation must not require
  constructing compiler state, numeric certificates or runtime records.
- Public inputs describe studies, metrics, and decisions in terms users can
  act on. Do not expose internal proof obligations or mathematical tuning
  controls merely because an implementation needs them.
- Derive defensible defaults when possible. Keep assumptions and the strength
  of guarantees explicit; ease of use MUST NOT depend on hiding either.
- Unsupported public requests raise a coded error with a stable package-wide
  code and immutable, structured context, before loading data or calculating
  results.
- The same hazard MUST use the same refusal code across entry points.
  Refusals MUST survive pickle and deepcopy round trips.

## Numerical and data contracts

- When planning and runtime, or dataframe and query paths, derive the same
  quantity independently, add an equivalence test at matched inputs.
- For small probabilities, allocate family and tail error conservatively. Use
  survival or log-tail functions; do not recover a tail through
  `1 - alpha`.
- Aggregate calculations cover large offsets, neighboring floating-point
  values, input-order changes, merged partitions, and values near overflow.
  Prefer centered, mergeable moments and overflow-safe cross terms.
- When a result can be unavailable, preserve a numeric null and its exact
  reason through schemas, conversions, serialization, and saved fixtures.
- Portable or hashed formats define collection order, tuple encoding, alias
  behavior, duplicate handling, and treatment of runtime-only values.
- Read-only SQL is parsed using the dialect that will execute it and is
  rejected when read-only safety cannot be established.

## Backend portability

- Study queries (`terrine/compile/`, `terrine/temporal/`) are built from Ibis
  expressions so every backend gets them through Ibis's translation. Do not add
  raw SQL strings or `@ibis.udf.*.builtin` bindings to native functions there;
  ruff (`TID251`) rejects `ibis.udf` and `ibis.to_sql` outside the allowed
  packages.
- When Ibis lacks or mistranslates an operation on a supported backend, add a
  dispatched helper in `terrine/backends/` (`dialect`, `numeric`, `timestamps`,
  `joins`, `extrema`) with a live parity test, not a workaround at each call
  site. Supporting a new warehouse means implementing those helpers for it.
  Known Ibis gaps: no ASOF JOIN on BigQuery, NULL-propagating LEAST/GREATEST
  there, and `interval * column` lowered through `EXTRACT(SECOND ...)` (multiply
  integer seconds, then `.as_interval("s")`).
- Raw SQL is acceptable only for: terrine's own DuckDB runtime state tables
  (`terrine/runtime/`), backend-provider catalog introspection and transaction
  control (`terrine/execution/`), and admitted user SQL relations.
- Warehouse results match the DuckDB reference within a stated tolerance
  (floats: tight, relative to the window's absolute sum, since cancellation
  defeats a purely relative bound; exact for counts and decimal sums).
  Do not port DuckDB-specific exactness machinery, such as certified float
  error bounds, to a warehouse to reach bit-for-bit parity.

## Complete changes

- Public API changes update production callers, exports and registries,
  docstrings, guides and examples, fixture builders, saved fixtures, tests,
  and type-checked callers.
- Removing a capability updates every surface that produces, plans, validates,
  serializes, or documents it in the same change: runtime dispatch, planners,
  wire formats, refusal registries, guides, limitations, fixtures, and tests.
  A surviving planner or fixture is evidence that the removal is incomplete.
- Comments and docstrings remain concise and evergreen. Preserve necessary
  derivations and assumptions.
