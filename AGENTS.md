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
- `make check TESTS="<affected paths>"` runs static gates and focused tests.
  `make fmt` formats source/tests; `uv build` builds local distributions.
- `make test-slow TESTS=...` selects the expensive tier; `make complexity`
  reports without gating.
- Initial warm fast-test feedback budget: 10 seconds; investigate sustained
  regressions rather than adding wall-clock assertions. Measure at milestones.

## Tests and validation

- Keep the smallest permanent suite that protects meaningful consumer-visible
  behavior. Every added test must catch a plausible regression; extend or
  consolidate existing coverage before adding redundant tests. No test-count
  or blanket coverage-percentage targets.
- Test each behavior at the lowest useful boundary; retain targeted cross-path
  and end-to-end checks where composition matters. Do not repeat every edge
  case at every layer. Prefer targeted parameterization over Cartesian matrices.
- Use tiny deterministic fixtures and bounded property-based cases. Avoid sleeps
  and dependence on the real clock; use controlled clocks or synchronization.
  Source-text assertions, forwarding-only checks and mock echoes are not useful
  regression tests. Use throwaway checks for one-off verification instead.
- Unit tests must stay fast. Measure suite/test durations at coherent milestones
  and investigate regressions; keep genuinely expensive integration, benchmark
  and calibration work in explicit tiers. Do not use brittle wall-clock
  assertions as unit-test performance gates.
- Plan acceptance lists and suggested test names specify required behaviors,
  not a quota of test functions. Consolidate without losing acceptance coverage,
  and update any affected command selectors.
- Unit tests MUST NOT use the network or a real warehouse. Live warehouse
  checks belong under `integration/warehouse_execution/`.
- Tests MUST be independent under `pytest-xdist`. A test MUST NOT rely on
  execution order or state created by another test. Modules that mutate a
  shared filesystem path MUST use `pytest.mark.xdist_group("<name>")`.
- Tests slower than the ordinary suite use `@pytest.mark.slow`.
  Simulation-based coverage, bias, or parameter-recovery checks additionally
  use `@pytest.mark.parameter_recovery` and retain a small fast smoke case.
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
