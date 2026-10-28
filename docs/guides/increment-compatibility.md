# `increment` compatibility

Terrine can translate the published [`increment`](https://pypi.org/project/increment/) semantic definitions into the same native `Catalog` and `Model` path used by Terrine catalogs. Install the optional extra with `pip install 'terrine[increment]'`; the supported release is exactly `increment==0.1.0a2`. Release `0.1.0a1` does not publish the semantic definitions API; other releases are not covered by this compatibility contract. The bridge imports `increment` only when `Model.from_increment(...)` is called, so native catalog use does not require the extra.

```python
model = Model.from_increment(
    "definitions/",
    con=con,
    readiness={"events": Coverage("2025-01-01", "2025-01-31")},
)
```

`path_or_definitions` accepts an increment YAML file, directory, or a real `increment.semantics.Definitions` instance. A directory is loaded with increment's own loader after Terrine checks each YAML document for duplicate mapping keys. Missing the optional distribution raises a coded `terrine.increment.missing_extra` refusal before the input is read. The bridge projects dataset semantics—fact sources, facts, inline properties, referenced dimensions, and metrics—not the complete increment application. Exposure and experiment declarations, `week_start`, and unreferenced dimensions are outside this projection and do not prevent Catalog creation. They are not interpreted: no population or treatment is inferred, and an exposure name is not a fact anchor unless the source explicitly declares that fact.

## Preserved Catalog semantics

The bridge keeps each SQL query as its source relation and re-admits it through Terrine's whitelist for the execution dialect. Increment's loader admission is not treated as Terrine's safety boundary. A declared SQL dialect must match `con.name`; when omitted, the connection dialect is used. The translated Catalog is a `schema_version: 2` document: increment's `timestamp_column` becomes the source `time`, every alternative entity column becomes a `units` entry (mapped identity-to-identity), facts including occurrence-only facts become `events`, a dimension's `entity` becomes its `unit` and `key`, each metric's `entity` becomes its `unit`, and property dtype and `as_of`, source dimension references, validity range or change-point columns, and metric filters are retained.

Supported metric kinds are `mean`, `conversion`, and `ratio`. Their sum/count/count-distinct/event-average/calendar-average/min/max aggregations and declared `window_days` values are carried into native measures. A conversion's implicit count aggregation matches increment's own default. Ratio numerator and denominator measures receive deterministic `<metric>.numerator` / `<metric>.denominator` names. When an increment property name occurs in multiple source/dimension scopes, it is qualified with that scope (for example, `events.platform`); if a source and dimension share a scope name, the qualifier includes its kind (for example, `source.shared.region` versus `dimension.shared.region`). Filter references are rewritten by the owning source. A resulting name collision or a filter that cannot distinguish two same-named properties in its source is rejected with a coded refusal. A shared dimension is represented once; its deterministic owner is the lexicographically first referencing fact source, while any source that declares that dimension can use its dimension-backed filters.

Fixed and percentile winsorization declarations are preserved as JSON transform metadata on the native metric, never converted into a different operation. Adapter construction binds metadata only; it does not execute or approximate the transform, so this is not evidence of transformed outcomes. Unsupported increment metric kinds (including retention, active, total, and quantile) are refused using Terrine's shared unsupported-temporal-kind refusal. Source SQL using functions outside Terrine's read-only whitelist also remains refused, even if increment's broader loader admits it.

The bridge binds metadata only: it does not execute filters, apply windows or
decide row availability. Execution requires separate native design validation
and runtime completeness checks; preserving a declaration does not authorize
executing it. Upstream SQL `WHERE` excludes NULL-valued filter rows; the native
dataset contract distinguishes UNKNOWN, observed nulls and missing dimension
history. Importing a predicate is not evidence of identical result availability.
For an explicit nullness criterion,
declare a boolean property in admitted source SQL and filter on that property.

The optional SDK is loaded dynamically at this boundary. Native output types are
checked without installing it; the published SDK contract is exercised separately
by compatibility checks rather than requiring its types for native development.

## Limits

- `window_days` is preserved in `Measure.window_days`. The approved temporal contract requires the requested duration to equal each declared `window_days`; conflicts are refused rather than silently rewritten.
- The bridge preserves published property `as_of` descriptors; `pre_exposure` describes a latest value before first exposure. Catalog mapping does not compute exposure-specific values or infer an eligible population from that descriptor alone.
- Exposure and experiment declarations are outside Catalog projection. Their assignment, treatment, and population meaning must be supplied explicitly through Terrine's design/anchor workflow; this bridge does not claim experiment parity.
- The `increment` dependency is optional. Native Catalog imports, binding, and execution remain independent of increment.
