# Semantic-layer interoperability

Semantic compatibility and runtime backend support are separate contracts. A
semantic adapter must map reviewed definitions into Terrine's native `Catalog`;
SQL compilation or a familiar file extension does not prove that mapping.

| Input | Status | Contract and evidence |
| --- | --- | --- |
| Native Catalog schema versions 2–4 | Verified on local DuckDB | Explicit source and metric bindings; v4 also stores formulas and population requirements |
| `increment==0.1.0a2` | Verified for the documented dataset projection | [Versioned mapping and limits](increment-compatibility.md); [executed mapping/refusal tests](../../tests/compat/test_increment_semantics.py) |
| Portable metric definitions v1 | Verified on local DuckDB | Use `Model.from_metric_definitions` for named measures, formulas and explicitly bound populations |
| Other increment versions | Unverified | Use the tested optional release or provide a native Catalog |
| MetricFlow/LookML and arbitrary semantic YAML | Unsupported mapping | No versioned lossless adapter exists; foreign fields fail native Catalog validation rather than becoming guessed metrics |
| New reviewed dbt manifest-to-Catalog binding | Owner-gated, unimplemented | No new manifest reader or named dbt integration is supplied; bind an explicit native relation instead |

`Model.from_catalog(catalog_or_path, con=con, readiness=...)` is independently
usable without increment. Catalog YAML is safely loaded and strictly validated;
it is not a generic semantic-layer importer. Source SQL is admitted for the
execution dialect, with native relation identity and function-ownership checks.
No adapter may infer metrics, treatment, eligibility or coverage from names.

`Model.from_increment(...)` imports its optional SDK only when requested. The
adapter preserves the documented dataset definitions and refuses unsupported
metric meanings with the existing shared temporal refusal. Preserving a metric
or filter declaration is not proof of executable availability or transformed
outcomes; the normal Design validation and completeness rules still apply.

## Metrics

Install `terrine[metric-definitions]` once `metric-definitions` is available
from your package index. In development, install the matching wheel directly.
Open an Ibis connection to the tables named by your YAML and pass the unit
roster and complete event days explicitly:

```python
import terrine as tr

units = {"customer": tr.UnitSource("customers", "customer_id")}
readiness = {"orders": tr.Coverage("2026-01-01", "2026-01-02")}
model = tr.Model.from_metric_definitions(
    "metrics.yaml",
    con=con,
    units=units,
    readiness=readiness,
    metrics=["net_revenue", "revenue_per_customer"],
)
design = tr.Design(
    "customer",
    tr.FixedDate("2026-01-01T12:00:00Z"),
    outcomes={"net": tr.agg("net_revenue", window=("0s", "1d"))},
)
frame = design.build(model)
```

Use `metrics=` to select the definitions you need and their dependencies.
Omitting it selects every metric. Use separate Models for definitions that
require different rosters for the same unit. A declared population is not
inferred from order activity: if `revenue_per_customer` requires
`customers.customer_id`, a different roster or key is refused even if it
currently has the same rows.

Each aggregate uses the Design's own half-open window at the unit and snapshot
grain. Filters apply to their named measures. Physical dimension keys must be
non-null and unique; the frame validates this before publishing results. A
complete window with no matching events contributes zero to sums and counts.
An incomplete window stays null with its availability reason, and a complete
window with a zero divisor yields null with `zero_denominator`.

The frame reports per-unit values and conversion indicators. For a report-level
ratio, sum the matching components at the report grain and divide once; an
average of the displayed ratios answers a different question. Formula arithmetic
uses binary64 after the measures have been reduced in their native numeric
domains. Use `tr.agg` for formulas rather than event-value quantiles or distinct
counts.

`model.to_catalog()` writes a native v4 Catalog with the formula and any
required population's name, table and key. It does not contain the live roster,
connection or readiness. Bind it again with
`tr.Model.from_catalog(model.to_catalog(), con=con, units=units, readiness=readiness)`.
Native v3 catalogs continue to load without a declared eligible population.

Use `Model.from_increment` for existing Increment SDK objects or native YAML.
The metric definitions reader does not reinterpret Increment SQL, historical
properties, exposure settings or transforms as portable declarations.

The [common-stack guide](common-stack-interop.md) covers bounded Arrow/dataframe
handoffs and pinned Dataset metadata. The [backend matrix](backend-compatibility.md)
records actual execution evidence; it does not promote semantic mapping support
to PostgreSQL, Snowflake or BigQuery runtime support.
