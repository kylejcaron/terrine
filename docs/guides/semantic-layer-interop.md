# Semantic-layer interoperability

Semantic compatibility and runtime backend support are separate contracts. A
semantic adapter must map reviewed definitions into Terrine's native `Catalog`;
SQL compilation or a familiar file extension does not prove that mapping.

| Input | Status | Contract and evidence |
| --- | --- | --- |
| Native Catalog schema version 2 | Verified on local DuckDB | Explicit sources, events, properties, dimensions and measures; [native execution without increment](../../tests/compat/test_semantic_interop.py) |
| `increment==0.1.0a2` | Verified for the documented dataset projection | [Versioned mapping and limits](increment-compatibility.md); [executed mapping/refusal tests](../../tests/compat/test_increment_semantics.py) |
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

The [common-stack guide](common-stack-interop.md) covers bounded Arrow/dataframe
handoffs and pinned Dataset metadata. The [backend matrix](backend-compatibility.md)
records actual execution evidence; it does not promote semantic mapping support
to PostgreSQL, Snowflake or BigQuery runtime support.
