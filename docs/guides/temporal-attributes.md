# Temporal dimension attributes

Dimension attributes are selected from effective-time history without turning a
lazy expression into a data check. `attribute_asof(points, history, dimension,
property_name, *, unit_column, cutoff_column, strict=True)` returns the original
point columns plus `__terrine_attribute_value` and
`__terrine_attribute_found`. A matched row whose property is null has
`found=True`; an absent history row has `found=False` and a null value. The
fixed output names must not already be present on the point relation.

For declared range validity, event-time lookup uses `[valid_from, valid_to)`.
Strict as-of selection is the left limit: `valid_from < cutoff` and
`valid_to >= cutoff` (or an open end), so a change effective exactly at the
cutoff is excluded. Non-strict selection includes a start equal to the cutoff
and excludes an end equal to it. A `changed_at` history is interpreted as
`[changed_at, next_changed_at)` with the final version open-ended. This is
valid/effective-time semantics only; it does not establish when a correction
was recorded or known.

`enrich_events(events, source, dimension_relations)` applies event-time
selection and retains event columns. It resolves each dimension's
`unit` through `source.units`, rather than assuming the analysis
unit key is the dimension key. Each dimension's property gets a deterministic
value alias and match-flag alias from the internal
`_enriched_property_columns(dimensions, raw_columns)` resolver. Callers that
need enriched output names should use this resolver with the same bound
dimensions and normalized event columns passed to `enrich_events`; the result
maps `(dimension_name, property_name)` to `(value_column, found_column)`.
Dimension physical columns are not resolver inputs because they are projected
away after the join. Resolver outputs use reserved Terrine names, avoid all
supplied event columns case-insensitively, and are stable for the same metadata
and event schema.

Before materializing a verified export, callers must explicitly run
`validate_history(history, dimension, *, execute)` with the caller's pinned
executor. The required `execute` callable accepts an Ibis aggregate expression
and returns a PyArrow table; validation issues only bounded aggregate results,
not full history rows. The executor must use the same admitted snapshot as the
eventual materialization. Neither as-of lookup nor event enrichment performs an
implicit validation query, retries, or silently deduplicates ambiguous rows.
A `valid_from`-only range has open-ended records, so validation rejects
multiple rows for one entity as overlapping. `changed_at` remains separate:
successive change timestamps infer each version's end from the next timestamp
and are not rejected merely for having multiple rows per entity. A failed
validation prevents verified materialization; a bare expression execution is
not a verification receipt.

Both lookup entry points reject missing declared validity columns with
`terrine.semantic.schema_mismatch` before constructing a join. Correct the
history relation or its declared column mapping; neither entry point substitutes
an untyped backend exception for this refusal.

Study composition also checks observation completeness, not just whether a row
matched. Carried inline/derived change-point state requires continuous declared
source and unit observation from its matched timestamp through the cutoff. A
record before coverage starts is unavailable even if its derived interval spans
the cutoff. Re-observation within coverage or an explicitly authoritative
`valid_from`/`valid_to` record supplies a usable state; a derived next-change end
alone does not. Source-history gaps take precedence over unit-observation gaps.

Strict and non-strict joins compare mixed microsecond/nanosecond instants without
discarding submicrosecond changes. History checks reject malformed aggregate
count results rather than treating an empty, null or fractional result as success.
