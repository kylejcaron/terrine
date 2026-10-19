# Independent baseline dataset worlds

`terrine.simulate` creates deterministic raw-source worlds for exercising dataset
construction. The packaged `baseline.yaml` catalog and independent reference
builder are part of the fixture contract; the simulator does not ship fitted
estimators or treatment-effect truth.

```python
from terrine.simulate import WorldConfig, simulate
from tests.reference.baseline import reference_rows

world = simulate(WorldConfig(n_units=64), seed=17)
expected_rows = reference_rows(world)
```

The oracle represents expected dataset rows and values. It is not a paired
potential-outcome table, ATE, or other causal target. Dataset checks should
compare anchors, window values, completeness, null reasons, and censoring to the
independent reference implementation.

## Arrow sources and native Catalog

`World` holds immutable mappings and Arrow tables:

- `roster`: physical `unit_id:int64` and UTC-microsecond observation bounds.
- `events`: `event_id:string`, `unit_id:int64`, `ts:timestamp[us,UTC]`,
  `kind:string`, and `value:float64`.
- `attributes`: `unit_id`, `valid_from`, `valid_to`, `x`, `plan_tier`, and the
  recorded baseline attribute.
- `analysis_table`: point-in-time source values with observation bounds,
  completeness flags, and exact null reasons.
- `oracle`: expected dataset rows used only by independent fixture checks.

`world.semantic_yaml` comes from the installed
`terrine.simulate/baseline.yaml` package resource. It is a version-1 native
Catalog document. The named source maps logical `user` to physical `unit_id`;
physical tables are not renamed to match logical entity names. The SCD2
`world_attributes` dimension joins on its explicit physical key.

`world.readiness` declares complete enclosing UTC calendar days, including
event-free days; coverage is never inferred from a maximum timestamp. A fully
measured empty additive window is zero, while unavailable values retain null and
their exact reason. The independent reference in
`tests/reference/baseline.py` scans raw source rows with Python predicates and
grouping; it does not use the production compiler or its temporal resolvers.

## Bounded scale workloads

`ScaleConfig` version 2 describes units, events per unit, retained batch rows,
seed, sparse source span, scheduled anchor count and window width.
`event_batches`, `anchor_batches`, `unit_batches` and `correction_batches`
yield typed Arrow tables without constructing a whole event world.
`unit_start`/`unit_stop` partition keyed streams independently of batch size
and traversal order. Version 1 configurations are explicitly rejected.

Physical event IDs are unique; occurrence IDs may repeat across dates.
Timestamp ties, null values/labels and sparse days are intentional observations.
Corrections identify existing physical events and declare updates/deletions.
`oracle_batches` contains separate expected target counts, not hidden truth
columns in source observations. Independent window references use integer
histograms and exact Type-7 interpolation, checked against pandas on local
fixtures.

The opt-in [benchmark suite](../../benchmarks/README.md) executes real
Model/Design Frames and retained runtime generations, including corrections.
Its intermediate bounds come from input cardinalities, not elapsed-time
thresholds. Traced Python memory, process peak RSS, output bytes and unknown
scan bytes are separate measurements. No warehouse-scale capacity or universal
overhead claim follows from a local calibration.
