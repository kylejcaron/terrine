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
