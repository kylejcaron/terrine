# Independent baseline-treatment synthetic worlds

`terrine.simulate` produces a fixed cohort with baseline treatment, measured
baseline covariates, complete fixed-horizon net revenue, and a separate paired
potential-outcome oracle. It requires neither a semantic binder nor a compiler,
estimator, warehouse connection, or increment installation.

```python
from terrine.simulate import WorldConfig, simulate
from terrine.simulate.truth import ate

world = simulate(WorldConfig(n_units=64, outcome_noise_sd=0), seed=17)
population_target = ate(world)
realized_cohort_target = ate(world, target="cohort")
```

These are different targets. Population truth integrates the declared generating
law; cohort truth averages the actual paired potential outcomes for this draw.
Neither is obtained by fitting an estimator or compiling counterfactual datasets.
A planted effect alone is not evidence that an estimator's standard errors or
confidence intervals are calibrated.

## Generating equations and target weights

Let `X ~ Uniform(-1,1)`, with an independent plan tier `C` taking `basic`, `plus`,
`enterprise` with probabilities `.5`, `.3`, `.2`. Define `h=X²−1/3`, and let
`gamma=propensity_quadratic`, `b=outcome_quadratic`:

```text
logit(e) = -.2 + .8X + .5 I(C=plus) - .4 I(C=enterprise) + gamma h
mu0      = 10 + 2X + 1.5 I(C=plus) - I(C=enterprise) + b h
delta    = 2 + .75X + .5 I(C=plus)
mu1      = mu0 + delta
epsilon  ~ Normal(0, outcome_noise_sd²), independent of baseline and assignment
Y0       = mu0 + epsilon
Y1       = mu1 + epsilon
A        ~ Bernoulli(e)
Y        = Y1 if A else Y0
q        = 1 + target_weight_slope * X
```

Both potential outcomes are generated **before** treatment assignment. Their
shared shock is the declared joint potential-outcome model, not an uncertainty
calibration shortcut. Negative observed net revenue is allowed and represents
refunds. The logistic probability is evaluated with a log-sigmoid expression.
Measurements use binary64 arithmetic; they are not exact compiler aggregates and
do not carry compiler rounding-error certificates. Nonfinite generated values
raise a coded refusal rather than being replaced, clipped, or silently omitted.

Configuration is frozen. Cohort size and elapsed-second windows must be positive
integers; time zero must be aware; coefficients must be finite; noise must be
nonnegative. Measurement endpoints and the following calendar-day completeness
endpoint must fit Python's UTC datetime range; the final complete day cannot be
`date.max`. The restriction `abs(target_weight_slope) < 1` makes the declared
baseline-function target weights positive throughout the covariate support.
Seed is an integer in `[0,2**64)`.

By the generating law, `E[X]=0`, `E[X²]=1/3`, and `E[q]=1`, so the analytical
population ATE is `2 + .5*.3`. The weighted population ATE adds
`.75*target_weight_slope/3`. Realized cohort truth instead uses
`sum(q*(Y1-Y0))/sum(q)` when `weighted=True`, and ordinary paired averaging
otherwise. Changing observed coverage or deleting observed events must not alter
the oracle target. Cohort truth retains floating-point rounding in the recorded
pairs rather than substituting the analytical effect formula.

The observed baseline features are `x` and categorical `plan_tier`. Main-effect
logistic propensity and arm-specific linear outcome models match this law when
`gamma=b=0`. Nonzero `gamma` or `b` introduces the explicitly shown quadratic
term; a model that omits it is misspecified. This statement describes model
families, not an empirical estimator recovery or interval-coverage result.

## Reproducible streams

Generator version 1 uses `SeedSequence(seed).spawn(4)` and explicit NumPy PCG64
children, indexed as follows:

| Child | Purpose | Draw order |
| --- | --- | --- |
| 0 | Baseline | all `X`, then all plan tiers |
| 1 | Assignment | Bernoulli draws after both potential outcomes exist |
| 2 | Outcomes | one normal shared shock per unit |
| 3 | Event timing | all revenue offsets, then engagement offsets by unit |

The metadata records this contract and the seed. Changing outcome noise or the
observation horizon does not consume baseline or assignment randomness. Event
IDs are stable logical posting IDs, not timestamp ranks. Stream partitioning is
not a promise of cross-version NumPy bitwise distribution stability or that
changing cohort size preserves earlier categorical draws.

## Arrow tables and native Catalog data

`World` holds immutable mappings and Arrow tables:

- `roster`: physical `unit_id:int64`, nullable UTC-microsecond `observed_from` and
  `observed_until`. Every roster member is a candidate, including inactive units
  with no events. There are no membership endpoints in this fixed population.
- `events`: `event_id:string`, `unit_id:int64`, `ts:timestamp[us,UTC]`,
  `kind:string`, `value:float64`. Engagement is posted before time zero; each
  unit's observed net revenue is posted once in `[t0,t0+horizon)`.
- `attributes`: `unit_id`, `valid_from`, `valid_to`, `x`, `plan_tier`, and
  `baseline_enabled`. Ordinary histories begin strictly before time zero and
  remain valid through it.
- `analysis_table`: `unit_id,anchor_id,t0,x,plan_tier,baseline_enabled,net_revenue,
  target_weight`, plus per-measurement observation bounds, completeness flags,
  and null reasons. It has no propensity, conditional mean, or counterfactual
  columns.
- `oracle`: `unit_id,x,plan_tier,e,mu0,mu1,y0,y1,target_weight`, segregated from
  analysis data and never passed implicitly into estimation.

`world.semantic_yaml` comes from the installed `terrine.simulate/baseline.yaml`
package resource, not a repository-relative file. It is a version-1 native
Catalog document, with fixed day boundary `+00:00` and DuckDB dialect. The named
`world_events` source maps logical `user` to physical `unit_id`; physical tables
are never renamed to make their keys match logical entity names. The SCD2
`world_attributes` dimension joins on its explicit physical `unit_id`.
`engagement_score` maps to `x`; all dimension-backed properties explicitly use
effective event-time semantics. They do not claim separate recorded-at evidence.

`engagement` is an occurrence fact and `revenue` a value fact. Typed `kind`
equality filters separate their measures. `sessions` is a mean-kind metric whose
unit value is an event count; `net_revenue` is a mean-kind metric whose unit value
is a revenue sum. Mean-kind describes a downstream population summary, not an
instruction to average a unit's individual events. No winsorization is declared.

Only integration code needs to write this document to a file, register the three
Arrow tables, and call `Model.from_catalog(path, con=con,
readiness=world.readiness, units=world.units, event_ids=world.event_ids)`. Generating
the document itself does not load or bind it.

## Coverage, strict history, and the independent reference

`world.readiness["world_events"]` explicitly declares complete enclosing UTC
calendar days, including event-free days. Coverage is never inferred from the
maximum observed timestamp. `world.units["user"]` maps to `world_roster` and its
observation columns; null ordinary observation bounds assume continuous
measurement **only inside declared source coverage**.

Event windows are half-open `[start,end)` at the exact intraday time zero.
Strict history before cutoff `c` uses `valid_from < c` and
`(valid_to >= c or valid_to is null)`: a version beginning exactly at `c` does not
replace the immediately preceding version. Overlapping, duplicate, or
nonpositive history intervals are invalid, not arbitrary tie-breaking cases.
Missing history is unavailable, distinct from a known observed null value.

The raw reference in `tests/reference/baseline.py` scans raw rows with independent
Python predicates and grouping. It never reads `analysis_table` or `oracle` and
uses no prefix sums or production temporal resolver. It intersects each source's
calendar coverage with each unit's measurement interval, independently of cohort
membership. An observation end exactly at a window's right endpoint is complete;
ending inside the interval produces null and `incomplete_observation`, even if
some events were observed. Incomplete source coverage produces null and
`incomplete_coverage`; that reason takes precedence if both requirements fail.
A fully measured empty additive window is zero. A nonempty all-null value series
is null with `all_null`, not zero. A strict-as-of with no matching history is null
with `missing_history`. Candidates are retained in all these cases.

`tests/fixtures/baseline.py::materialized_baseline` uses those raw rows and the
shared typed `Roles`, `ColumnMeta`, `FeatureCutoff`, `EligibilitySummary`,
`ExecutionSnapshot`, and `Provenance`. Features have a strict zero cutoff with
honestly labeled retrospective effective-time knowledge; baseline treatment is
declared known at time zero. Eligibility is empty, with an all-retained summary.
There is no invented warehouse snapshot or replay guarantee. Only
`weighted=True` assigns the weight role, safe baseline cutoff, baseline-function
law, and explicit treatment-independence metadata. The ordinary unweighted
fixture can contain the physical target-weight column without assigning it a
causal role. Boundary and malformed-history cases are hand-built fixtures,
not perturbations of the ordinary recovery data-generating law.

## Scope

This world describes independent units, fixed baseline treatment, a fixed cohort,
and fixed-horizon ATE targets. It provides no generic panel or sequential-trial
truth, future-initiation identification, censoring correction, sampled-control
weight law, or ATT estimator. Observational assumptions remain assumptions;
metadata alone cannot establish exchangeability, consistency, no interference,
or usable overlap. Recovery and uncertainty calibration are separate exercises.
