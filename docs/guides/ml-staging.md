# Reproducible ML dataset staging

Terrine builds immutable datasets from a `Design` and a bound `Model`; it does not fit estimators. Training and scoring are lazy dataset views over the same feature specifications, and Parquet exports are pinned to one published dataset generation.

## Training data

Select outcome labels and, optionally, a subset of declared features. The default incomplete-label policy drops rows with unavailable or known-null labels; `keep` preserves null values and their reason metadata. Excluded-label and unassigned-split reason counts survive ordinary materialization, staged Arrow metadata, and `DatasetManifest.frame_metadata`.

```python
frame = design.build(model)
training = frame.training_set(
    label="churned",
    features=("past_sessions", "plan_tier"),
    incomplete_labels="drop",
)
dataset = training.stage("training")

for batch in dataset.to_arrow_batches(batch_size=65_536):
    consume(batch)

export = dataset.export_parquet(destination="./training-export")
```

Pass a `TimeSplit` or `GroupSplit` from `terrine.dataset.splits` to `training_set(split=...)` to append `__terrine_split` and `__terrine_split_reason`. Calendar-date cutoffs resolve at the Catalog-local day boundary and persist as absolute instants. Time splits use the complete selected-label support, not merely the anchor: half-open interval labels may end at a cutoff, while observed point labels must precede it. Overlaps, embargoes, unavailable labels, and combined time/group mismatches remain `unassigned`; they are never reassigned to test. Group keys select unit or cluster-unit identity and use the versioned typed deterministic codec. Fit downstream preprocessing only on rows explicitly assigned to `train`.

Relative labels also retain the selector origin as point support and the compiler's conservative selector-search intervals. A label window ending before a cutoff is insufficient when the event used to select that window depends on later observations. Group-only assignment tests label completeness independently of temporal endpoints. Version-1 manifests that predate optional frame metadata remain reopenable without changing their persisted encoding.

The result is a pinned reference. It does not collect the full Frame into a client-side table. Export shards are checksummed and recorded in a manifest; a retry to the same destination resumes only verified shards for the same dataset generation and export layout. Existing destinations with a different generation, layout, or configuration are refused without overwriting them.

## Scoring snapshots

`Design.snapshot` re-anchors a cross-sectional Design to an explicit scoring instant and requires explicit eligibility. It drops outcomes and treatment, uses the configured unit roster rather than a historical event cohort, and validates only the selected feature and eligibility dependencies. Date values resolve at the Model's Catalog-local day boundary; aware timestamps are normalized to UTC. The literal `"today"` is resolved once per call and stored as an absolute instant.

```python
scoring_frame = design.snapshot(
    model,
    as_of="2026-10-18",
    eligibility={"eligible": eligibility_predicate},
    features=("past_sessions", "plan_tier"),
)
scoring = scoring_frame.stage("scoring")
```

A scoring snapshot does not establish historical recorded-at availability. Shared feature definitions reduce definition drift but do not prove that a retrospectively corrected value was known at a past prediction time. A new scoring date is a new explicit snapshot; it does not mutate an older pinned generation.

## Reopening a pinned dataset

Keep the `DatasetManifest` with the consumer-owned export, or persist `DatasetManifest.to_json()` separately from the immutable `DatasetRecipe` stored by the runtime. Reopen the exact committed generation rather than consulting a moving destination:

```python
reopened = Dataset.from_manifest(dataset.manifest, con=model.con)
for batch in reopened.to_arrow_batches():
    consume(batch)
```

The manifest binds the canonical construction recipe, selected view, generation, schema, and output checksums. Reopening verifies the committed publication, partition checksums, physical schema, and row count before returning the reference. `DatasetRecipeStore` stores a `DatasetRecipe`, not the manifest; restore a serialized manifest with `DatasetManifest.from_json()` and pass it to `Dataset.from_manifest()`. Credentials, live connections, SQL, and row payloads are not stored in the recipe.

## Recipe-backed refresh

`plan_refresh` reconstructs the saved view and produces a metadata-only runtime plan. `refresh` captures current inputs once, reopens that retained snapshot for every runtime task, and publishes a new pinned generation through the built-in executor. It never changes the old reference or a reader already consuming it.

```python
from terrine.runtime.changes import Change, ChangeSet

changes = ChangeSet((Change("update", "events"),), "operator")
plan = reopened.plan_refresh(model, changes=changes, budget=budget)
refreshed = reopened.refresh(model, changes=changes, budget=budget)
for batch in refreshed.to_arrow_batches():
    consume(batch)
```

The local DuckDB path does not discover authoritative upstream revisions. Omitting `changes` conservatively rebuilds the recipe; upstream revision fields remain explicitly unknown even though the retained input receipt is exact. An authoritative empty `ChangeSet` may reuse the pinned generation only while the model definition is unchanged. Refreshing from a superseded generation refuses rather than overwriting a newer publication. A scoring refresh preserves its resolved `as_of`; changing the prediction date requires a new explicit snapshot.

Staging requires a file-backed DuckDB Model connection. The runtime namespace defaults to `terrine_runtime` (`TERRINE_RUNTIME_NAMESPACE`); retained inputs and bounded Arrow task outputs default to `<database-parent>/.terrine/snapshots` (`TERRINE_RUNTIME_SNAPSHOT_ROOT`). The built-in executor currently accepts cross-sectional and StatePanel Designs. Unsupported shapes refuse before source capture; specialized local Frame materialization remains available separately.

Saved requests bind the entire dataset view and model reconstruction identity. Replaying the persisted request through `run_serialized_plan` returns its committed receipt; changing a view under the same request key refuses. Consumers need neither custom task callbacks nor live source connections to read an already committed output generation.

Parquet export currently provides local filesystem publication. A URI alone does not establish an object-store atomicity or durability contract; use only a storage backend whose publication and recovery behavior has been independently verified.

## Worked local examples

Run the complete training/export/correction workflow and the feature-only scoring workflow from the repository root:

```sh
uv run python examples/ml_staging.py
uv run python examples/batch_scoring.py
```

Both examples configure an owner-local namespace and retained snapshot directory, create a file-backed Model, build a Design, stage it, consume bounded Arrow batches, reopen its manifest, export Parquet, and refresh after a source correction. The training example retains exact integral Decimal values beyond the default Python Decimal arithmetic precision. The scoring example removes the unused label relation before staging and preserves the explicit scoring instant through refresh. Temporary databases, snapshots, and exports are cleaned up when each script exits.

