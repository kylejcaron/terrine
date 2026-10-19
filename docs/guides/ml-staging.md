# Reproducible ML dataset staging

Terrine builds immutable datasets from a `Design` and a bound `Model`; it does not fit estimators. Training and scoring are lazy dataset views over the same feature specifications, and Parquet exports are pinned to one published dataset generation.

## Training data

Select outcome labels and, optionally, a subset of declared features. The default incomplete-label policy drops rows with unavailable or known-null labels and records excluded reasons; `keep` preserves null values and their reason metadata. A split policy is not silently applied by this view: use the explicit split-assignment API or omit `split`.

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

Keep the `DatasetManifest` with the consumer-owned export or persist it using the owner's configured immutable recipe storage. Reopen the exact committed generation rather than consulting a moving destination:

```python
reopened = Dataset.from_manifest(dataset.manifest, con=model.con)
for batch in reopened.to_arrow_batches():
    consume(batch)
```

The manifest binds the canonical construction recipe, selected view, generation, schema, and output checksums. Reopening verifies the committed publication, partition checksums, physical schema, and row count before returning the reference. Credentials, live connections, SQL, and row payloads are not stored in the recipe.

Parquet export currently provides local filesystem publication. A URI alone does not establish an object-store atomicity or durability contract; use only a storage backend whose publication and recovery behavior has been independently verified.
