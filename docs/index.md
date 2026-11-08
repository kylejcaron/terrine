# Terrine datasets

Terrine builds typed, point-in-time analytical and ML datasets from existing
source tables. It keeps exact anchors, pre-anchor features, post-anchor outcomes,
source completeness, null reasons and column roles together. It makes no causal
claims and ships no effect estimators.

Start with [your first dataset](guides/first-dataset.md), then use the
[guides index](guides/index.md). The everyday objects are **Model**, **Design**,
**Frame** and **Dataset**; optional runtimes, adapters and downstream libraries
stay in focused modules.

Browse the stable [API reference](reference/api/terrine.md), including
[recipes](reference/api/recipes.md) and [design specifications](reference/api/spec.md).
The [facade stability table](guides/public-api.md#focused-facades) identifies
provisional names that are not included in the reference.

Before execution, read [capabilities](guides/capabilities.md),
[cost and data residency](guides/cost-and-residency.md), and
[inherent limits](guides/limitations.md). A code in the
[refusal reference](reference/refusals.md) names the unsupported request and its
recovery category rather than silently changing the question.

## Build the documentation locally

```sh
uv run --group docs python -m tests.documentation --build
```

This builds MkDocs Material with the source-linked guides, executable README,
examples, acceptance cases, benchmark records and stable API reference available
together, without requiring a public repository URL. Output is under
`site/`. The intermediate source copy is temporary. Nothing is
deployed or published. To regenerate the refusal reference after changing
declarations, run `uv run python -m tests.documentation`, then the documentation
tests.
