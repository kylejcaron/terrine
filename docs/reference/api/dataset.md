# `terrine.dataset`

Stable split policies for leakage-aware training views. `Dataset` itself is provisional; see the [facade stability table](../../guides/public-api.md#focused-facades).

::: terrine.dataset
    options:
      members:
        - GroupSplit
        - TimeSplit

## Frame definitions

`Frame.to_definition() -> DatasetDefinition` exports the resolved construction
of a built Frame: its design, ordered selections, bound split or absolute
scoring time, and frozen transforms. It neither reads source data nor requires
the publication runtime. Save with `canonical_json()` and reconstruct with
`DatasetDefinition.from_json(data).build(model)` on compatible model bindings.
The definition and its digest identify the construction, not an input snapshot,
an execution authorization, or a published generation. See
[saving a Frame definition](../../guides/ml-staging.md#saving-a-frame-definition-without-staging).
