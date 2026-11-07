# `terrine.spec`

Stable data-only declarations and expressions for the full-flexibility Design API. Names classified as provisional are intentionally omitted; see the [facade stability table](../../guides/public-api.md#focused-facades).

::: terrine.spec
    options:
      members:
        - Expr
        - Predicate
        - Eq
        - Ne
        - Gt
        - Gte
        - Lt
        - Lte
        - IsIn
        - Between
        - IsNull
        - And
        - Or
        - Not
        - DatasetDefinition
        - DatasetDefinitionReference
        - DatasetDefinitionStore
        - AnchorRange
        - BinnedMetricState
        - CohortGrouping
        - CohortSpec
        - LifeTable
        - EventAnchor
        - EventTime
        - FixedDate
        - FunnelSpec
        - Initiation
        - PersistentAdoption
        - Schedule
        - SessionSpec
        - StateObservations
        - StatePanel
        - TimeToEvent
        - Trajectory
        - Treatment
        - TrialWeight
        - Winsorization
        - agg
        - all_of
        - any_of
        - asof
        - eq
        - ne
        - quantile
        - event_time
        - from_json
        - from_yaml
        - gt
        - gte
        - lt
        - lte
        - isin
        - between
        - is_null
        - not_
        - to_json
        - since
        - to_yaml

## Design

The stable Design describes row grain, anchors, expressions, predicates and
completeness. Provisional specialized shapes are omitted; see the [facade
stability table](../../guides/public-api.md#focused-facades).

::: terrine.spec.Design
    options:
      show_docstring_parameters: false
      show_docstring_attributes: false
      show_signature_annotations: false
