# `terrine`

The package-root workflow for binding data, describing a Design, and creating a Frame. See the [facade stability table](../../guides/public-api.md#focused-facades) for names whose status is provisional and therefore not listed in this stable reference.

::: terrine
    options:
      members:
        - UnitSource
        - Events
        - History
        - Coverage
        - count
        - sum
        - mean
        - min
        - max
        - count_distinct
        - ratio
        - FixedDate
        - EventAnchor
        - Schedule
        - AnchorRange
        - agg
        - asof
        - event_time
        - since
        - eq
        - ne
        - gt
        - gte
        - lt
        - lte
        - isin
        - between
        - is_null
        - all_of
        - any_of
        - not_
        - recipes
        - Budget
        - preview
        - to_yaml
        - from_yaml
        - TerrineError

## Model

::: terrine.Model
    options:
      members:
        - from_tables
        - from_catalog
        - to_catalog

`Design` is the stable full-flexibility compilation target; see
[`terrine.spec.Design`](spec.md#terrine.spec.Design) for its stable reference.

`UnverifiedBackendWarning` is also a stable root export. It marks an explicitly
admitted Ibis backend whose read or consistency guarantees are not verified; see
[backend compatibility](../../guides/backend-compatibility.md#unverified-ibis-backends).
