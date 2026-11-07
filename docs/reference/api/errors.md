# `terrine.errors`

Stable coded errors, warning categories, and refusal-code vocabulary.

::: terrine.errors
    options:
      members:
        - TerrineError
        - REFUSAL_CODES

`UnverifiedBackendWarning` is the stable warning raised when an Ibis backend is
explicitly admitted without verified read or consistency guarantees. See the
[backend compatibility guide](../../guides/backend-compatibility.md#unverified-ibis-backends).
