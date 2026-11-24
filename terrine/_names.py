"""Column identities used to compare names, never to select a physical column."""

from collections.abc import Iterable, Set
from unicodedata import normalize


def canonical_name(name: str) -> str:
    return normalize("NFC", name).casefold()


def available_name(base: str, occupied: Set[str]) -> str:
    """Choose a private alias outside a set of canonical names."""
    candidate = base
    suffix = 0
    while canonical_name(candidate) in occupied:
        suffix += 1
        candidate = f"{base}_{suffix}"
    return candidate


def value_columns(names: Iterable[str]) -> tuple[str, ...]:
    """Expand values into their published value, completeness and reason triples."""
    return tuple(
        column for name in names for column in (name, f"{name}__complete", f"{name}__reason")
    )
