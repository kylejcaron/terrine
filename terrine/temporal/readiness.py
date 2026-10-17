"""Explicit per-fact-source bound completeness declarations."""

from collections.abc import Mapping
from types import MappingProxyType

from terrine.data import BoundCoverage
from terrine.errors import RefusalSpec

__all__ = ["Readiness"]

_MISSING = RefusalSpec(
    "terrine.readiness.missing_source",
    "No completeness coverage was declared for fact source {source!r}.",
)


class Readiness:
    """Immutable map from fact-source names to bound coverage intervals."""

    __slots__ = ("_sources",)

    def __init__(self, sources: Mapping[str, BoundCoverage]) -> None:
        if not isinstance(sources, Mapping) or any(
            not isinstance(name, str) or not name or not isinstance(value, BoundCoverage)
            for name, value in sources.items()
        ):
            raise TypeError("sources must map names to BoundCoverage")
        self._sources = MappingProxyType(dict(sources))

    @property
    def sources(self) -> Mapping[str, BoundCoverage]:
        return self._sources

    def for_fact(self, source_name: str) -> BoundCoverage:
        try:
            return self._sources[source_name]
        except KeyError as exc:
            raise _MISSING.error(source=source_name) from exc

    def __repr__(self) -> str:
        return f"Readiness(sources={dict(self._sources)!r})"
