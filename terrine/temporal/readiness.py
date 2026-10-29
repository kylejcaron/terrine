"""Explicit per-fact-source bound completeness declarations."""

from collections.abc import Mapping
from types import MappingProxyType

from terrine.data import BoundCoverage
from terrine.errors import RefusalSpec

__all__ = ["Readiness"]

_MISSING = RefusalSpec(
    "terrine.readiness.missing_source",
    "Event or history table {source!r} has no declared complete time range. "
    "Pass complete=(first_day, last_day) with the inclusive days fully loaded for that table.",
)
_INVALID_BINDING = RefusalSpec(
    "terrine.model.invalid_binding",
    "Model input {field!r} is invalid ({reason}). "
    "Provide the declared complete time range for each event and history table.",
)


class Readiness:
    """Immutable map from fact-source names to bound coverage intervals."""

    __slots__ = ("_sources",)

    def __init__(self, sources: Mapping[str, BoundCoverage]) -> None:
        if not isinstance(sources, Mapping) or any(
            not isinstance(name, str) or not name or not isinstance(value, BoundCoverage)
            for name, value in sources.items()
        ):
            raise _INVALID_BINDING.error(reason="wrong_type", field="sources")
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
