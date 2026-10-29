"""Stable coded refusals with immutable, portable structured context."""

import math
from collections.abc import Generator, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

__all__ = ["TerrineError", "RefusalSpec", "UnverifiedBackendWarning"]

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]


class UnverifiedBackendWarning(UserWarning):
    """An explicitly admitted backend has no verified read or consistency guarantees."""


class FrozenMap(Mapping[str, Any]):
    """Read-only mapping whose nested values are immutable JSON values."""

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, Any]) -> None:
        object.__setattr__(self, "_values", MappingProxyType(dict(values)))

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("immutable mapping")

    def __reduce__(self):
        return FrozenMap, (dict(self._values),)


def freeze(value: Any) -> Any:
    """Detach JSON-compatible input and recursively make it read-only."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("context numbers must be finite")
        return value
    if isinstance(value, FrozenMap):
        return value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("context keys must be strings")
        return FrozenMap({key: freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(item) for item in value)
    raise TypeError(f"not a JSON value: {type(value).__name__}")


def thaw(value: Any) -> Any:
    """Convert immutable JSON metadata to ordinary serialization values."""
    if isinstance(value, Mapping):
        return {key: thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw(item) for item in value]
    return value


class TerrineError(ValueError):
    """Refusal identified by a stable code rather than message wording."""

    def __init__(self, message: str, *, code: str, context: Mapping[str, Any]) -> None:
        super().__init__(message)
        self._code = code
        self._context = freeze(context)
        self._spec: RefusalSpec | None = None

    @property
    def code(self) -> str:
        return self._code

    @property
    def context(self) -> Mapping[str, Any]:
        return self._context

    def __reduce__(self):
        return _restore_error, (str(self), self.code, thaw(self.context))


def _restore_error(message: str, code: str, context: Mapping[str, Any]) -> TerrineError:
    return TerrineError(message, code=code, context=context)


@dataclass(frozen=True, slots=True)
class RefusalSpec:
    """Module-owned refusal declaration with structured interpolation inputs."""

    code: str
    template: str

    def error(self, **context: Any) -> TerrineError:
        error = TerrineError(self.template.format(**context), code=self.code, context=context)
        error._spec = self
        return error


@contextmanager
def parameter_names(**names: str) -> Generator[None]:
    """Report a refusal raised inside the block under the caller's own parameter names.

    A refusal whose ``field`` is a key of ``names`` is raised again from the same
    declaration with that field renamed; every other refusal passes through unchanged.
    """
    try:
        yield
    except TerrineError as error:
        field = error.context.get("field")
        if error._spec is None or not isinstance(field, str) or field not in names:
            raise
        raise error._spec.error(**{**thaw(error.context), "field": names[field]}) from error
