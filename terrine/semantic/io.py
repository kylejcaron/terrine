"""Safe YAML loading for native semantic catalogs."""

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError
from yaml.resolver import BaseResolver

from terrine.errors import RefusalSpec
from terrine.semantic.models import Catalog

__all__ = ["load_catalog", "dump_catalog"]

_DUPLICATE = RefusalSpec(
    "terrine.catalog.duplicate_yaml_key",
    "The model YAML repeats key {key!r}. Keep one declaration for each key.",
)
_INVALID_YAML = RefusalSpec(
    "terrine.catalog.invalid_yaml",
    "The model YAML cannot be read safely ({reason}). "
    "Correct the YAML and use plain values without custom tags.",
)

_INVALID_DEFINITION = RefusalSpec(
    "terrine.catalog.invalid_definition",
    "The model document is invalid. Correct the listed inputs:\n{diagnostics}",
)
_INVALID_INPUT = RefusalSpec(
    "terrine.catalog.invalid_definition",
    "Model document input {field!r} is invalid ({reason}). "
    "Use a model YAML document with the supported schema version and declared names.",
)
_HINTS = {
    "missing": "Provide this required Catalog field.",
    "extra_forbidden": "Remove this field or use a declared Catalog field.",
    "invalid_day_boundary": (
        'Use "UTC", a signed HH:MM offset, or a UTC-prefixed fixed offset within ±14:00.'
    ),
    "invalid_validity_axis": (
        "Set validity.valid_from with optional validity.valid_to, or set validity.changed_at alone."
    ),
    "invalid_filter_arity": (
        "Choose values matching the filter operation: one, two, or a nonempty list."
    ),
    "invalid_filter_values": "Use JSON scalar values in this filter.",
    "invalid_metric_shape": (
        "For kind ratio, set numerator and denominator only; other kinds require measure only."
    ),
    "unsafe_transform": (
        "Use JSON-safe winsorization data rather than callbacks or runtime objects."
    ),
    "literal_error": "Choose one of the supported values for this field.",
    "string_type": "Provide a string for this field.",
    "int_type": "Provide an integer for this field.",
    "dict_type": "Provide a mapping for this field.",
    "tuple_type": "Provide a sequence for this field.",
}
_DEFAULT_HINT = "Correct this field to match the declared Catalog schema."
_SCHEMA_VERSION = 2
_SCHEMA_VERSION_HINT = (
    "Declare schema_version: 2 and use the current keys: sources carry time, units and "
    "events; the root and each source list events; dimensions carry unit and key; "
    "measures carry event; metrics carry unit."
)


def _catalog_issues(error: ValidationError) -> tuple[dict[str, Any], ...]:
    issues = []
    for detail in error.errors(include_url=False, include_context=False, include_input=False):
        location = tuple(
            component for component in detail["loc"] if isinstance(component, (str, int))
        )
        kind = detail["type"]
        issues.append({"location": location, "kind": kind, "hint": _HINTS.get(kind, _DEFAULT_HINT)})
    return tuple(issues)


def _invalid_definition(error: ValidationError):
    issues = _catalog_issues(error)
    diagnostics = "\n".join(
        f"- {'.'.join(map(str, issue['location'])) or 'Catalog'}: {issue['hint']}"
        for issue in issues
    )
    return _INVALID_DEFINITION.error(issues=issues, diagnostics=diagnostics)


class _UniqueSafeLoader(yaml.SafeLoader):
    pass


def _mapping(
    loader: _UniqueSafeLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as exc:
            raise _INVALID_YAML.error(reason="mapping keys must be scalar") from exc
        if duplicate:
            raise _DUPLICATE.error(key=str(key))
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueSafeLoader.add_constructor(BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def validate_catalog(value: Any) -> Catalog:
    """Validate an in-memory Catalog definition with loader refusal semantics."""
    declared = value.get("schema_version", _SCHEMA_VERSION) if isinstance(value, Mapping) else None
    if isinstance(value, Mapping) and declared != _SCHEMA_VERSION:
        issue = {
            "location": ("schema_version",),
            "kind": "unsupported_schema_version",
            "hint": _SCHEMA_VERSION_HINT,
        }
        raise _INVALID_DEFINITION.error(
            issues=(issue,), diagnostics=f"- schema_version: {_SCHEMA_VERSION_HINT}"
        )
    try:
        return Catalog.model_validate(value)
    except ValidationError as exc:
        raise _invalid_definition(exc) from None


def load_catalog(path: str | Path) -> Catalog:
    """Load one strictly validated Catalog without executing YAML constructors."""
    try:
        with Path(path).open(encoding="utf-8") as stream:
            value = yaml.load(stream, Loader=_UniqueSafeLoader)
    except yaml.YAMLError as exc:
        raise _INVALID_YAML.error(reason="unsafe tag or malformed YAML") from exc
    return validate_catalog(value)


def dump_catalog(catalog: Catalog) -> str:
    """Serialize a native Catalog using ordinary safe YAML values."""
    if not isinstance(catalog, Catalog):
        raise _INVALID_INPUT.error(reason="wrong_type", field="catalog")
    return yaml.safe_dump(catalog.model_dump(mode="json"), sort_keys=False)
