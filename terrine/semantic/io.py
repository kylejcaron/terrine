"""Safe YAML loading for native semantic catalogs."""

from pathlib import Path
from typing import Any

import yaml
from yaml.resolver import BaseResolver

from terrine.errors import RefusalSpec
from terrine.semantic.models import Catalog

__all__ = ["load_catalog", "dump_catalog"]

_DUPLICATE = RefusalSpec("terrine.catalog.duplicate_yaml_key", "Catalog YAML repeats key {key!r}.")
_INVALID_YAML = RefusalSpec(
    "terrine.catalog.invalid_yaml", "Catalog YAML is not safe valid YAML: {reason}."
)


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


def load_catalog(path: str | Path) -> Catalog:
    """Load one strictly validated Catalog without executing YAML constructors."""
    try:
        with Path(path).open(encoding="utf-8") as stream:
            value = yaml.load(stream, Loader=_UniqueSafeLoader)
    except yaml.YAMLError as exc:
        raise _INVALID_YAML.error(reason="unsafe tag or malformed YAML") from exc
    return Catalog.model_validate(value)


def dump_catalog(catalog: Catalog) -> str:
    """Serialize a native Catalog using ordinary safe YAML values."""
    if not isinstance(catalog, Catalog):
        raise TypeError("catalog must be a Catalog")
    return yaml.safe_dump(catalog.model_dump(mode="json"), sort_keys=False)
