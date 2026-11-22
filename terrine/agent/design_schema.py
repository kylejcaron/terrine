"""Discovery schema for the versioned Design codec."""

from typing import Any

from terrine.spec._design_structure import JSON_SCHEMA_DIALECT, design_definitions
from terrine.spec.io import SCHEMA_VERSION


def design_schema() -> dict[str, Any]:
    return {
        "$schema": JSON_SCHEMA_DIALECT,
        "$defs": design_definitions(),
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "design"],
        "properties": {
            "schema_version": {"const": SCHEMA_VERSION},
            "design": {"$ref": "#/$defs/Design"},
        },
    }
