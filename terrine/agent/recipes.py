"""Typed discovery records for the existing DatasetRecipe codec."""

import json
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

from terrine._time import utc_instant
from terrine.agent.design_schema import design_schema
from terrine.dataset.splits import policy_from_record
from terrine.errors import thaw
from terrine.model import Model
from terrine.spec.anchors import FixedDate
from terrine.spec.dataset import INVALID_RECIPE, DatasetRecipe
from terrine.spec.io import from_json


class ViewRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    features: tuple[str, ...]

    @field_validator("features")
    @classmethod
    def _features(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value) or any(not item for item in value):
            raise ValueError("features must contain unique nonempty names")
        return value


class GroupRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    kind: Literal["group"]
    group: str
    train_fraction: str
    validation_fraction: str
    seed: int
    codec: Literal["terrine.group.v1"]


class TimeRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    kind: Literal["time"]
    train_until: str
    validation_until: str | None
    embargo_microseconds: Annotated[int, Field(ge=0)]
    groups: GroupRecord | None


class TrainingView(ViewRecord):
    kind: Literal["training"]
    labels: Annotated[tuple[str, ...], Field(min_length=1)]
    incomplete_labels: Literal["drop", "keep"]
    split: Annotated[TimeRecord | GroupRecord, Field(discriminator="kind")] | None

    @field_validator("labels")
    @classmethod
    def _labels(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return cls._features(value)


class ScoringView(ViewRecord):
    kind: Literal["scoring"]
    as_of: str
    eligibility: dict[str, Any]
    parent_design_hash: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]

    @field_validator("as_of")
    @classmethod
    def _instant(cls, value: str) -> str:
        utc_instant(value)
        return value


_VIEW = TypeAdapter(Annotated[TrainingView | ScoringView, Field(discriminator="kind")])


def parse_recipe(serialized: str, model: Model) -> DatasetRecipe:
    recipe = DatasetRecipe.from_json(serialized)
    if recipe.view is not None:
        try:
            _VIEW.validate_json(json.dumps(thaw(recipe.view)))
            if recipe.view["kind"] == "training" and recipe.view["split"] is not None:
                policy_from_record(thaw(recipe.view["split"]))
        except (ValueError, OverflowError) as error:
            raise INVALID_RECIPE.error(
                reason="use the discovered typed training or scoring view"
            ) from error
        if recipe.view["kind"] == "scoring":
            design = recipe.base_design
            if (
                design.shape != "cross_section"
                or not isinstance(design.anchor, FixedDate)
                or design.outcomes
                or set(recipe.view["features"]) != set(design.features)
            ):
                raise INVALID_RECIPE.error(
                    reason="scoring view must match its fixed feature-only base Design"
                )
            anchor = design.anchor.date
            resolved = (
                utc_instant(anchor) if isinstance(anchor, datetime) else model.day_boundary(anchor)
            )
            if utc_instant(recipe.view["as_of"]) != resolved:
                raise INVALID_RECIPE.error(
                    reason="scoring instant must match its base Design anchor"
                )
            document = json.loads(recipe.canonical_json())["base_design"]
            document["eligibility"] = thaw(recipe.view["eligibility"])
            try:
                eligibility_design = from_json(
                    json.dumps({"schema_version": 1, "design": document})
                )
            except (KeyError, TypeError, ValueError, OverflowError) as error:
                raise INVALID_RECIPE.error(
                    reason="scoring eligibility must use typed predicates"
                ) from error
            if eligibility_design.eligibility != design.eligibility:
                raise INVALID_RECIPE.error(reason="scoring eligibility must match its base Design")
    return recipe


def recipe_schema() -> dict[str, Any]:
    design = design_schema()
    view = _VIEW.json_schema()
    definitions = {**design["$defs"], **view.pop("$defs", {})}
    definitions["ScoringView"]["properties"]["eligibility"] = definitions["Design"]["properties"][
        "eligibility"
    ]
    return {
        "$schema": design["$schema"],
        "$defs": definitions,
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "base_design", "view"],
        "properties": {
            "schema_version": {"const": 1},
            "base_design": {"$ref": "#/$defs/Design"},
            "view": {"anyOf": [view, {"type": "null"}]},
        },
    }
