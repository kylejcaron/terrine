import copy
import pickle

import pytest

from terrine.errors import RefusalSpec, TerrineError


def test_refusal_context_is_detached_deeply_immutable_and_portable():
    context = {"source": ["events"], "bounds": {"start": 0}}
    error = TerrineError("unsafe input", code="terrine.example.unsafe", context=context)
    context["source"].append("other")
    assert error.context["source"] == ("events",)
    with pytest.raises(TypeError):
        error.context["bounds"]["start"] = 1
    with pytest.raises(AttributeError):
        error.code = "different"  # ty: ignore[invalid-assignment]
    for restored in (copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
        assert restored.code == error.code
        assert restored.context == error.context
        assert str(restored) == str(error)


def test_refusal_template_and_invalid_context():
    spec = RefusalSpec("terrine.example.missing", "Missing {source}")
    error = spec.error(source="events")
    assert error.code == spec.code
    assert error.context["source"] == "events"
    with pytest.raises((TypeError, ValueError)):
        TerrineError("bad context", code=spec.code, context={"value": float("nan")})
