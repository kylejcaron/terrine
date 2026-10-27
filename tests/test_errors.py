import copy
import pickle

import pytest

from terrine.errors import RefusalSpec, TerrineError


def test_refusal_context_is_detached_and_deeply_immutable():
    context = {"source": ["events"], "bounds": {"start": 0}}
    error = TerrineError("unsafe input", code="terrine.example.unsafe", context=context)
    context["source"].append("other")
    assert error.context["source"] == ("events",)
    with pytest.raises(TypeError):
        error.context["bounds"]["start"] = 1
    with pytest.raises(AttributeError):
        error.code = "different"  # ty: ignore[invalid-assignment]


@pytest.mark.parametrize("origin", ["example", "manifest"])
def test_refusals_survive_pickle_and_deepcopy(origin):
    if origin == "manifest":
        from terrine.dataset.manifest import DatasetManifest

        with pytest.raises(TerrineError) as caught:
            DatasetManifest.from_json("{}")
        error = caught.value
        assert error.code == "terrine.dataset.invalid_manifest"
    else:
        error = RefusalSpec("terrine.example.unsafe", "Unsafe {source}").error(source=["events"])
    with pytest.raises(TypeError):
        error.context["reason"] = "changed"  # ty: ignore[invalid-assignment]
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
