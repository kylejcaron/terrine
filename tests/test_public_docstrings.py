import inspect
from collections.abc import Iterator

import pytest

import terrine
import terrine.dataset
import terrine.export
import terrine.reasons
import terrine.recipes
import terrine.spec
from terrine.frame import Frame


def _public_objects() -> Iterator[tuple[str, object]]:
    for module in (
        terrine,
        terrine.recipes,
        terrine.spec,
        terrine.dataset,
        terrine.export,
        terrine.reasons,
    ):
        for name in module.__all__:
            obj = getattr(module, name)
            if callable(obj):
                yield f"{module.__name__}.{name}", obj
    for owner, names in (
        (terrine.Model, ("from_tables", "from_catalog", "from_increment", "to_catalog")),
        (terrine.Design, ("validate", "build", "describe", "snapshot")),
        (
            Frame,
            (
                "materialize",
                "to_pyarrow",
                "to_pandas",
                "to_polars",
                "training_set",
                "stage",
                "cohorts",
            ),
        ),
    ):
        for name in names:
            yield f"{owner.__name__}.{name}", getattr(owner, name)


@pytest.mark.parametrize(("name", "obj"), list(_public_objects()))
def test_public_names_document_themselves_with_an_example(name: str, obj: object):
    doc = inspect.getdoc(obj)
    assert doc, name
    assert doc.strip().splitlines()[-1].startswith("Example:"), name
