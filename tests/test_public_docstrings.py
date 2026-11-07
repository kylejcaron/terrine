import inspect
import re
from collections.abc import Iterator
from textwrap import dedent

import pytest

import terrine
import terrine.dataset
import terrine.export
import terrine.reasons
import terrine.recipes
import terrine.spec
import terrine.windows
from terrine.frame import Frame


def _public_objects() -> Iterator[tuple[str, object]]:
    for module in (
        terrine,
        terrine.recipes,
        terrine.spec,
        terrine.dataset,
        terrine.export,
        terrine.reasons,
        terrine.windows,
    ):
        for name in module.__all__:
            obj = getattr(module, name)
            if callable(obj):
                yield f"{module.__name__}.{name}", obj
    for owner, names in (
        (Frame, ()),
        (terrine.Dataset, ("to_arrow_batches", "to_pyarrow", "to_pandas", "to_polars")),
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
        if not names:
            yield owner.__name__, owner
        for name in names:
            yield f"{owner.__name__}.{name}", getattr(owner, name)


def _example_blocks(doc: str) -> Iterator[str]:
    for section in re.split(r"^Example:[ \t]*", doc, flags=re.M)[1:]:
        inline, *following = section.splitlines()
        body = []
        for line in following:
            if line and not line.startswith("    "):
                break
            body.append(line)
        yield "\n".join((inline, dedent("\n".join(body)))).strip()


@pytest.mark.parametrize(("name", "obj"), list(_public_objects()))
def test_public_names_document_themselves_with_an_example(name: str, obj: object):
    doc = inspect.getdoc(obj)
    assert doc, name
    assert all(_example_blocks(doc)), name


def test_public_callables_document_every_parameter():
    failures = []
    for name, obj in _public_objects():
        assert callable(obj), name
        doc = inspect.getdoc(obj) or ""
        args = re.search(r"^Args:\n(.*?)(?=^\S|\Z)", doc, re.M | re.S)
        documented = set(re.findall(r"^    \*{0,2}(\w+): .+", args[1], re.M)) if args else set()
        try:
            parameters = inspect.signature(obj).parameters
        except ValueError:
            parameters = {"message": None}
        missing = set(parameters) - {"self", "cls"} - documented
        if missing:
            failures.append(f"{name}: undocumented parameters {sorted(missing)}")
        if not inspect.isclass(obj) and not any(
            section in doc for section in ("Returns:\n", "Yields:\n")
        ):
            failures.append(f"{name}: missing Returns or Yields section")
    assert not failures, "\n".join(failures)
