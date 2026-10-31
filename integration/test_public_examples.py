from __future__ import annotations

import inspect

import ibis
import pytest

import terrine as tr
import terrine.spec as spec
from tests.fixtures.readme import readme_design, readme_model, readme_tables
from tests.test_public_docstrings import _example_blocks, _public_objects

_OPTIONAL_EXAMPLE_MODULES = {"Model.from_increment": "increment.semantics"}


@pytest.fixture(scope="module")
def example_namespace(tmp_path_factory):
    con = ibis.duckdb.connect(str(tmp_path_factory.mktemp("public_examples") / "shop.duckdb"))
    tables = readme_tables()
    for name, table in tables.items():
        con.create_table(name, table)
    model = readme_model(con)
    design = readme_design()
    frame = design.build(model)
    dataset = frame.stage("customer_spend")
    yield {
        "tr": tr,
        "spec": spec,
        "model": model,
        "design": design,
        "frame": frame,
        "orders": tables["orders"].to_pandas(),
        "customers": tables["customers"].to_pandas(),
        "dataset": dataset,
    }
    con.disconnect()


@pytest.mark.parametrize(
    "optional_module", [None, "increment.semantics"], ids=["core", "increment"]
)
def test_public_examples_execute(example_namespace, optional_module):
    if optional_module is not None:
        pytest.importorskip(optional_module)
    failures = []
    for name, obj in _public_objects():
        if _OPTIONAL_EXAMPLE_MODULES.get(name) != optional_module:
            continue
        doc = inspect.getdoc(obj) or ""
        examples = list(_example_blocks(doc))
        if not examples:
            failures.append(f"{name}: missing Example")
        for example in examples:
            try:
                exec(compile(example, f"<{name}>", "exec"), dict(example_namespace))
            except Exception as error:
                failures.append(f"{name}: {example}\n  {type(error).__name__}: {error}")
    assert not failures, "\n".join(failures)
