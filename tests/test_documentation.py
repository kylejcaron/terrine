from pathlib import Path

import pytest


def test_refusal_reference_covers_all_module_declarations():
    from tests.documentation import refusal_declarations, render_refusals

    root = Path(__file__).resolve().parents[1]
    declarations = refusal_declarations(root / "terrine")
    assert declarations
    assert (root / "docs/reference/refusals.md").read_text() == render_refusals(declarations)


def test_refusal_discovery_retains_inline_and_duplicate_code_variants(tmp_path):
    from tests.documentation import refusal_declarations, render_refusals

    (tmp_path / "declared.py").write_text(
        "from terrine.errors import RefusalSpec\n"
        '_MISSING = RefusalSpec("terrine.model.invalid_binding", "Missing {name!r}.")\n'
        "def validate():\n"
        '    raise RefusalSpec("terrine.model.invalid_binding", '
        '"Invalid {reason}.").error(reason="x")\n'
    )
    declarations = refusal_declarations(tmp_path)
    assert [(item.code, item.fields) for item in declarations] == [
        ("terrine.model.invalid_binding", ("name",)),
        ("terrine.model.invalid_binding", ("reason",)),
    ]
    rendered = render_refusals(declarations)
    assert "Missing {name!r}." in rendered
    assert "Invalid {reason}." in rendered
    (tmp_path / "dynamic.py").write_text('RefusalSpec(dynamic_code, "Unsafe omission")')
    with pytest.raises(ValueError):
        refusal_declarations(tmp_path)


def test_emitted_refusals_have_documented_interpolation_fields_and_routes():
    from terrine import Coverage, Window
    from terrine.backends.capabilities import UNVERIFIED_CAPABILITIES
    from terrine.errors import TerrineError
    from tests.documentation import recovery_guide, refusal_declarations

    declared = refusal_declarations(Path(__file__).resolve().parents[1] / "terrine")
    operations = (
        lambda: Window("1d", "0s"),
        lambda: Coverage("9999-12-31", "9999-12-31"),
        lambda: UNVERIFIED_CAPABILITIES.require(
            "arrow_batches", backend="other", route="Use local DuckDB."
        ),
    )
    for operation in operations:
        with pytest.raises(TerrineError) as caught:
            operation()
        matches = [item for item in declared if item.code == caught.value.code]
        assert matches
        assert any(set(item.fields) <= caught.value.context.keys() for item in matches)
        assert recovery_guide(caught.value.code)[0].endswith(".md")


def test_published_documentation_links_resolve():
    from tests.documentation import broken_links

    root = Path(__file__).resolve().parents[1]
    assert not broken_links(root)
