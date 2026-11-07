import griffe
import pytest

from scripts import api_check
from scripts.api_check import stable_breakages

_RELEASE = {
    "__init__.py": (
        "from terrine.model import Model\n"
        "from terrine.spec import agg, approx_distinct\n"
        '__all__ = ["Model", "agg", "approx_distinct"]\n'
    ),
    "spec/__init__.py": (
        "from terrine.spec.expressions import agg, approx_distinct\n"
        '__all__ = ["agg", "approx_distinct"]\n'
    ),
    "spec/expressions.py": (
        "def agg(metric, window):\n    pass\n\n\n"
        "def approx_distinct(metric, window):\n    pass\n\n\n"
        "def unexported(metric, window):\n    pass\n"
    ),
    "model.py": (
        '__all__ = ["Model"]\n\n\n'
        "class Model:\n"
        "    def from_tables(self, units, events):\n        pass\n\n"
        "    @classmethod\n"
        "    def from_increment(cls, path, con):\n        pass\n"
    ),
}


@pytest.fixture(autouse=True)
def _outside_repository(tmp_path, monkeypatch):
    # Griffe resolves a package name against the working directory first; load
    # the temporary `terrine` packages, never this checkout's.
    monkeypatch.chdir(tmp_path)


def _changed(path: str, old: str, new: str) -> dict[str, str]:
    assert old in _RELEASE[path]
    return {**_RELEASE, path: _RELEASE[path].replace(old, new)}


@pytest.mark.parametrize(
    ("path", "old", "new", "reported"),
    [
        pytest.param(
            "spec/expressions.py",
            "agg(metric, window)",
            "agg(metric)",
            ["terrine.agg"],
            id="re-exported-internal-function",
        ),
        pytest.param(
            "spec/expressions.py",
            "unexported(metric, window)",
            "unexported(metric)",
            [],
            id="unexported-function",
        ),
        pytest.param(
            "spec/expressions.py",
            "approx_distinct(metric, window)",
            "approx_distinct(metric)",
            [],
            id="provisional-function-through-aliases",
        ),
        pytest.param(
            "model.py",
            "from_increment(cls, path, con)",
            "from_increment(cls, path)",
            [],
            id="provisional-method-of-stable-class",
        ),
        pytest.param(
            "model.py",
            "from_tables(self, units, events)",
            "from_tables(self, units)",
            ["terrine.Model"],
            id="stable-method",
        ),
        pytest.param(
            "spec/__init__.py",
            '__all__ = ["agg", ',
            "__all__ = [",
            ["terrine.spec.agg"],
            id="name-removed-from-facade",
        ),
    ],
)
def test_stable_breakages_report_only_the_stable_exported_surface(path, old, new, reported):
    with (
        griffe.temporary_visited_package("terrine", _RELEASE) as release,
        griffe.temporary_visited_package("terrine", _changed(path, old, new)) as current,
    ):
        breakages = stable_breakages(release, current)

    assert [line.split(":", 1)[0] for line in breakages] == reported


def test_formerly_shared_exports_are_compared_with_their_own_current_targets():
    current = {
        **_RELEASE,
        "__init__.py": _RELEASE["__init__.py"].replace(
            "from terrine.spec import agg, approx_distinct",
            "from terrine.compat import agg\nfrom terrine.spec import approx_distinct",
        ),
        "compat.py": "def agg(metric, window):\n    pass\n",
        "spec/expressions.py": _RELEASE["spec/expressions.py"].replace(
            "agg(metric, window)", "agg(metric)"
        ),
    }
    with (
        griffe.temporary_visited_package("terrine", _RELEASE) as release,
        griffe.temporary_visited_package("terrine", current) as changed,
    ):
        breakages = stable_breakages(release, changed)
    assert [line.split(":", 1)[0] for line in breakages] == ["terrine.spec.agg"]


def test_release_tag_prefers_current_line(monkeypatch, tmp_path):
    def fake_git(root, *args):
        if "--is-shallow-repository" in args:
            return b"false"
        return b"v0.1.0\n" if "v0.*" in args else b"v1.0.0rc1\n"

    monkeypatch.setattr(api_check, "_git", fake_git)

    assert api_check.release_tag(tmp_path, "0.2.0") == "v0.1.0"
    assert api_check.release_tag(tmp_path, "1.0.0rc1") == "v1.0.0rc1"

def test_release_tag_ignores_development_tags(monkeypatch, tmp_path):
    def fake_git(root, *args):
        if "--is-shallow-repository" in args:
            return b"false"
        return b"v0.1.0.dev2\n" if "v0.*" in args else b""

    monkeypatch.setattr(api_check, "_git", fake_git)

    assert api_check.release_tag(tmp_path, "0.1.0") is None



def test_release_tag_fallback_parses_development_version(monkeypatch, tmp_path):
    def fake_git(root, *args):
        if "--is-shallow-repository" in args:
            return b"false"
        return b"v0.1.0.dev2\n" if "v0.*" in args else b""

    monkeypatch.setattr(api_check, "Version", None)
    monkeypatch.setattr(api_check, "_git", fake_git)

    assert api_check.release_tag(tmp_path, "0.1.0.dev2") is None
def test_release_tag_none_when_line_untagged(monkeypatch, tmp_path):
    def fake_git(root, *args):
        return b"false" if "--is-shallow-repository" in args else b""

    monkeypatch.setattr(api_check, "_git", fake_git)

    assert api_check.release_tag(tmp_path, "0.2.0") is None

def test_release_tag_excludes_v1_dev_alpha_and_beta_tags(monkeypatch, tmp_path):
    def fake_git(root, *args):
        if "--is-shallow-repository" in args:
            return b"false"
        return b"v1.1.0a1\nv1.1.0b1\nv1.1.0.dev3\nv1.0.0rc1\n"

    monkeypatch.setattr(api_check, "_git", fake_git)

    assert api_check.release_tag(tmp_path, "1.1.0") == "v1.0.0rc1"


def test_release_tag_fallback_treats_dev_release_of_rc_as_v1(monkeypatch, tmp_path):
    def fake_git(root, *args):
        if "--is-shallow-repository" in args:
            return b"false"
        return b"v1.1.0a1\nv1.1.0b1\nv1.1.0.dev3\nv1.0.0rc1\n" if "v1.*" in args else b""

    monkeypatch.setattr(api_check, "Version", None)
    monkeypatch.setattr(api_check, "_git", fake_git)

    assert api_check.release_tag(tmp_path, "1.0.0rc2.dev1") == "v1.0.0rc1"

def test_release_tag_fallback_keeps_rc1_dev_before_v1_boundary(monkeypatch, tmp_path):
    def fake_git(root, *args):
        if "--is-shallow-repository" in args:
            return b"false"
        return b"v0.1.0\n" if "v0.*" in args else b"v1.0.0rc1\n"

    monkeypatch.setattr(api_check, "Version", None)
    monkeypatch.setattr(api_check, "_git", fake_git)

    assert api_check.release_tag(tmp_path, "1.0.0rc1.dev1") == "v0.1.0"

def test_release_tag_fallback_treats_rc1_post_development_as_v1(monkeypatch, tmp_path):
    def fake_git(root, *args):
        if "--is-shallow-repository" in args:
            return b"false"
        return b"v1.0.0rc1\n" if "v1.*" in args else b""

    monkeypatch.setattr(api_check, "Version", None)
    monkeypatch.setattr(api_check, "_git", fake_git)

    assert api_check.release_tag(tmp_path, "1.0.0rc1.post1.dev1") == "v1.0.0rc1"


def test_signature_type_leak_detected(tmp_path, monkeypatch):
    package = tmp_path / "terrine"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "api.py").write_text(
        "class Hidden:\n"
        "    pass\n\n"
        "def f(x: Hidden):\n"
        "    pass\n"
    )
    monkeypatch.chdir(tmp_path)
    module = griffe.load("terrine.api", search_paths=[tmp_path])

    assert api_check.check_signature_types({"terrine.api.f": module["f"]}) == [
        "terrine.api.f: terrine.api.Hidden"
    ]


def test_real_surface_has_no_signature_type_leaks():
    package = api_check._load(api_check.Path(__file__).resolve().parents[1])

    assert api_check.check_signature_types(api_check._stable_objects(package)) == []


def test_main_fails_when_stable_signature_names_hidden_type(tmp_path, monkeypatch):
    package = tmp_path / "terrine"
    package.mkdir()
    (package / "__init__.py").write_text(
        "from .api import f\n"
        '__all__ = ["f"]\n'
    )
    (package / "api.py").write_text(
        "class Hidden:\n"
        "    pass\n\n"
        "def f(value: Hidden):\n"
        "    pass\n"
    )
    module = griffe.load("terrine", search_paths=[tmp_path])
    repository = api_check.Path(__file__).resolve().parents[1]
    monkeypatch.setattr(
        api_check,
        "_git",
        lambda root, *args: str(repository).encode(),
    )
    monkeypatch.setattr(api_check, "_load", lambda root: module)
    monkeypatch.setattr(api_check, "release_tag", lambda root, version: None)
    monkeypatch.setattr(api_check, "_newest_minor_release_tag", lambda root, version: None)

    assert api_check.main([]) == 1


def test_constructor_signature_type_leak_detected(tmp_path, monkeypatch):
    package = tmp_path / "terrine"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "api.py").write_text(
        "class Hidden:\n"
        "    pass\n\n"
        "class Public:\n"
        "    def __init__(self, value: Hidden):\n"
        "        pass\n"
    )
    monkeypatch.chdir(tmp_path)
    module = griffe.load("terrine.api", search_paths=[tmp_path])

    assert api_check.check_signature_types({"terrine.api.Public": module["Public"]}) == [
        "terrine.api.Public.__init__: terrine.api.Hidden"
    ]


def test_qualified_stable_annotation_has_no_module_prefix_leak(tmp_path, monkeypatch):
    package = tmp_path / "terrine"
    package.mkdir()
    (package / "__init__.py").write_text(
        "from .api import f\n"
        "from .types import Hidden\n"
        '__all__ = ["f", "Hidden"]\n'
    )
    (package / "types.py").write_text("class Hidden:\n    pass\n")
    (package / "api.py").write_text(
        "import terrine.types as types\n\n"
        "def f(value: types.Hidden):\n"
        "    pass\n"
    )
    monkeypatch.chdir(tmp_path)
    module = griffe.load("terrine", search_paths=[tmp_path])

    assert api_check.check_signature_types(
        {"terrine.Hidden": module["Hidden"], "terrine.f": module["f"]}
    ) == []


def test_unclassified_type_is_not_hidden_by_stable_nested_type(tmp_path, monkeypatch):
    package = tmp_path / "terrine"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "api.py").write_text(
        "class Hidden:\n"
        "    class Public:\n"
        "        pass\n\n"
        "def f(value: Hidden | Hidden.Public):\n"
        "    pass\n"
    )
    monkeypatch.chdir(tmp_path)
    module = griffe.load("terrine.api", search_paths=[tmp_path])

    assert api_check.check_signature_types(
        {
            "terrine.api.Hidden.Public": module["Hidden"]["Public"],
            "terrine.api.f": module["f"],
        }
    ) == ["terrine.api.f: terrine.api.Hidden"]


def test_handwritten_pydantic_constructor_is_not_exempt(tmp_path, monkeypatch):
    package = tmp_path / "terrine"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "api.py").write_text(
        "from pydantic import BaseModel\n\n"
        "class Hidden:\n"
        "    pass\n\n"
        "class Public(BaseModel):\n"
        "    def __init__(self, value: Hidden):\n"
        "        super().__init__()\n"
    )
    monkeypatch.chdir(tmp_path)
    module = griffe.load("terrine.api", search_paths=[tmp_path])
    public = module["Public"]
    constructor = public.members["__init__"]

    assert not api_check._is_generated_pydantic_init(public, "__init__", constructor)
    assert api_check.check_signature_types({"terrine.api.Public": public}) == [
        "terrine.api.Public.__init__: terrine.api.Hidden"
    ]


def test_imported_handwritten_pydantic_constructor_is_not_exempt(tmp_path, monkeypatch):
    package = tmp_path / "terrine"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "helpers.py").write_text(
        "class Hidden:\n"
        "    pass\n\n"
        "def custom_init(self, value: Hidden):\n"
        "    pass\n"
    )
    (package / "api.py").write_text(
        "from pydantic import BaseModel\n\n"
        "class Public(BaseModel):\n"
        "    from terrine.helpers import custom_init as __init__\n"
    )
    monkeypatch.chdir(tmp_path)
    module = griffe.load("terrine.api", search_paths=[tmp_path])
    public = module["Public"]
    constructor = public.members["__init__"]

    assert not api_check._is_generated_pydantic_init(public, "__init__", constructor)
    assert api_check.check_signature_types({"terrine.api.Public": public}) == [
        "terrine.api.Public.__init__: terrine.helpers.Hidden"
    ]
