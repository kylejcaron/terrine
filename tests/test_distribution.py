import tomllib
from pathlib import Path

import pytest

from tests.integration.distribution_smoke import check_notices


def test_all_direct_dependencies_and_optional_extras_have_notices():
    project = tomllib.loads(Path("pyproject.toml").read_text())["project"]
    check_notices(project, Path("THIRD_PARTY_NOTICES.txt").read_text())


def test_notice_inventory_requires_marker_guarded_optional_dependency():
    project = {
        "dependencies": ["example-base>=1"],
        "optional-dependencies": {"adapter": ["example_extra==2; python_version < '3.14'"]},
    }
    with pytest.raises(AssertionError):
        check_notices(project, "Package: example-base\n")
    check_notices(project, "Package: example-base\nPackage: example-extra\n")
