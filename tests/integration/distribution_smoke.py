"""Check source/wheel contents and execute the installed first-dataset tutorial."""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tomllib
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from zipfile import ZipFile


def _normalized(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def check_notices(project: dict, notices: str) -> None:
    requirements = [
        *project["dependencies"],
        *(item for group in project["optional-dependencies"].values() for item in group),
    ]
    required = set()
    for requirement in requirements:
        match = re.match(r"[A-Za-z0-9_.-]+", requirement)
        assert match is not None, requirement
        required.add(_normalized(match[0]))
    documented = {
        _normalized(name) for name in re.findall(r"^Package: (\S+)", notices, flags=re.MULTILINE)
    }
    assert not (missing := required - documented), f"Missing dependency notices: {sorted(missing)}"


def check_artifacts(wheel: Path, sdist: Path) -> None:
    with ZipFile(wheel) as archive:
        wheel_names = set(archive.namelist())
        required = {"terrine/__init__.py", "terrine/py.typed", "terrine/simulate/baseline.yaml"}
        assert required <= wheel_names, f"Missing wheel files: {required - wheel_names}"
        license_files = {
            PurePosixPath(name).name for name in wheel_names if "/licenses/" in name
        }
        expected_licenses = {"LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.txt"}
        assert expected_licenses <= license_files, "Wheel must retain license and notices"
        notices = [
            name for name in wheel_names if name.endswith("/licenses/THIRD_PARTY_NOTICES.txt")
        ]
        assert len(notices) == 1, "Wheel must retain third-party notices in distribution metadata"
        wheel_notices = archive.read(notices[0]).decode()
        metadata_name = next(name for name in wheel_names if name.endswith(".dist-info/METADATA"))
        metadata = BytesParser().parsebytes(archive.read(metadata_name))
    with tarfile.open(sdist) as archive:
        members = archive.getmembers()
        assert all(
            not PurePosixPath(item.name).is_absolute()
            and ".." not in PurePosixPath(item.name).parts
            for item in members
        ), "Unsafe sdist path"
        roots = {PurePosixPath(item.name).parts[0] for item in members}
        assert len(roots) == 1, "sdist must have one root"
        source_names = {"/".join(PurePosixPath(item.name).parts[1:]) for item in members}
        required = {
            "README.md",
            "CHANGELOG.md",
            "LICENSE",
            "NOTICE",
            "THIRD_PARTY_NOTICES.txt",
            "pyproject.toml",
            "uv.lock",
            "mkdocs.yml",
            "docs/index.md",
            "docs/guides/index.md",
            "docs/reference/refusals.md",
            "examples/first_dataset.py",
            "tests/documentation.py",
            "tests/integration/distribution_smoke.py",
            "tests/test_optional_dependencies.py",
            "terrine/py.typed",
            "terrine/simulate/baseline.yaml",
        }
        assert required <= source_names, f"Missing sdist files: {required - source_names}"
        forbidden = {".git", ".venv", ".superpowers", ".worktrees", "__pycache__"}
        assert not any(forbidden.intersection(PurePosixPath(name).parts) for name in source_names)
        assert not any(name.endswith((".duckdb", ".duckdb.wal", ".pyc")) for name in source_names)
        prefix = next(iter(roots))
        project_file = archive.extractfile(f"{prefix}/pyproject.toml")
        notice_file = archive.extractfile(f"{prefix}/THIRD_PARTY_NOTICES.txt")
        assert project_file is not None and notice_file is not None
        project = tomllib.loads(project_file.read().decode())["project"]
        source_notices = notice_file.read().decode()
        check_notices(project, source_notices)
        assert wheel_notices == source_notices, "Wheel/sdist notices differ"
        assert metadata["Name"] == project["name"]
        assert metadata["Version"] == project["version"]
        assert metadata["License-Expression"] == project["license"] == "Apache-2.0"
        assert {item.strip() for item in metadata["Requires-Python"].split(",")} == {
            item.strip() for item in project["requires-python"].split(",")
        }
    print("Wheel/sdist contents, metadata and direct-dependency notices verified")


def _run(command: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True, timeout=600)


def smoke(wheel: Path, sdist: Path, *, python: str, resolution: str) -> None:
    check_artifacts(wheel, sdist)
    uv = shutil.which("uv")
    assert uv is not None, "uv is required for isolated distribution acceptance"
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "PYTHONPATH",
            "PYTHONHOME",
            "VIRTUAL_ENV",
            "TERRINE_RUNTIME_NAMESPACE",
            "TERRINE_RUNTIME_SNAPSHOT_ROOT",
        }
    }
    with TemporaryDirectory(prefix="terrine-distribution-") as directory:
        root = Path(directory)
        source = root / "source"
        with tarfile.open(sdist) as archive:
            archive.extractall(source, filter="data")
        project_root = next(source.iterdir())
        rebuilt = root / "rebuilt"
        _run(
            [uv, "build", "--wheel", str(project_root), "--out-dir", str(rebuilt)],
            cwd=root,
            env=env,
        )
        rebuilt_wheel = next(rebuilt.glob("*.whl"))
        check_artifacts(rebuilt_wheel, sdist)
        with ZipFile(wheel) as original, ZipFile(rebuilt_wheel) as rebuilt_archive:
            original_files = {
                name: original.read(name)
                for name in original.namelist()
                if name.startswith("terrine/")
            }
            rebuilt_files = {
                name: rebuilt_archive.read(name)
                for name in rebuilt_archive.namelist()
                if name.startswith("terrine/")
            }
            assert original_files == rebuilt_files, "sdist rebuild changed package contents"
        consumer = root / "consumer"
        consumer.mkdir()
        environment = root / "venv"
        _run([uv, "venv", "--python", python, str(environment)], cwd=consumer, env=env)
        executable = environment / "bin/python"

        def install(extra: bool) -> None:
            if resolution == "lowest-direct":
                requirements = root / ("native.txt" if extra else "base.txt")
                command = [
                    uv,
                    "pip",
                    "compile",
                    str(project_root / "pyproject.toml"),
                    "--python",
                    str(executable),
                    "--resolution",
                    "lowest-direct",
                    "--only-binary",
                    ":all:",
                    "--output-file",
                    str(requirements),
                ]
                if extra:
                    command += ["--extra", "duckdb"]
                _run(command, cwd=consumer, env=env)
                _run(
                    [uv, "pip", "install", "--python", str(executable), "-r", str(requirements)],
                    cwd=consumer,
                    env=env,
                )
                _run(
                    [
                        uv,
                        "pip",
                        "install",
                        "--python",
                        str(executable),
                        "--no-deps",
                        str(rebuilt_wheel),
                    ],
                    cwd=consumer,
                    env=env,
                )
            else:
                requirement = f"{rebuilt_wheel}[duckdb]" if extra else str(rebuilt_wheel)
                _run(
                    [uv, "pip", "install", "--python", str(executable), requirement],
                    cwd=consumer,
                    env=env,
                )

        install(False)
        isolation = project_root / "tests/test_optional_dependencies.py"
        probe = (
            "import importlib.metadata as m, runpy, pathlib, terrine; "
            f"assert pathlib.Path(terrine.__file__).resolve().is_relative_to("
            f"pathlib.Path({str(environment)!r}).resolve()); "
            "assert not {'duckdb','pandas','polars','increment','datasketches','apache-airflow',"
            "'dagster','dbt-core','doubleml','dowhy','duckreg','lifelines','diff-diff'} & "
            "{d.metadata['Name'].lower() for d in m.distributions()}; "
            f"runpy.run_path({str(isolation)!r})"
            "['test_backend_metadata_imports_without_optional_drivers_or_dataframes']()"
        )
        _run([str(executable), "-I", "-c", probe], cwd=consumer, env=env)
        install(True)
        _run(
            [str(executable), "-I", str(project_root / "examples/first_dataset.py")],
            cwd=consumer,
            env=env,
        )
        versions = (
            "import importlib.metadata as m, sys; print(sys.version); "
            "print({n:m.version(n) for n in ('terrine','ibis-framework','duckdb','pydantic',"
            "'PyYAML','sqlglot','pyarrow','numpy')})"
        )
        _run([str(executable), "-I", "-c", versions], cwd=consumer, env=env)
        print(f"Clean-wheel tutorial and optional import isolation passed ({resolution})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--sdist", required=True, type=Path)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--resolution", choices=("highest", "lowest-direct"), default="highest")
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    if args.check_only:
        check_artifacts(args.wheel.resolve(), args.sdist.resolve())
    else:
        smoke(
            args.wheel.resolve(),
            args.sdist.resolve(),
            python=args.python,
            resolution=args.resolution,
        )


if __name__ == "__main__":
    main()
