"""Report signature breakages of terrine's stable API against a 1.x release.

The stable surface is every name in a stable facade's ``__all__``, resolved
through re-exports to its definition, together with that definition's members.
Provisional names are excluded after resolution, including provisional members
of stable classes. Formats, numerics and outputs are checked elsewhere.

Exit status: 0 when the stable API is compatible or no 1.x release is tagged,
1 when a stable signature broke or a current stable export does not resolve,
2 when the release cannot be loaded.
"""

from __future__ import annotations

import argparse
import io
import re
import subprocess
import tarfile
import tomllib
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory

import griffe

try:
    from packaging.version import Version
except ImportError:
    Version = None


STABLE_FACADES: tuple[str, ...] = (
    "terrine",
    "terrine.recipes",
    "terrine.metrics",
    "terrine.spec",
    "terrine.windows",
    "terrine.errors",
    "terrine.dataset",
    "terrine.model",
    "terrine.frame",
    "terrine.data",
    "terrine.reasons",
)
PROVISIONAL_NAMES: frozenset[str] = frozenset(
    {
        "terrine.spec.approx_distinct",
        "terrine.spec.approx_quantile",
        "terrine.model.Model.from_increment",
        "terrine.Dataset",
        "terrine.frame.Frame.stage",
        "terrine.frame.Frame.expr",
        "terrine.spec.Sequential",
        "terrine.spec.EventPanel",
        "terrine.spec.CalendarPanel",
        "terrine.spec.CalendarComparisonPanel",
        "terrine.spec.matched_t0",
        "terrine.spec.CalendarComparisonSpec",
        "terrine.windows.duration",
        "terrine.data.ApproximationMeta",
        "terrine.data.CalendarComparisonMeta",
        "terrine.data.CohortOperationMeta",
        "terrine.data.ColumnMeta",
        "terrine.data.DatasetViewMeta",
        "terrine.data.ExpressionTiming",
        "terrine.data.FeatureCutoff",
        "terrine.data.Preview",
        "terrine.data.Provenance",
        "terrine.data.Roles",
        "terrine.data.SamplingMeta",
        "terrine.data.SequenceMeta",
        "terrine.data.StatePanelMeta",
        "terrine.data.TreatmentHistoryMeta",
        "terrine.data.ExecutionSnapshot",
        "terrine.data.CoverageSummary",
        "terrine.data.EligibilitySummary",
        "terrine.data.MaterializedFrame",
        "terrine.execution.inputs.VerifiedInputBinding",
        "terrine.runtime.changes.ChangeSet",
        "terrine.runtime.planner.RefreshPlan",
        "terrine.dataset.manifest.DatasetManifest",
        "terrine.execution.inputs.PhysicalRelationId",
        "terrine.execution.inputs.RelationHandle",
        "terrine.data.DataFrameLike",
        "terrine.data.Where",
        "terrine.spec.expressions.JsonScalar",
        "terrine.spec.expressions.MetricAgg",
        "terrine.spec.expressions.AttributeAsOf",
        "terrine.spec.expressions.RetentionMetric",
        "terrine.spec.expressions.MetricQuantile",
        "terrine.spec.expressions.Eq",
        "terrine.spec.expressions.Gt",
        "terrine.spec.expressions.IsNull",
        "terrine.spec.expressions.And",
    }
)

_PACKAGE = "terrine"
_PRERELEASE_SUFFIXES = ("a", "b", "rc")
_FIRST_STABLE_LINE_VERSION = "1.0.0rc1"


class HistoryUnavailable(Exception):
    """The release to compare with cannot be read from this checkout."""


def stable_surface(package: griffe.Module) -> dict[str, str]:
    """Map each stable facade-qualified name to the path of its definition.

    Args:
        package: The loaded ``terrine`` package.

    Returns:
        Facade-qualified names, such as ``terrine.spec.agg``, and their resolved
        definition paths, without provisional names.
    """
    provisional = _provisional_paths(package)
    surface = {}
    for facade_path, facade in _facades(package):
        for name in _exports(facade):
            qualified = f"{facade_path}.{name}"
            member = facade.members.get(name)
            target = None if member is None else _resolved(member)
            if target is None or target.is_alias:
                raise LookupError(f"{qualified} is exported but does not resolve to a definition")
            if qualified not in PROVISIONAL_NAMES and not _within({target.path}, provisional):
                surface[qualified] = target.path
    return surface


def stable_breakages(old: griffe.Module, new: griffe.Module) -> list[str]:
    """Describe stable names that disappeared or whose signatures broke.

    Args:
        old: The released package.
        new: The current package.

    Returns:
        One ``<stable name>: <change>`` line per breakage, ordered by name.
    """
    provisional = _provisional_paths(old) | _provisional_paths(new)
    current = {f"{path}.{name}" for path, facade in _facades(new) for name in _exports(facade)}
    reports = []
    compared = set()
    for name, target in sorted(stable_surface(old).items()):
        if name not in current:
            reports.append(f"{name}: removed from the stable API")
            continue
        # A stable facade exported as a module is checked through its own names.
        if target in STABLE_FACADES:
            continue
        replacement = _lookup(new, name)
        if replacement is None or _resolved(replacement).is_alias:
            reports.append(f"{name}: no longer resolves to a definition")
            continue
        replacement = _resolved(replacement)
        # Exports that shared a definition may now point to different ones;
        # each pair of released and current definitions is compared once.
        if (target, replacement.path) in compared:
            continue
        compared.add((target, replacement.path))
        released = _resolved(_require(old, name))
        for breakage in _signature_breakages(released, replacement):
            if not _within(_paths(breakage.obj), provisional):
                explanation = breakage.explain(style=griffe.ExplanationStyle.MARKDOWN)
                reports.append(f"{name}: {explanation.removeprefix('- ')}")
    return reports


def _signature_breakages(
    old: griffe.Object | griffe.Alias, new: griffe.Object | griffe.Alias
) -> Iterator[griffe.Breakage]:
    # griffe compares the members of the objects it is given; wrapping each
    # definition compares its own signature too.
    released, current = griffe.Module("released"), griffe.Module("current")
    released.set_member("api", griffe.Alias("api", old))
    current.set_member("api", griffe.Alias("api", new))
    return griffe.find_breaking_changes(released, current)


def _facades(package: griffe.Module) -> Iterator[tuple[str, griffe.Module]]:
    for path in STABLE_FACADES:
        facade = _lookup(package, path)
        if facade is not None:
            facade = _resolved(facade)
            if isinstance(facade, griffe.Module):
                yield path, facade


def _exports(facade: griffe.Module) -> list[str]:
    return [str(name) for name in facade.exports or ()]


def _provisional_paths(package: griffe.Module) -> set[str]:
    paths = set()
    for name in PROVISIONAL_NAMES:
        obj = _lookup(package, name)
        if obj is not None:
            paths |= _paths(obj)
    return paths


def _lookup(package: griffe.Module, path: str) -> griffe.Object | griffe.Alias | None:
    if path == package.path:
        return package
    try:
        return package[path.removeprefix(f"{package.path}.")]
    except (KeyError, griffe.AliasResolutionError, griffe.CyclicAliasError):
        return None


def _require(package: griffe.Module, path: str) -> griffe.Object | griffe.Alias:
    obj = _lookup(package, path)
    if obj is None:
        raise LookupError(f"{path} does not resolve to a definition")
    return obj


def _resolved(obj: griffe.Object | griffe.Alias) -> griffe.Object | griffe.Alias:
    if not isinstance(obj, griffe.Alias):
        return obj
    try:
        return obj.final_target
    except (griffe.AliasResolutionError, griffe.CyclicAliasError):
        return obj


def _paths(obj: griffe.Object | griffe.Alias) -> set[str]:
    return {obj.path, _resolved(obj).path}


def _within(paths: set[str], owners: set[str]) -> bool:
    return any(path == owner or path.startswith(f"{owner}.") for path in paths for owner in owners)


def _stable_objects(package: griffe.Module) -> dict[str, griffe.Object]:
    """Resolve stable facade exports to the definitions used for annotations."""
    provisional = _provisional_paths(package)
    surface = {}
    for facade_path, facade in _facades(package):
        for name in _exports(facade):
            qualified = f"{facade_path}.{name}"
            member = facade.members.get(name)
            target = None if member is None else _resolved(member)
            if target is None or target.is_alias:
                continue
            if qualified not in PROVISIONAL_NAMES and not _within({target.path}, provisional):
                surface[qualified] = target
    return surface


def _annotation_names(annotation: griffe.Expr) -> Iterator[str]:
    if isinstance(annotation, str):
        return
    for expression in annotation.iterate():
        if not isinstance(expression, griffe.ExprName | griffe.ExprAttribute):
            continue
        path = expression.canonical_path
        if path is not None and path.startswith(f"{_PACKAGE}."):
            yield path

def check_signature_types(surface: Mapping[str, griffe.Object]) -> list[str]:
    """Find stable callables whose annotations name non-stable Terrine types."""
    stable_paths = {name for name in surface}
    stable_paths.update(_resolved(obj).path for obj in surface.values())
    violations = []

    def check_callable(qualified: str, obj: griffe.Object | griffe.Alias) -> None:
        target = _resolved(obj)
        if not isinstance(target, griffe.Function):
            return
        annotations = [
            parameter.annotation
            for parameter in target.parameters
            if parameter.annotation is not None
        ]
        if target.returns is not None:
            annotations.append(target.returns)
        found = set()
        for annotation in annotations:
            for name in _annotation_names(annotation):
                if name not in stable_paths and name not in PROVISIONAL_NAMES:
                    found.add(name)
        violations.extend(f"{qualified}: {name}" for name in sorted(found))

    for qualified, obj in surface.items():
        target = _resolved(obj)
        if isinstance(target, griffe.Class):
            for name, member in target.members.items():
                member_name = f"{qualified}.{name}"
                member_path = f"{target.path}.{name}"
                if (
                    not name.startswith("_")
                    and member_name not in PROVISIONAL_NAMES
                    and member_path not in PROVISIONAL_NAMES
                ):
                    check_callable(member_name, member)
        check_callable(qualified, obj)
    return violations


def _git(root: Path, *args: str) -> bytes:
    return subprocess.run(["git", *args], cwd=root, check=True, capture_output=True).stdout


def release_tag(root: Path, current_version: str) -> str | None:
    """Return the newest release tag on the line containing current_version."""
    if Version is not None:
        stable_line = Version(current_version) >= Version(_FIRST_STABLE_LINE_VERSION)
    else:
        match = re.fullmatch(
            r"(\d+)\.(\d+)\.(\d+)(?:(a|b|rc)(\d+))?(?:\.post(\d+))?"
            r"(?:\.dev(\d+))?(?:\+[a-zA-Z0-9.-]+)?",
            current_version,
        )
        if match is None:
            raise ValueError(f"invalid package version: {current_version}")
        major, minor, patch = (int(part) for part in match.group(1, 2, 3))
        prerelease, prerelease_number = match.group(4), match.group(5)
        postrelease, devrelease = match.group(6), match.group(7)
        rc_reached = prerelease == "rc" and (
            int(prerelease_number) > 1
            or (int(prerelease_number) == 1 and (postrelease is not None or devrelease is None))
        )
        stable_line = (major, minor, patch) > (1, 0, 0) or (
            (major, minor, patch) == (1, 0, 0)
            and ((prerelease is None and devrelease is None) or rc_reached)
        )
    line = "v1.*" if stable_line else "v0.*"
    if _git(root, "rev-parse", "--is-shallow-repository").strip() == b"true":
        raise HistoryUnavailable("the clone is shallow; fetch full history and tags")
    ordering = [f"versionsort.suffix={suffix}" for suffix in _PRERELEASE_SUFFIXES]
    options = [part for setting in ordering for part in ("-c", setting)]
    tags = _git(root, *options, "tag", "--list", line, "--sort=-v:refname").split()
    tags = [
        tag
        for tag in tags
        if _is_eligible_release_tag(tag.decode(), allow_release_candidate=stable_line)
    ]
    return tags[0].decode() if tags else None

def _is_eligible_release_tag(tag: str, *, allow_release_candidate: bool) -> bool:
    version = tag.removeprefix("v")
    if Version is not None:
        try:
            parsed = Version(version)
        except ValueError:
            return False
        if parsed.is_devrelease or parsed.is_postrelease:
            return False
        return not parsed.is_prerelease or (
            allow_release_candidate and parsed.pre[0] == "rc"
        )
    match = re.fullmatch(
        r"\d+\.\d+\.\d+(?:(a|b|rc)\d+)?(?:\.post\d+)?(?:\.dev\d+)?"
        r"(?:\+[a-zA-Z0-9.-]+)?",
        version,
    )
    if match is None or ".post" in version or ".dev" in version:
        return False
    prerelease = match.group(1)
    return prerelease is None or (allow_release_candidate and prerelease == "rc")


def _release_minor(version: str) -> tuple[int, int]:
    if Version is not None:
        parsed = Version(version)
        return parsed.release[0], parsed.release[1] if len(parsed.release) > 1 else 0
    match = re.match(r"(\d+)\.(\d+)", version)
    if match is None:
        raise ValueError(f"invalid package version: {version}")
    return int(match.group(1)), int(match.group(2))


def _newest_minor_release_tag(root: Path, version: str) -> str | None:
    major, minor = _release_minor(version)
    ordering = [f"versionsort.suffix={suffix}" for suffix in _PRERELEASE_SUFFIXES]
    options = [part for setting in ordering for part in ("-c", setting)]
    tags = _git(
        root, *options, "tag", "--list", f"v{major}.{minor}.*", "--sort=-v:refname"
    ).split()
    final_tags = [
        tag
        for tag in tags
        if _is_eligible_release_tag(tag.decode(), allow_release_candidate=False)
    ]
    return final_tags[0].decode() if final_tags else None


def _load(search_path: Path) -> griffe.Module:
    package = griffe.load(
        _PACKAGE, search_paths=[search_path], try_relative_path=False, allow_inspection=False
    )
    if not isinstance(package, griffe.Module):
        raise ImportError(f"{_PACKAGE} in {search_path} is not a package")
    return package


def _load_release(root: Path, tag: str, destination: Path) -> griffe.Module:
    archive = _git(root, "archive", "--format=tar", tag, "--", _PACKAGE)
    with tarfile.open(fileobj=io.BytesIO(archive)) as members:
        members.extractall(destination, filter="data")
    return _load(destination)


def _reason(error: Exception) -> str:
    if isinstance(error, subprocess.CalledProcessError) and error.stderr:
        return error.stderr.decode(errors="replace").strip()
    return str(error)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check terrine's stable API signatures.")
    parser.add_argument(
        "--against", metavar="TAG", help="release to compare with (default: newest tag on the current release line)"
    )
    args = parser.parse_args(argv)
    try:
        root = Path(_git(Path.cwd(), "rev-parse", "--show-toplevel").decode().strip())
        current_version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
        current_major, current_minor = _release_minor(current_version)
        tag = args.against or release_tag(root, current_version)
        informational_tag = None
        if current_major == 0 and args.against is None:
            minor_tag = _newest_minor_release_tag(root, current_version)
            if minor_tag is not None and minor_tag != tag:
                informational_tag = tag
            if minor_tag is not None:
                tag = minor_tag
    except (HistoryUnavailable, subprocess.CalledProcessError, OSError, ValueError) as error:
        print(f"comparison history unavailable: {_reason(error)}")
        return 2
    current = _load(root)
    try:
        stable_surface(current)
    except LookupError as error:
        print(f"stable API cannot be checked: {error}")
        return 1
    type_leaks = check_signature_types(_stable_objects(current))
    if type_leaks:
        print("stable API signature types are not stable or provisional:")
        for line in type_leaks:
            print(f"  {line}")
        return 1
    if tag is None:
        print("no release tag on the current release line: the stable API has no baseline yet")
        return 0
    informational_breakages = None
    failed_tag = tag
    with TemporaryDirectory(prefix="terrine-api-") as directory:
        try:
            release = _load_release(root, tag, Path(directory))
            stable_surface(release)
            breakages = stable_breakages(release, current)
            if informational_tag is not None:
                failed_tag = informational_tag
                with TemporaryDirectory(prefix="terrine-api-info-") as info_directory:
                    info_release = _load_release(root, informational_tag, Path(info_directory))
                    stable_surface(info_release)
                    informational_breakages = stable_breakages(info_release, current)
        except (
            subprocess.CalledProcessError,
            OSError,
            ImportError,
            tarfile.TarError,
            LookupError,
        ) as error:
            print(f"comparison history unavailable: cannot load {failed_tag}: {_reason(error)}")
            return 2
    baseline_major, baseline_minor = _release_minor(tag.removeprefix("v"))
    same_compatibility_line = (
        baseline_major == current_major
        and (current_major != 0 or baseline_minor == current_minor)
    )
    if informational_tag is not None:
        print(f"informational comparison against {informational_tag}:")
        if informational_breakages:
            for line in informational_breakages:
                print(f"  {line}")
        else:
            print(f"stable API signatures are compatible with {informational_tag}")
    if breakages:
        print(f"stable API breakages against {tag}:")
        for line in breakages:
            print(f"  {line}")
    else:
        print(f"stable API signatures are compatible with {tag}")
    if not same_compatibility_line:
        print("breaking changes are permitted across 0.x minor releases")
        return 0
    return 1 if breakages else 0


if __name__ == "__main__":
    raise SystemExit(main())
