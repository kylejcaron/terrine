"""Build the refusal reference without importing optional integrations."""

from __future__ import annotations

import argparse
import ast
import html
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from string import Formatter
from tempfile import TemporaryDirectory
from urllib.parse import unquote, urlsplit


@dataclass(frozen=True)
class RefusalDeclaration:
    code: str
    template: str
    fields: tuple[str, ...]
    source: str


def refusal_declarations(package: Path) -> tuple[RefusalDeclaration, ...]:
    declarations = []
    for path in sorted(package.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in sorted(ast.walk(tree), key=lambda item: getattr(item, "lineno", 0)):
            if not isinstance(node, ast.Call):
                continue
            name = node.func.id if isinstance(node.func, ast.Name) else None
            if name != "RefusalSpec":
                continue
            values = dict(zip(("code", "template"), node.args, strict=False))
            values.update({item.arg: item.value for item in node.keywords if item.arg})
            try:
                code = ast.literal_eval(values["code"])
                template = ast.literal_eval(values["template"])
            except (KeyError, ValueError, TypeError) as error:
                raise ValueError(
                    f"Refusal declarations must be literal: {path}:{node.lineno}"
                ) from error
            if not isinstance(code, str) or not isinstance(template, str):
                raise ValueError(
                    f"Refusal declarations must be literal strings: {path}:{node.lineno}"
                )
            fields = tuple(
                sorted({field for _, field, _, _ in Formatter().parse(template) if field})
            )
            declarations.append(
                RefusalDeclaration(code, template, fields, str(path.relative_to(package.parent)))
            )
    return tuple(declarations)


_RECOVERY = {
    "agent": ("dataset-agent-tools.md", "Use owner-approved resources, grants and fresh receipts."),
    "backend": ("backend-compatibility.md", "Use verified local DuckDB operation capabilities."),
    "budget": (
        "cost-and-residency.md",
        "Reduce output or obtain a separately reviewed owner budget.",
    ),
    "catalog": ("temporal-engine.md", "Correct the named Catalog field or source declaration."),
    "cohort": ("trajectories-survival.md", "Check grouping and observable cohort denominators."),
    "compile": ("design-validation.md", "Correct the named Design or timestamp constraint."),
    "convert": ("common-stack-interop.md", "Install the conversion extra; retain Arrow metadata."),
    "dataset": ("ml-staging.md", "Use pinned manifests, valid recipes and owned destinations."),
    "design": (
        "design-validation.md",
        "Repair temporal/history contracts; do not zero-fill unknowns.",
    ),
    "diff_diff": ("library-handoffs.md", "Use a supported persistent-adoption panel."),
    "execution": ("backend-compatibility.md", "Use owned native sessions or retained inputs."),
    "export": ("library-handoffs.md", "Satisfy shared preflight and the library shape contract."),
    "filter": ("temporal-engine.md", "Use typed, known filters and complete dimension histories."),
    "increment": ("increment-compatibility.md", "Use the supported extra or a native Catalog."),
    "lifelines": ("library-handoffs.md", "Use per-unit records to retain delayed-entry history."),
    "metadata": (
        "temporal-engine.md",
        "Use consistent coverage, roles and representable metadata.",
    ),
    "metric": ("temporal-engine.md", "Use a supported aggregation, measure, precision and shape."),
    "model": (
        "temporal-engine.md",
        "Correct source, roster and coverage bindings; rebind the Model.",
    ),
    "readiness": ("temporal-engine.md", "Declare authoritative completeness for selected sources."),
    "runtime": (
        "materialization-runtime.md",
        "Inspect authoritative run state. Replay unchanged requests; replan changed inputs.",
    ),
    "semantic": ("temporal-engine.md", "Correct native semantics or use an admitted staged table."),
    "sequence": ("temporal-engine.md", "Declare stable event IDs and bounded sequence sizes."),
    "simulate": ("synthetic-worlds.md", "Use representable, consistent raw-source configurations."),
    "spec": (
        "design-specs.md",
        "Use typed constructors, supported fields and unique output names.",
    ),
    "sql": ("temporal-engine.md", "Use one admitted read-only query or an owner-staged table."),
    "state_panel": (
        "state-panels.md",
        "Use ordered, bounded observations and explicit missingness.",
    ),
    "study": ("design-validation.md", "Repair the named Design contract before collecting rows."),
    "survival": (
        "trajectories-survival.md",
        "Declare per-unit event/censor and observation bounds.",
    ),
    "temporal": ("temporal-engine.md", "Check exact endpoints, coverage and arithmetic validity."),
    "trajectory": ("trajectories-survival.md", "Use bounded periods and consistent observation."),
    "transform": ("ml-staging.md", "Fit on training rows; reuse unchanged frozen scoring limits."),
    "window": ("design-specs.md", "Use ordered integral elapsed endpoints with s/m/h/d/w units."),
}


def recovery_guide(code: str) -> tuple[str, str]:
    namespace = code.split(".")[1]
    if namespace not in _RECOVERY:
        raise ValueError(f"Refusal namespace needs recovery guidance: {namespace}")
    return _RECOVERY[namespace]


def render_refusals(declarations: tuple[RefusalDeclaration, ...]) -> str:
    lines = [
        "# Refusal codes",
        "",
        "This reference is generated from every module-owned `RefusalSpec` declaration,",
        "including inline declarations. Codes are stable; message wording is not an API.",
        "Interpolation fields below are required by the message template, **not** an",
        "exhaustive context schema. Inspect the immutable `TerrineError.context` for",
        "additional request-specific fields, exact reasons and available routes.",
        "Multiple message variants for the same code are retained. Do not retry a",
        "refusal by silently widening a budget, changing an input or dropping rows.",
        "",
        "Regenerate with `uv run python -m tests.documentation`; completeness is checked",
        "by `uv run pytest -q tests/test_documentation.py -k refusal`.",
        "",
        "## Handling a refusal",
        "",
        "Catch `terrine.TerrineError`, dispatch on `.code`, and report `.context` only",
        "inside the owner's trust boundary: backend errors can describe source names.",
        "A missing extra names its installation route. Unknown coverage requires an",
        "authoritative declaration, not the latest event timestamp. Unknown strict scan",
        "costs require independently verified bounds or an explicit owner policy choice.",
        "See the recovery category linked with each code before changing the request.",
        "",
    ]
    for namespace in sorted({item.code.split(".")[1] for item in declarations}):
        guide, guidance = recovery_guide(f"terrine.{namespace}.example")
        lines.extend(
            [f"## {namespace}", "", guidance, "", f"[Recovery guide](../guides/{guide})", ""]
        )
        lines.extend(
            [
                "| Code | Message template | Interpolation fields | Declaration |",
                "| --- | --- | --- | --- |",
            ]
        )
        for item in sorted(
            set(declarations), key=lambda item: (item.code, item.template, item.source)
        ):
            if item.code.split(".")[1] != namespace:
                continue
            template = html.escape(item.template).replace("|", "&#124;").replace("\n", "<br>")
            fields = ", ".join(f"`{field}`" for field in item.fields) or "None"
            lines.append(
                f"| `{item.code}` | <code>{template}</code> | {fields} | "
                f"[{item.source}](../../{item.source}) |"
            )
        lines.append("")
    return "\n".join(lines)


def broken_links(root: Path) -> list[str]:
    sources = [
        root / "README.md",
        *sorted((root / "docs/guides").glob("*.md")),
        *sorted((root / "docs/reference").glob("*.md")),
    ]
    broken = []
    for source in sources:
        text = re.sub(r"```.*?```", "", source.read_text(), flags=re.DOTALL)
        links = re.findall(r"\[[^\]]*\]\(([^)]+)\)", text)
        links += re.findall(r"^\[[^\]]+\]:\s*(\S+)", text, flags=re.MULTILINE)
        for link in links:
            target = urlsplit(link.strip("<>"))
            if target.scheme or target.netloc or not target.path:
                continue
            if not (source.parent / unquote(target.path)).exists():
                broken.append(f"{source.relative_to(root)}: {link}")
    return broken


def build_docs(root: Path) -> None:
    from mkdocs.commands.build import build
    from mkdocs.config import load_config

    with TemporaryDirectory(prefix="terrine-docs-") as directory:
        source = Path(directory)
        for name in ("docs", "terrine", "tests", "examples", "integration", "benchmarks"):
            shutil.copytree(
                root / name,
                source / name,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "superpowers"),
            )
        for name in ("README.md", "CHANGELOG.md", "THIRD_PARTY_NOTICES.txt"):
            shutil.copyfile(root / name, source / name)
        config = load_config(
            config_file=str(root / "mkdocs.yml"),
            docs_dir=str(source),
            strict=True,
        )
        build(config)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true", help="Build the strict local documentation")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if args.build:
        build_docs(root)
        return
    destination = root / "docs/reference/refusals.md"
    destination.parent.mkdir(parents=True, exist_ok=True)
    declarations = refusal_declarations(root / "terrine")
    destination.write_text(render_refusals(declarations), encoding="utf-8")
    print(
        f"Documented {len({item.code for item in declarations})} codes "
        f"from {len(declarations)} declarations"
    )


if __name__ == "__main__":
    main()
