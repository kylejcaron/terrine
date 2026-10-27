from __future__ import annotations

import ast
from collections.abc import Iterable, Iterator
from pathlib import Path

import pytest

from terrine.backends.registry import ENGINES

_ROOT = Path(__file__).resolve().parents[1]
_ENGINE_FILES = frozenset(
    {
        "terrine/backends/registry.py",
        "terrine/runtime/backends/duckdb.py",
        *(f"terrine/backends/engines/{name}.py" for name in ENGINES),
        *(f"{engine.provider_module.replace('.', '/')}.py" for engine in ENGINES.values()),
    }
)
_ENGINE_LITERALS = frozenset(ENGINES) | frozenset(engine.ibis_module for engine in ENGINES.values())


def _identity(value: ast.AST, identities: set[str]) -> bool:
    if isinstance(value, ast.Name):
        return value.id in identities or bool(
            {"engine", "backend", "dialect"} & set(value.id.casefold().split("_"))
        )
    if isinstance(value, ast.Attribute):
        if value.attr == "__module__":
            return True
        if value.attr in {"name", "dialect", "backend", "engine"}:
            return _identity(value.value, identities) or (
                isinstance(value.value, ast.Name)
                and value.value.id in {"con", "connection", "catalog"}
            )
    if isinstance(value, ast.Call):
        if isinstance(value.func, ast.Attribute):
            if value.func.attr in {"casefold", "lower"}:
                return _identity(value.func.value, identities)
            if value.func.attr == "_find_backend" or (
                value.func.attr == "get_backend"
                and isinstance(value.func.value, ast.Name)
                and value.func.value.id == "ibis"
            ):
                return True
        if isinstance(value.func, ast.Name):
            if value.func.id == "str" and value.args:
                return _identity(value.args[0], identities)
            if value.func.id == "getattr" and len(value.args) >= 2:
                field = value.args[1]
                return (
                    isinstance(field, ast.Constant)
                    and isinstance(field.value, str)
                    and field.value in {"name", "dialect", "backend", "engine", "__module__"}
                    and _identity(ast.Attribute(value=value.args[0], attr=field.value), identities)
                )
    return False


def _literals(value: ast.AST, aliases: dict[str, frozenset[str]]) -> frozenset[str]:
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        literal = value.value.casefold()
        return frozenset({literal}) if literal in _ENGINE_LITERALS else frozenset()
    if isinstance(value, ast.Name):
        return aliases.get(value.id, frozenset())
    if isinstance(value, (ast.Set, ast.Tuple, ast.List)):
        return frozenset().union(*(_literals(item, aliases) for item in value.elts))
    if isinstance(value, ast.Call) and isinstance(value.func, ast.Name):
        if value.func.id in {"set", "frozenset", "tuple", "list"} and value.args:
            return _literals(value.args[0], aliases)
    return frozenset()


_FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
_COMPREHENSIONS = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)
_SCOPES = (*_FUNCTIONS, ast.ClassDef, *_COMPREHENSIONS)


def _scope_nodes(roots: Iterable[ast.AST]) -> Iterator[ast.AST]:
    for node in roots:
        yield node
        if not isinstance(node, _SCOPES):
            yield from _scope_nodes(ast.iter_child_nodes(node))


def _argument_names(arguments: ast.arguments) -> set[str]:
    return {
        argument.arg
        for argument in (
            *arguments.posonlyargs,
            *arguments.args,
            *arguments.kwonlyargs,
            *([arguments.vararg] if arguments.vararg else []),
            *([arguments.kwarg] if arguments.kwarg else []),
        )
    }


def engine_selection_violations(source: str, *, path: str) -> tuple[int, ...]:
    if Path(path).as_posix() in _ENGINE_FILES:
        return ()
    tree = ast.parse(source, filename=path)
    lines: set[int] = set()

    def scan(
        roots: Iterable[ast.AST],
        inherited_aliases: dict[str, frozenset[str]],
        inherited_identities: set[str],
        parameters: Iterable[str] = (),
        *,
        class_scope: bool = False,
    ) -> None:
        nodes = tuple(_scope_nodes(roots))
        bound = set(parameters)
        for node in nodes:
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                bound.add(node.id)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                bound.add(node.name)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                bound.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
        aliases = {name: values for name, values in inherited_aliases.items() if name not in bound}
        identities = inherited_identities - bound
        assignments = [node for node in nodes if isinstance(node, (ast.Assign, ast.AnnAssign))]
        # Resolve aliases only within this scope; local bindings hide enclosing names.
        for _ in range(len(assignments) + 1):
            changed = False
            for node in assignments:
                value = node.value
                if value is None:
                    continue
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if not isinstance(target, ast.Name):
                        continue
                    literals = _literals(value, aliases)
                    if literals and aliases.get(target.id) != literals:
                        aliases[target.id] = literals
                        changed = True
                    if _identity(value, identities) and target.id not in identities:
                        identities.add(target.id)
                        changed = True
            if not changed:
                break

        def selects(value: ast.AST, candidate: ast.AST) -> bool:
            literals = _literals(candidate, aliases)
            return bool(literals) and (
                _identity(value, identities)
                or any(literal.startswith("ibis.backends.") for literal in literals)
            )

        for node in nodes:
            if isinstance(node, ast.Compare):
                for left, operator, right in zip(
                    [node.left, *node.comparators], node.ops, node.comparators, strict=False
                ):
                    if isinstance(operator, (ast.Eq, ast.NotEq, ast.In, ast.NotIn)) and (
                        selects(left, right) or selects(right, left)
                    ):
                        lines.add(node.lineno)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == "isin" and any(
                    selects(node.func.value, argument) for argument in node.args
                ):
                    lines.add(node.lineno)
            elif isinstance(node, ast.Match):
                for case in node.cases:
                    if any(
                        selects(node.subject, pattern.value)
                        for pattern in ast.walk(case.pattern)
                        if isinstance(pattern, ast.MatchValue)
                    ):
                        lines.add(case.pattern.lineno)
            elif isinstance(node, _FUNCTIONS):
                scan(
                    [*node.args.defaults, *(value for value in node.args.kw_defaults if value)],
                    aliases,
                    identities,
                )
                if not isinstance(node, ast.Lambda):
                    scan(node.decorator_list, aliases, identities)
                scan(
                    [node.body] if isinstance(node, ast.Lambda) else node.body,
                    inherited_aliases if class_scope else aliases,
                    inherited_identities if class_scope else identities,
                    _argument_names(node.args),
                )
            elif isinstance(node, ast.ClassDef):
                scan(
                    [*node.bases, *node.decorator_list, *(value.value for value in node.keywords)],
                    aliases,
                    identities,
                )
                scan(
                    node.body,
                    inherited_aliases if class_scope else aliases,
                    inherited_identities if class_scope else identities,
                    class_scope=True,
                )
            elif isinstance(node, _COMPREHENSIONS):
                scan([node.generators[0].iter], aliases, identities)
                scan(
                    [
                        *([node.key, node.value] if isinstance(node, ast.DictComp) else [node.elt]),
                        *(generator.iter for generator in node.generators[1:]),
                        *(
                            condition
                            for generator in node.generators
                            for condition in generator.ifs
                        ),
                    ],
                    aliases,
                    identities,
                    {
                        target.id
                        for generator in node.generators
                        for target in ast.walk(generator.target)
                        if isinstance(target, ast.Name)
                    },
                )

    scan(tree.body, {}, set())
    return tuple(sorted(lines))


@pytest.mark.parametrize(
    ("source", "lines"),
    [
        ('if dialect == "duckdb": pass', (1,)),
        ('if "postgres" != backend.casefold(): pass', (1,)),
        ('NAMES = {"bigquery", "snowflake"}\nif dialect in NAMES: pass', (2,)),
        ('NAMES = frozenset(("bigquery", "snowflake"))\nif backend not in NAMES: pass', (2,)),
        ('if "DUCKDB" == dialect.casefold(): pass', (1,)),
        ('if type(con).__module__ == "ibis.backends.postgres": pass', (1,)),
        ('module = type(con).__module__\nif module != "ibis.backends.snowflake": pass', (2,)),
        ('if backend.isin(["duckdb", "postgres"]): pass', (1,)),
        ('match dialect:\n    case "duckdb" | "postgres": pass\n    case _: pass', (2,)),
        ('selected = con.name\nif selected == "bigquery": pass', (2,)),
        ('if table._find_backend(use_default=False).name == "duckdb": pass', (1,)),
        ('if "duckdb" == table._find_backend(use_default=False).name: pass', (1,)),
        ('if ibis.get_backend(table).name == "duckdb": pass', (1,)),
        ('if "duckdb" != ibis.get_backend(table).name: pass', (1,)),
        ('selected = con.name\nif selected.casefold() == "duckdb": pass', (2,)),
        ('selected = con.name\nif selected.casefold() in {"duckdb", "postgres"}: pass', (2,)),
        ('selected = con.name\nif selected.casefold().isin(["duckdb"]): pass', (2,)),
        ('selected = con.name\nmatch selected.casefold():\n    case "duckdb": pass', (3,)),
        (
            "def inspect(con):\n"
            "    selected = con.name\n"
            "    def uses_backend():\n"
            '        return selected.casefold() == "duckdb"\n'
            "    return uses_backend",
            (4,),
        ),
    ],
)
def test_guard_rejects_engine_selection(source, lines):
    assert engine_selection_violations(source, path="terrine/temporal/example.py") == lines
    for path in _ENGINE_FILES:
        assert not engine_selection_violations(source, path=path)


@pytest.mark.parametrize(
    "source",
    [
        'query.sql(dialect="duckdb")',
        'sge.DataType.build("NUMERIC", dialect="postgres")',
        'if type(raw).__module__.split(".")[0] == "duckdb": pass',
        'if type(raw).__module__.split(".")[:2] != ["snowflake", "connector"]: pass',
        'if type(client).__module__.split(".")[:3] != ["google", "cloud", "bigquery"]: pass',
        'if field.name == "duckdb" or field.dialect in {"postgres", "bigquery"}: pass',
        'if name == "snowflake": pass',
        'import ibis.backends.duckdb\n"""dialect == \'duckdb\'"""',
        (
            "def inspect(con):\n"
            "    selected = con.name\n"
            "    return selected\n"
            "def is_label(selected):\n"
            '    return selected == "duckdb"'
        ),
        (
            "selected = con.name\n"
            "def is_label(selected):\n"
            '    return selected.casefold() == "duckdb"'
        ),
        (
            "def engine_names():\n"
            '    NAMES = {"duckdb"}\n'
            "    return NAMES\n"
            "def is_label(dialect, NAMES):\n"
            "    return dialect in NAMES"
        ),
        ('NAMES = {"duckdb"}\ndef is_label(dialect, NAMES):\n    return dialect in NAMES'),
    ],
)
def test_guard_ignores_nonselection_literals(source):
    assert not engine_selection_violations(source, path="terrine/compile/example.py")


def test_guard_exemptions_are_exact_files():
    for path in (
        "terrine/backends/numeric.py",
        "terrine/backends/engines/__init__.py",
        "terrine/execution/backend.py",
        "terrine/runtime/backends/base.py",
        "terrine/compile/example.py",
        "terrine/temporal/example.py",
    ):
        assert engine_selection_violations('if dialect == "duckdb": pass', path=path) == (1,)


def test_no_engine_selection_outside_engine_modules():
    violations = [
        f"{path.relative_to(_ROOT)}:{line}"
        for path in sorted((_ROOT / "terrine").rglob("*.py"))
        for line in engine_selection_violations(
            path.read_text(encoding="utf-8"), path=path.relative_to(_ROOT).as_posix()
        )
    ]
    assert not violations, "Engine selection outside engine modules:\n" + "\n".join(violations)


def test_package_scan_detects_reintroduced_selection(monkeypatch):
    target = _ROOT / "terrine/temporal/aggregate.py"
    original = Path.read_text

    def read_text(path, *args, **kwargs):
        source = original(path, *args, **kwargs)
        return source + '\nif dialect == "duckdb": pass\n' if path == target else source

    monkeypatch.setattr(Path, "read_text", read_text)
    with pytest.raises(AssertionError, match=r"terrine/temporal/aggregate\.py:\d+"):
        test_no_engine_selection_outside_engine_modules()
