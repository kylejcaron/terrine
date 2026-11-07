"""Drop columns that no consumer reads from the nested relations of a query.

Compilers build each value from staged intermediate projections that carry every
column forward; the SQL then lists each column at every nesting level. Pruning
keeps a column only where a later projection, predicate, grouping, ordering or
join reads it, so the statement shrinks without changing its rows or output.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import ibis
import ibis.expr.operations as ops
from ibis.common.collections import FrozenOrderedDict
from ibis.common.graph import Graph
from ibis.expr.operations.relations import Union as SetUnion

__all__ = ["prune_columns"]

# Ibis operations are dynamically declared annotables; they are handled untyped.
type Node = Any


def _not_relation(node: Node) -> bool:
    return not isinstance(node, ops.Relation)


def _reads(values: Iterable[Node]) -> tuple[list[Node], list[Node]]:
    """Fields the values read, and relations they read whole (subqueries).

    ``COUNT(*)`` reads its relation's rows but no column, so it needs neither.
    """
    fields: list[Node] = []
    whole: list[Node] = []
    for value in values:
        for node in Graph.from_bfs(value, filter=_not_relation).nodes():
            if isinstance(node, ops.Field):
                fields.append(node)
            elif not isinstance(node, (ops.Relation, ops.CountStar)):
                whole.extend(arg for arg in node.__args__ if isinstance(arg, ops.Relation))
    return fields, whole


def _kept(values: FrozenOrderedDict, needed: set[str]) -> list[str]:
    """Needed names in their order; a relation with no needed column keeps one."""
    names = [name for name in values if name in needed]
    return names or [next(iter(values))]


def _requirements(root: Node) -> tuple[dict[Node, set[str]], dict[Node, set[Node]]]:
    """Columns every consumer reads from each relation reachable from ``root``,
    and the relations that consume each one."""
    ordered: Any = Graph.from_bfs(root).toposort()
    graph = ordered[0]
    need: dict[Node, set[str]] = {root: set(root.schema.names)}
    consumers: dict[Node, set[Node]] = {}
    consumer: Node = root

    def require(relation: Node, names: Iterable[str]) -> None:
        need.setdefault(relation, set()).update(names)
        consumers.setdefault(relation, set()).add(consumer)

    def read(values: Iterable[Node]) -> None:
        fields, whole = _reads(values)
        for field in fields:
            require(field.rel, (field.name,))
        for relation in whole:
            require(relation, relation.schema.names)

    # Dependents precede their dependencies, so each relation's requirement is
    # complete before it is propagated to its parents.
    for node in reversed(list(graph)):
        if not isinstance(node, ops.Relation):
            continue
        consumer = node
        relation: Node = node
        needed = need.setdefault(relation, set(relation.schema.names))
        if isinstance(relation, ops.Project):
            require(relation.parent, ())
            read(relation.values[name] for name in _kept(relation.values, needed))
        elif isinstance(relation, ops.Aggregate):
            require(relation.parent, ())
            metrics = relation.metrics
            read((*relation.groups.values(), *(metrics[n] for n in metrics if n in needed)))
        elif isinstance(relation, ops.JoinChain):
            for table in relation.tables:
                require(table, ())
            read(relation.values[name] for name in _kept(relation.values, needed))
            read(predicate for link in relation.rest for predicate in link.predicates)
        elif isinstance(relation, (ops.Filter, ops.Sort)):
            require(relation.parent, needed)
            read(relation.predicates if isinstance(relation, ops.Filter) else relation.keys)
        elif isinstance(relation, (ops.Limit, ops.Sample, ops.Reference, ops.DropColumns)):
            require(relation.parent, needed)
        elif isinstance(relation, SetUnion) and not relation.distinct:
            require(relation.left, needed)
            require(relation.right, needed)
        else:
            # Any other relation reads every column of its inputs.
            for arg in relation.__children__:
                if isinstance(arg, ops.Relation):
                    require(arg, arg.schema.names)
                else:
                    read((arg,))
    return need, consumers


def prune_columns(table: ibis.Table) -> ibis.Table:
    """Return ``table`` with unread columns removed from every nested relation.

    The result has the same schema and rows; shared relations stay shared, so
    a relation read in several places still compiles once.
    """
    root: Node = table.op()
    need, consumers = _requirements(root)
    built: dict[Node, Node] = {}

    def relink(value: Node) -> Node:
        def replace(node: Node, kwargs: dict[str, Any] | None) -> Node:
            moved = {
                name: built[arg]
                for name, arg in zip(node.__argnames__, node.__args__, strict=True)
                if isinstance(arg, ops.Relation) and built.get(arg, arg) is not arg
            }
            if moved:
                kwargs = {
                    **dict(zip(node.__argnames__, node.__args__, strict=True)),
                    **(kwargs or {}),
                }
                kwargs.update(moved)
            return node.__recreate__(kwargs) if kwargs else node

        return value.replace(replace, filter=_not_relation)

    def exactly(relation: Node, names: set[str]) -> Node:
        """The rebuilt relation exposing exactly ``names``, in schema order."""
        rebuilt = build(relation)
        ordered = [name for name in relation.schema.names if name in names]
        if list(rebuilt.schema.names) == ordered:
            return rebuilt
        return rebuilt.to_expr().select(*ordered).op()

    def project(relation: Node, parent: Node, kept: dict[str, Node]) -> Node:
        if fields_of(relation.parent, kept.values()) and [
            (name, value.name) for name, value in kept.items()
        ] == [(name, name) for name in parent.schema.names]:
            # A projection of every column, unchanged, is its input.
            return parent
        sole = consumers[relation.parent] == {relation}
        if sole and isinstance(parent, ops.JoinChain) and fields_of(relation.parent, kept.values()):
            # Renaming or reordering a join's own output selects it in the join.
            values = {name: parent.values[value.name] for name, value in kept.items()}
            return parent.copy(values=FrozenOrderedDict(values))
        if sole and isinstance(parent, ops.Project) and fields_of(None, parent.values.values()):
            # Reading through a projection that only selects columns reads its input.
            inner = {ops.Field(parent, name): value for name, value in parent.values.items()}
            values = {
                name: relink(value).replace(inner, filter=_not_relation)
                for name, value in kept.items()
            }
            return relation.copy(parent=parent.parent, values=FrozenOrderedDict(values))
        values = {name: relink(value) for name, value in kept.items()}
        return relation.copy(parent=parent, values=FrozenOrderedDict(values))

    def fields_of(relation: Node, values: Iterable[Node]) -> bool:
        """Whether every value is a plain column of ``relation`` (any, if None)."""
        return all(
            isinstance(value, ops.Field) and (relation is None or value.rel == relation)
            for value in values
        )

    def build(relation: Node) -> Node:
        if relation in built:
            return built[relation]
        needed = need[relation]
        if isinstance(relation, ops.Project):
            kept = {name: relation.values[name] for name in _kept(relation.values, needed)}
            result = project(relation, build(relation.parent), kept)
        elif isinstance(relation, ops.Aggregate):
            parent = build(relation.parent)
            metrics = [name for name in relation.metrics if name in needed]
            if not metrics and not relation.groups:
                metrics = [next(iter(relation.metrics))]
            result = relation.copy(
                parent=parent,
                groups=FrozenOrderedDict({n: relink(v) for n, v in relation.groups.items()}),
                metrics=FrozenOrderedDict({n: relink(relation.metrics[n]) for n in metrics}),
            )
        elif isinstance(relation, ops.JoinChain):
            first = build(relation.first)
            rest = tuple(
                link.copy(
                    table=build(link.table),
                    predicates=tuple(relink(predicate) for predicate in link.predicates),
                )
                for link in relation.rest
            )
            values = {n: relink(relation.values[n]) for n in _kept(relation.values, needed)}
            result = relation.copy(first=first, rest=rest, values=FrozenOrderedDict(values))
        elif isinstance(relation, SetUnion) and not relation.distinct:
            result = relation.copy(
                left=exactly(relation.left, needed), right=exactly(relation.right, needed)
            )
        elif isinstance(relation, ops.DropColumns):
            parent = build(relation.parent)
            dropped = frozenset(name for name in relation.columns_to_drop if name in parent.schema)
            result = relation.copy(parent=parent, columns_to_drop=dropped)
        else:
            changes: dict[str, Any] = {}
            for name, arg in zip(relation.__argnames__, relation.__args__, strict=True):
                if isinstance(arg, ops.Relation):
                    changes[name] = build(arg)
                elif isinstance(arg, ops.Node):
                    changes[name] = relink(arg)
                elif isinstance(arg, tuple) and any(isinstance(a, ops.Node) for a in arg):
                    changes[name] = tuple(relink(a) if isinstance(a, ops.Node) else a for a in arg)
            result = relation.copy(**changes) if changes else relation
        built[relation] = result
        return result

    return build(root).to_expr()
