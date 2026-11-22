"""Compare exact cross-study reuse on synthetic, revisioned local DuckDB inputs."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import platform
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any
from unittest.mock import patch

import duckdb
import ibis
import pyarrow as pa

import terrine as tr
from benchmarks.workloads import (
    REUSE_ANCHOR,
    REUSE_COVERAGE,
    ReuseStudy,
    ReuseWorkload,
    reuse_workload,
)
from terrine.data import _ARROW_METADATA_KEY, MaterializedFrame
from terrine.execution.inputs import ArrowReader, ExecutionContext

_SCENARIOS = ("unchanged", "correction-heavy", "sparse")
_STRATEGIES = ("independent", "batched", "daily")
_WORKSPACE_LOCK = "terrine-arch-nj0y-profile.lock"
_THREADS = 2
_TIME = pa.timestamp("us", tz="UTC")
_SIDES = ("observed_from", "observed_until")


@dataclass(frozen=True)
class FileReceipt:
    path: Path
    digest: str
    size: int
    rows: int


@dataclass
class Invocation:
    revision: int
    approved: bool
    route: str = "raw"
    raw_studies: list[str] = field(default_factory=list)
    file_bytes_checked: int = 0
    output_rows: int = 0
    public_arrow_bytes: int = 0
    delivered_arrow_bytes: int = 0
    phases: dict[str, float] = field(default_factory=dict)
    study_gate_statements: int = 0
    study_gate_seconds: float = 0.0
    study_output_statements: int = 0
    study_output_and_transfer_seconds: float = 0.0
    total_seconds: float = 0.0

    @contextmanager
    def time(self, phase: str) -> Generator[None]:
        started = perf_counter()
        try:
            yield
        finally:
            self.phases[phase] = self.phases.get(phase, 0.0) + perf_counter() - started


@dataclass(frozen=True)
class StudyOutput:
    table: pa.Table
    metadata: dict[str, Any]
    design_hash: str
    model_hash: str


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _limited_views(
    db: duckdb.DuckDBPyConnection, studies: tuple[ReuseStudy, ...], *, individual: bool
) -> None:
    bounds = [(s.name, s.start, s.end) for s in studies] if individual else []
    aligned = [s for s in studies if s.day_aligned]
    bounds.append(("batched", min(s.start for s in aligned), max(s.end for s in aligned)))
    for name, start, end in bounds:
        # Separate predicates keep this stored view in the admitted read-only SQL subset.
        lower, upper = (_quote(value.replace(tzinfo=None).isoformat(" ")) for value in (start, end))
        db.execute(
            f"CREATE VIEW study_{name} AS SELECT * FROM ("
            f"SELECT * FROM facts WHERE instant >= TIMESTAMP {lower}"
            f") AS lower_bounded WHERE instant < TIMESTAMP {upper}"
        )


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _receipt(path: Path, rows: int) -> FileReceipt:
    return FileReceipt(path, _digest(path), path.stat().st_size, rows)


def _check_file(receipt: FileReceipt, result: Invocation) -> bool:
    with result.time("revalidation"):
        if not receipt.path.is_file() or receipt.path.stat().st_size != receipt.size:
            return False
        result.file_bytes_checked += receipt.size
        return _digest(receipt.path) == receipt.digest


def _count_facts(db: duckdb.DuckDBPyConnection) -> int:
    row = db.execute("SELECT count(*) FROM facts").fetchone()
    if row is None:
        raise AssertionError("Fact count query returned no row")
    return int(row[0])


def _publish(path: Path, workload: ReuseWorkload, revision: int) -> tuple[FileReceipt, float]:
    started = perf_counter()
    with duckdb.connect(str(path), config={"threads": _THREADS}) as db:
        db.execute(
            """CREATE TABLE roster AS SELECT i AS unit_id,
                 CASE WHEN i = 2 THEN TIMESTAMP '2025-01-18' END AS observed_from,
                 CASE WHEN i = 2 THEN TIMESTAMP '2025-01-20' END AS observed_until
               FROM range(?) AS t(i)""",
            [workload.units],
        )
        active_units = workload.units - 1
        density = workload.events_per_unit_day
        db.execute(
            """CREATE TABLE facts AS
               WITH positions AS (
                 SELECT i, i // ? AS bucket, i % ? AS within_day,
                        CASE WHEN i % 5 = 0 THEN ? ELSE 0 END AS move
                 FROM range(?) AS t(i)
                 WHERE ? = 0 OR (i + ?) % 11 <> 0
               ), coordinates AS (
                 SELECT *, 1 + (bucket + move) % ? AS unit_id,
                        (bucket // ? + (bucket % ?) * 7 + move) % 24 AS day
                 FROM positions
               )
               SELECT i AS event_id, unit_id,
                      TIMESTAMP '2025-01-01' + day * INTERVAL 1 DAY +
                        ((within_day * 7331 + (bucket % 3) * 27000) % 86400) *
                          INTERVAL 1 SECOND AS instant,
                      CASE WHEN unit_id = 1 OR i % 19 = 0 THEN NULL
                           WHEN unit_id = 3 AND day = 19 AND within_day = 0 THEN 9007199254740993
                           ELSE (i * 127) % 10007 - 5000 + move END::BIGINT AS cents
               FROM coordinates ORDER BY instant, unit_id, event_id""",
            [
                density,
                density,
                revision,
                active_units * workload.days_per_unit * density,
                revision,
                revision,
                active_units,
                active_units,
                active_units,
            ],
        )
        _limited_views(db, workload.studies, individual=True)
        rows = _count_facts(db)
    receipt = _receipt(path, rows)
    return receipt, perf_counter() - started


def _build_daily(
    path: Path, raw: FileReceipt, studies: tuple[ReuseStudy, ...], invocation: Invocation
) -> FileReceipt:
    with invocation.time("build"):
        db = duckdb.connect(str(path), config={"threads": _THREADS})
    try:
        with invocation.time("build"):
            db.execute(f"ATTACH {_quote(str(raw.path))} AS source (READ_ONLY)")
            # Positive row count + nullable exact sum distinguish empty from all-null days.
            db.execute(
                """CREATE TEMP TABLE daily_buffer AS
                   SELECT unit_id, date_trunc('day', instant) AS instant,
                          count(*) AS n, sum(cents::DECIMAL(38, 0)) AS cents
                   FROM source.main.facts GROUP BY 1, 2"""
            )
        with invocation.time("write"):
            db.execute("CREATE TABLE facts AS FROM daily_buffer ORDER BY instant, unit_id")
            db.execute("CREATE TABLE roster AS FROM source.main.roster")
            _limited_views(db, studies, individual=False)
            db.execute("CHECKPOINT")
        with invocation.time("verification"):
            rows = _count_facts(db)
            for built, stored in (("daily_buffer", "facts"), ("source.main.roster", "roster")):
                for left, right in ((built, stored), (stored, built)):
                    if (
                        db.execute(f"(FROM {left} EXCEPT ALL FROM {right}) LIMIT 1").fetchone()
                        is not None
                    ):
                        raise AssertionError(f"Persisted {stored} differs from its input")
    finally:
        with invocation.time("write"):
            db.close()
    with invocation.time("verification"):
        receipt = _receipt(path, rows)
        invocation.file_bytes_checked += receipt.size
        return receipt


@contextmanager
def _study_queries(result: Invocation) -> Generator[None]:
    """Non-overlapping timings: prior checks and the final gated Arrow stream."""
    execute, batches = ExecutionContext.execute, ExecutionContext.to_arrow_batches

    def gate(context: ExecutionContext, query: Any) -> pa.Table:
        started = perf_counter()
        result.study_gate_statements += 1
        try:
            return execute(context, query)
        finally:
            result.study_gate_seconds += perf_counter() - started

    @contextmanager
    def output(
        context: ExecutionContext, query: Any, *, chunk_size: int = 65_536
    ) -> Generator[ArrowReader]:
        started = perf_counter()
        prior_gate_seconds = result.study_gate_seconds
        result.study_output_statements += 1
        try:
            with batches(context, query, chunk_size=chunk_size) as reader:
                yield reader
        finally:
            # An empty output may run the separately timed fallback gate.
            elapsed = perf_counter() - started
            result.study_output_and_transfer_seconds += elapsed - (
                result.study_gate_seconds - prior_gate_seconds
            )

    with (
        patch.object(ExecutionContext, "execute", gate),
        patch.object(ExecutionContext, "to_arrow_batches", output),
    ):
        yield


@contextmanager
def _reader(path: Path) -> Generator[ibis.BaseBackend]:
    con = ibis.duckdb.connect(str(path), read_only=True, threads=_THREADS)
    try:
        yield con
    finally:
        con.disconnect()


def _read(
    con: ibis.BaseBackend, relation: str, design: tr.Design, units: int, *, daily: bool
) -> MaterializedFrame:
    start, end = REUSE_COVERAGE
    model = tr.Model.from_tables(
        con,
        units={
            "user": tr.UnitSource(
                "roster", "unit_id", observed_from="observed_from", observed_until="observed_until"
            )
        },
        events={
            "orders": tr.Events(
                relation,
                time="instant",
                key="unit_id",
                complete=(start.date(), end.date() - timedelta(days=1)),
                metrics={
                    "orders": tr.sum("n", exact=True) if daily else tr.count(),
                    "spend": tr.sum("cents", exact=True),
                },
            )
        },
    )
    return design.build(model).materialize(budget=tr.Budget(max_rows=units))


def _project(
    frame: MaterializedFrame, studies: tuple[ReuseStudy, ...], result: Invocation, *, daily: bool
) -> dict[str, StudyOutput]:
    native = frame.to_pyarrow()
    result.public_arrow_bytes += native.nbytes
    # Only the benchmark projects a fused Frame or casts summed counts back to int64.
    # Retain the real plan identities separately; never stamp a candidate as the raw Model.
    schema_metadata = dict(native.schema.metadata or {})
    base = json.loads(schema_metadata.pop(_ARROW_METADATA_KEY))
    provenance = base["provenance"]
    design_hash, model_hash = provenance.pop("design_hash"), provenance.pop("model_hash")
    outputs = {}
    all_metrics = {
        column
        for name, meta in frame.columns.items()
        for column in (
            name,
            meta.complete_column,
            meta.reason_column,
            meta.observed_from_column,
            meta.observed_until_column,
        )
    }
    for study in studies:
        design = study.design
        names = (*design.features, *design.outcomes)
        renames = {}
        columns = {}
        for index, name in enumerate(names):
            meta = base["columns"][name].copy()
            for side in _SIDES:
                ref = f"{side}_column"
                canonical = f"__terrine_value_{index}_{side}"
                renames[meta[ref]] = canonical
                meta[ref] = canonical
            columns[name] = meta
        own_metrics = {
            column
            for name, meta in columns.items()
            for column in (name, meta["complete_column"], meta["reason_column"])
        } | renames.keys()
        selected = [c for c in native.column_names if c in own_metrics or c not in all_metrics]
        table = (
            native.select(selected)
            .rename_columns([renames.get(c, c) for c in selected])
            .replace_schema_metadata(schema_metadata or None)
        )
        if daily:
            count = f"{study.name}_orders"
            index = table.schema.get_field_index(count)
            table = table.set_column(index, count, table[count].cast(pa.int64(), safe=True))
        roles = base["roles"] | {
            role: [name for name in base["roles"][role] if name in wanted]
            for role, wanted in (("features", design.features), ("outcomes", design.outcomes))
        }
        projected_provenance = provenance | {
            key: {name: provenance[key][name] for name in names if name in provenance[key]}
            for key in ("feature_cutoffs", "expression_timings")
        }
        metadata = base | {
            "roles": roles,
            "columns": columns,
            "provenance": projected_provenance,
            "bookkeeping_columns": [
                renames.get(name, name) for name in base["bookkeeping_columns"] if name in selected
            ],
        }
        outputs[study.name] = StudyOutput(table, metadata, design_hash, model_hash)
        result.output_rows += table.num_rows
        result.delivered_arrow_bytes += table.nbytes
    return outputs


def _batch_design(studies: tuple[ReuseStudy, ...]) -> tr.Design:
    return tr.Design(
        "user",
        tr.FixedDate(REUSE_ANCHOR),
        features={name: expr for s in studies for name, expr in s.design.features.items()},
        outcomes={name: expr for s in studies for name, expr in s.design.outcomes.items()},
    )


def _workload_step(
    strategy: str,
    revision: int,
    approved: bool,
    raw: FileReceipt,
    store: dict[str, FileReceipt],
    directory: Path,
    workload: ReuseWorkload,
) -> tuple[Invocation, dict[str, StudyOutput]]:
    started = perf_counter()
    result = Invocation(revision, approved)
    # Only these fixed, unfiltered integer/count declarations are in scope.
    aligned = tuple(s for s in workload.studies if s.day_aligned)
    raw_reads = (
        workload.studies
        if strategy == "independent"
        else tuple(s for s in workload.studies if not s.day_aligned)
    )
    daily = None
    if strategy == "daily":
        if not approved:
            result.route = "raw_unknown_revision"
        elif not _check_file(raw, result):
            result.route = "raw_revision_mismatch"
        else:
            candidate = store.get(raw.digest)
            if candidate is None:
                result.route = "build" if not store else "maintenance_full_rebuild"
                candidate = _build_daily(
                    directory / f"daily_{revision}.duckdb", raw, aligned, result
                )
                store[raw.digest] = candidate
                daily = candidate
            elif _check_file(candidate, result):
                result.route = "reuse"
                daily = candidate
            else:
                result.route = "raw_summary_mismatch"
    elif strategy == "batched":
        result.route = "batch"
    raw_batch = strategy != "independent" and daily is None
    result.raw_studies = [s.name for s in (workload.studies if raw_batch else raw_reads)]

    outputs = {}
    with _study_queries(result):
        if raw_reads or raw_batch:
            with result.time("raw_read"), _reader(raw.path) as con:
                for study in raw_reads:
                    frame = _read(
                        con, f"study_{study.name}", study.design, workload.units, daily=False
                    )
                    outputs.update(_project(frame, (study,), result, daily=False))
                if raw_batch:
                    frame = _read(
                        con, "study_batched", _batch_design(aligned), workload.units, daily=False
                    )
                    outputs.update(_project(frame, aligned, result, daily=False))
        if daily is not None:
            with result.time("daily_read"), _reader(daily.path) as con:
                frame = _read(
                    con, "study_batched", _batch_design(aligned), workload.units, daily=True
                )
                outputs.update(_project(frame, aligned, result, daily=True))
    result.total_seconds = perf_counter() - started
    return result, outputs


def _oracle(raw: FileReceipt, workload: ReuseWorkload) -> dict[str, pa.Table]:
    """Independent raw-row scan: Python ints, half-open timestamps, no compiler or day state."""
    windows = [(s.start.replace(tzinfo=None), s.end.replace(tzinfo=None)) for s in workload.studies]
    counts = [[0] * workload.units for _ in windows]
    totals: list[list[int | None]] = [[None] * workload.units for _ in windows]
    with duckdb.connect(str(raw.path), read_only=True, config={"threads": _THREADS}) as db:
        roster = db.execute("FROM roster ORDER BY unit_id").fetchall()
        with db.execute("SELECT unit_id, instant, cents FROM facts").to_arrow_reader(
            65_536
        ) as rows:
            for batch in rows:
                for unit, instant, cents in zip(
                    *(column.to_pylist() for column in batch), strict=True
                ):
                    for index, (start, end) in enumerate(windows):
                        if start <= instant < end:
                            counts[index][unit] += 1
                            if cents is not None:
                                totals[index][unit] = (totals[index][unit] or 0) + cents
    outputs = {}
    for index, study in enumerate(workload.studies):
        outputs[study.name] = _oracle_study(study, roster, counts[index], totals[index])
    return outputs


def _oracle_study(
    study: ReuseStudy,
    roster: list[tuple[int, datetime | None, datetime | None]],
    counts: list[int],
    totals: list[int | None],
) -> pa.Table:
    start, end = REUSE_COVERAGE
    rows = []
    for (unit, observed_from, observed_until), count, total in zip(
        roster, counts, totals, strict=True
    ):
        lower = observed_from.replace(tzinfo=UTC) if observed_from is not None else None
        upper = observed_until.replace(tzinfo=UTC) if observed_until is not None else None
        reason = None
        if study.start < start or study.end > end:
            reason = "incomplete_coverage"
        elif (lower is not None and study.start < lower) or (
            upper is not None and study.end > upper
        ):
            reason = "incomplete_observation"
        complete = reason is None
        total_reason = "all_values_null" if complete and count and total is None else reason
        rows.append(
            (
                unit,
                "fixed",
                REUSE_ANCHOR,
                count if complete else None,
                complete,
                reason,
                Decimal(total or 0) if total_reason is None else None,
                complete,
                total_reason,
                lower,
                upper,
                "fixed_date",
                max(lower or start, start),
                min(upper or end, end),
                max(lower or start, start),
                min(upper or end, end),
            )
        )
    count, total = f"{study.name}_orders", f"{study.name}_spend"
    schema = pa.schema(
        [
            ("unit", pa.int64()),
            ("anchor_id", pa.string()),
            ("t0", _TIME),
            (count, pa.int64()),
            (f"{count}__complete", pa.bool_()),
            (f"{count}__reason", pa.string()),
            (total, pa.decimal128(38, 0)),
            (f"{total}__complete", pa.bool_()),
            (f"{total}__reason", pa.string()),
            ("_observed_from", _TIME),
            ("_observed_until", _TIME),
            ("anchor_provenance", pa.string()),
            *((f"__terrine_value_{index}_{side}", _TIME) for index in range(2) for side in _SIDES),
        ]
    )
    return pa.Table.from_arrays(
        [
            pa.array(values, type=f.type)
            for f, values in zip(schema, zip(*rows, strict=True), strict=True)
        ],
        schema=schema,
    )


def _check_outputs(
    strategy: str,
    invocation: Invocation,
    actual: dict[str, StudyOutput],
    expected: dict[str, pa.Table],
    baseline: dict[str, dict[str, Any]],
) -> None:
    if actual.keys() != expected.keys():
        raise AssertionError(f"{strategy}: study set mismatch")
    for name, target in expected.items():
        output = actual[name]
        prefix = f"{strategy} revision {invocation.revision} {name}"
        if not output.table.schema.equals(target.schema, check_metadata=True):
            raise AssertionError(
                f"{prefix}: schema mismatch\n{output.table.schema}\n!=\n{target.schema}"
            )
        if not output.table.equals(target):
            differing = [
                column
                for column in target.column_names
                if not output.table[column].equals(target[column])
            ]
            raise AssertionError(f"{prefix}: raw-row oracle mismatch in {differing}")
        if strategy == "independent":
            baseline[name] = output.metadata
        elif output.metadata != baseline[name]:
            changed = [
                key for key in output.metadata if output.metadata[key] != baseline[name][key]
            ]
            raise AssertionError(f"{prefix}: logical Arrow metadata mismatch in {changed}")


def _file_stats(receipt: FileReceipt) -> dict[str, str | int]:
    return {"rows": receipt.rows, "bytes": receipt.size, "sha256": receipt.digest}


def run(scenario: str, repeats: int) -> dict[str, Any]:
    workload = reuse_workload(scenario)
    with (
        (Path(tempfile.gettempdir()) / _WORKSPACE_LOCK).open("a") as lock,
        tempfile.TemporaryDirectory(prefix="terrine-study-reuse-") as scratch,
    ):
        fcntl.flock(lock, fcntl.LOCK_EX)
        root = Path(scratch)
        published = {
            r: _publish(root / f"input_{r}.duckdb", workload, r)
            for r, _ in dict(workload.trace).items()
        }
        expected = {r: _oracle(receipt, workload) for r, (receipt, _) in published.items()}
        reports = {s: [] for s in _STRATEGIES}
        baseline: dict[int, dict[str, dict[str, Any]]] = {r: {} for r in published}
        for repeat in range(repeats):
            # First repeat establishes baseline metadata; rotate thereafter to reduce order bias.
            offset = repeat % len(_STRATEGIES)
            for strategy in (*_STRATEGIES[offset:], *_STRATEGIES[:offset]):
                directory = root / f"{strategy}_{repeat}"
                directory.mkdir()
                store: dict[str, FileReceipt] = {}
                invocations = []
                identities = {}
                for revision, approved in workload.trace:
                    raw, _ = published[revision]
                    invocation, outputs = _workload_step(
                        strategy, revision, approved, raw, store, directory, workload
                    )
                    _check_outputs(
                        strategy, invocation, outputs, expected[revision], baseline[revision]
                    )
                    invocations.append(asdict(invocation))
                    identities[invocation.route] = {
                        name: {"design_hash": out.design_hash, "model_hash": out.model_hash}
                        for name, out in outputs.items()
                    }
                reports[strategy].append(
                    {
                        "invocations": invocations,
                        "study_identities_by_route": identities,
                        "summaries": [_file_stats(receipt) for receipt in store.values()],
                        "summary_storage_bytes": sum(receipt.size for receipt in store.values()),
                    }
                )
    cumulative = {
        strategy: [
            median(sum(call["total_seconds"] for call in run["invocations"][:k]) for run in runs)
            for k in range(1, len(workload.trace) + 1)
        ]
        for strategy, runs in reports.items()
    }
    independent, batched, daily = (cumulative[s] for s in _STRATEGIES)
    return {
        "scenario": scenario,
        "repeats": repeats,
        "units": workload.units,
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
            "ibis": ibis.__version__,
            "arrow": pa.__version__,
            "threads": _THREADS,
        },
        "input_revisions": {
            r: {**_file_stats(receipt), "publish_seconds": elapsed}
            for r, (receipt, elapsed) in published.items()
        },
        "study_windows": {
            s.name: [s.start.isoformat(), s.end.isoformat()] for s in workload.studies
        },
        "eligible_studies": [s.name for s in workload.studies if s.day_aligned],
        "compatibility_group": {
            "bound_source": "orders/facts",
            "unit_mapping": "user/unit_id",
            "measures": ["occurrence count", "signed integer cents sum"],
            "filter": None,
            "arithmetic": "exact",
            "day_boundary": "UTC",
        },
        "oracle": (
            "all public data columns and schema vs raw rows; logical metadata vs independent Frames"
        ),
        "warehouse_scan_bytes": None,
        "deployment_trace": None,
        "median_cumulative_seconds": cumulative,
        "daily_benefit_seconds_vs_independent": independent[-1] - daily[-1],
        "daily_benefit_seconds_vs_best_raw": min(independent[-1], batched[-1]) - daily[-1],
        "first_observed_daily_win_vs_independent": next(
            (i for i, (a, c) in enumerate(zip(independent, daily, strict=True), 1) if c < a), None
        ),
        "first_observed_daily_win_vs_best_raw": next(
            (
                i
                for i, (a, b, c) in enumerate(zip(independent, batched, daily, strict=True), 1)
                if c < min(a, b)
            ),
            None,
        ),
        "runs": reports,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=_SCENARIOS, required=True)
    parser.add_argument(
        "--repeats", type=int, default=3, help="independent timing passes over the trace"
    )
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    print(json.dumps(run(args.scenario, args.repeats), sort_keys=True))


if __name__ == "__main__":
    main()
