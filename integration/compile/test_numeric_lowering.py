"""Model-backed native and exact arithmetic workflows through the compiler."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from fractions import Fraction

import ibis
import pyarrow as pa
import pytest

import terrine as tr
from terrine.spec import Design, FixedDate, agg

_T0 = datetime(2025, 1, 25, tzinfo=UTC)


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


def _model(con, units, times, amounts, *, exact=False):
    con.create_table("customers", pa.table({"customer_id": sorted(set(units))}), overwrite=True)
    con.create_table(
        "orders",
        pa.table(
            {
                "customer_id": units,
                "ordered_at": pa.array(times, pa.timestamp("us", tz="UTC")),
                "amount": pa.array(amounts, pa.float64()),
            }
        ),
        overwrite=True,
    )
    return tr.Model.from_tables(
        con,
        units={"customer": tr.UnitSource("customers", "customer_id")},
        events={
            "orders": tr.Events(
                "orders",
                time="ordered_at",
                key="customer_id",
                metrics={
                    "spend": tr.sum("amount", exact=exact),
                    "mean_spend": tr.mean("amount", exact=exact),
                    "largest": tr.max("amount", exact=exact),
                },
                complete=("2025-01-01", "2025-01-31"),
            )
        },
    )


def _hand_windows(con, start: timedelta):
    """Selected contributions from an independent half-open SQL query."""
    lower = (_T0 + start).isoformat(sep=" ")
    upper = _T0.isoformat(sep=" ")
    rows = con.sql(
        "SELECT customer_id,"
        f" list(amount) FILTER (WHERE ordered_at >= TIMESTAMPTZ '{lower}'"
        f" AND ordered_at < TIMESTAMPTZ '{upper}') AS window_values"
        " FROM orders GROUP BY customer_id"
    ).to_pyarrow()
    return {row["customer_id"]: row["window_values"] or [] for row in rows.to_pylist()}


def test_native_sum_matches_selected_hand_query(con):
    generator = random.Random(20261107)
    units = [f"u{generator.randrange(10)}" for _ in range(1000)]
    times = [
        datetime(2025, 1, 1, tzinfo=UTC) + timedelta(seconds=generator.randrange(30 * 86400))
        for _ in units
    ]
    # Thousandths: rounding each input once at 1e-9 would exceed the native bound.
    amounts = [generator.uniform(-1e-3, 1e-3) for _ in units]
    model = _model(con, units, times, amounts)
    windows = {"week": timedelta(days=-7), "hours": timedelta(hours=-84)}
    design = Design(
        "customer",
        FixedDate(_T0),
        features={
            "week": agg("spend", window=("-7d", "0s")),
            "hours": agg("spend", window=("-84h", "0s")),
            "mean_week": agg("mean_spend", window=("-7d", "0s")),
        },
    )
    frame = design.build(model)
    rows = {row["unit"]: row for row in frame.to_pyarrow().to_pylist()}
    assert len(rows) == 10
    for name, start in (*windows.items(), ("mean_week", windows["week"])):
        assert frame.columns[name].arithmetic == "native"
        for unit, window in _hand_windows(con, start).items():
            exact = sum((Fraction(value) for value in window), Fraction())
            bound = 2 * len(window) * Fraction(2**-53) * sum(map(abs, window))
            value = rows[unit][name]
            if name == "mean_week":
                expected = exact / len(window)
                bound = bound / len(window) + 2**-52 * abs(float(expected))
            else:
                expected = exact
            assert abs(Fraction(value) - expected) <= bound


@pytest.mark.parametrize("origin", ["fixed", "event", "relative", "override", "persisted"])
def test_native_recent_window_ignores_excluded_large_history(con, origin):
    model = _model(
        con,
        ["a", "a"],
        [_T0 - timedelta(days=10), _T0 - timedelta(hours=12)],
        [1e16, 1.0],
    )
    from terrine.compile.anchors import AnchorTable, compile_anchors
    from terrine.compile.columns import compile_columns, exact_scope
    from terrine.compile.numeric import _declared_origin_bounds

    anchor = tr.EventAnchor("orders") if origin == "event" else FixedDate(_T0)
    relative = tr.event_time("orders", window=("-1d", 0)) if origin == "relative" else None
    design = Design(
        "customer",
        anchor,
        features={"recent": agg("spend", window=("-1d", 0), relative_to=relative)},
    )
    if origin == "event":
        assert _declared_origin_bounds(design, model) is None
    if origin in {"override", "persisted"}:
        anchors = compile_anchors(design, model).table
        supplied = anchors.mutate(t0=ibis.literal(_T0 - timedelta(hours=12)))
        with exact_scope(model):
            compiled = compile_columns(
                design,
                model,
                _anchor_table=AnchorTable(supplied, ()) if origin == "override" else None,
                persisted_anchors=supplied if origin == "persisted" else None,
            )
        compiled.validate(execute=con.to_pyarrow)
        table = con.to_pyarrow(compiled.table)
        columns = compiled.columns
    else:
        result = design.build(model).materialize()
        table = result.table
        columns = result.columns
    row = table.to_pylist()[0]
    assert row["recent"] == (1.0 if origin == "fixed" else 0.0)
    assert (row["recent__complete"], row["recent__reason"]) == (True, None)
    assert columns["recent"].arithmetic == "native"


def test_exact_opt_in_keeps_quantized_path(con):
    # Below half a nano-unit, the exact path's one-time quantization drops the value.
    amounts = [4e-10, 4e-10]
    times = [_T0 - timedelta(days=1), _T0 - timedelta(days=2)]
    design = Design(
        "customer",
        FixedDate(_T0),
        features={name: agg(name, window=("-7d", "0s")) for name in ("spend", "largest")},
    )
    results, reasons = {}, {}
    for exact in (False, True):
        model = _model(con, ["a", "a"], times, amounts, exact=exact)
        frame = design.build(model)
        table = frame.to_pyarrow()
        results[exact] = table["spend"][0].as_py()
        reasons[exact] = (table["spend__reason"][0].as_py(), table["spend__complete"][0].as_py())
        # Every metric column records the arithmetic its declaration chose.
        mode = "exact" if exact else "native"
        assert {name: frame.columns[name].arithmetic for name in ("spend", "largest")} == {
            "spend": mode,
            "largest": mode,
        }
    # The exact path says where it discarded nonzero contributions; the native
    # path keeps them and has nothing to note. Neither makes the window incomplete.
    assert results[True] == 0.0
    assert reasons == {True: ("below_resolution", True), False: (None, True)}
    total = sum(map(Fraction, amounts), Fraction())
    assert abs(Fraction(results[False]) - total) <= 2 * len(amounts) * 2**-53 * sum(amounts)


@pytest.mark.parametrize(
    ("origins", "relative", "expected"),
    [
        ([_T0], False, ("event", "event")),
        ([_T0, _T0 + timedelta(days=1)], False, ("day", "event")),
        ([_T0, _T0 + timedelta(days=1)], True, ("event", "event")),
        (None, False, ("event", "event")),
        ([_T0, _T0], False, ("event", "event")),
    ],
)
def test_direct_batch_lowering_selects_declared_representation(con, origins, relative, expected):
    from terrine.compile.numeric import CompileContext, lower_aggregates
    from terrine.semantic.bind import normalize_events
    from terrine.temporal.aggregate import AggregateRequest
    from terrine.windows import Window

    model = _model(
        con, ["a", "a"], [_T0 - timedelta(hours=12), _T0 + timedelta(hours=12)], [2.0, 3.0]
    )
    source = model.sources["orders"]
    events = normalize_events(con.table("orders"), source, source.units["customer"])
    anchors = con.sql(
        "SELECT 'a' AS unit, id AS anchor_id, t0 FROM (VALUES "
        "('first', TIMESTAMPTZ '2025-01-25'), "
        "('last', TIMESTAMPTZ '2025-01-26')) AS a(id, t0)"
    )
    ctx = CompileContext(
        model,
        "customer",
        origins,
        (_T0, _T0 + timedelta(days=1)),
        lambda _: events,
    )
    requests = [
        AggregateRequest(model.metrics["spend"], Window("-1d", 0), "whole", "feature"),
        AggregateRequest(model.metrics["mean_spend"], Window("-18h", 0), "partial", "feature"),
        AggregateRequest(model.metrics["mean_spend"], Window("-1d", 0), "mean", "feature"),
    ]
    lowered = lower_aggregates(ctx, requests, anchors=anchors, relative=relative)
    assert lowered.strategies == {
        "whole": f"local_{expected[0]}/native_float",
        "partial": f"local_{expected[1]}/native_float",
        "mean": f"local_{expected[0]}/native_float",
    }
    assert list(lowered.result.columns) == ["whole", "partial", "mean"]
    lowered.result.validate(con.to_pyarrow)
    rows = con.to_pyarrow(lowered.result.table).to_pylist()
    assert sorted((row["whole"], row["partial"], row["mean"]) for row in rows) == [
        (2.0, 2.0, 2.0),
        (3.0, 3.0, 3.0),
    ]


def test_schedule_native_certificate_uses_last_origin_not_alignment_sample(con):
    from terrine.backends.dialect import current_dialect, dialect_scope
    from terrine.compile.numeric import _declared_origin_bounds

    first = datetime(9999, 12, 28, tzinfo=UTC)
    last = datetime(9999, 12, 30, tzinfo=UTC)
    model = _model(con, ["a", "a"], [first, last], [2.0, 3.0])
    model = tr.Model.from_tables(
        con,
        units={"customer": tr.UnitSource("customers", "customer_id")},
        events={
            "orders": tr.Events(
                "orders",
                time="ordered_at",
                key="customer_id",
                metrics={"spend": tr.sum("amount")},
                complete=("9999-12-27", "9999-12-30"),
            )
        },
    )
    design = Design(
        "customer",
        tr.Schedule("1d", "9999-12-28", "9999-12-30"),
        outcomes={"next": agg("spend", window=(0, "2d"))},
    )
    bounds = _declared_origin_bounds(design, model)
    assert bounds == (first, last)
    typed = ibis.table({"at": "timestamp('UTC')"})
    with dialect_scope("duckdb"):
        dialect = current_dialect()
        assert (
            dialect.native_shifted_comparison_keys(
                typed.at, typed.at, offset_seconds=2 * 86400, right_bounds=bounds
            )
            is None
        )
        for instant in (first, first + timedelta(days=1)):
            assert (
                dialect.native_shifted_comparison_keys(
                    typed.at, typed.at, offset_seconds=2 * 86400, right_bounds=(instant, instant)
                )
                is not None
            )
    rows = design.build(model).to_pyarrow().sort_by("t0").to_pylist()
    assert [
        (row["t0"], row["next"], row["next__complete"], row["next__reason"]) for row in rows
    ] == [
        (first, 2.0, True, None),
        (first + timedelta(days=1), 3.0, True, None),
        (last, None, False, "incomplete_coverage"),
    ]


def test_revision_one_replay_refuses_and_empty_change_refresh_recomputes(tmp_path):
    import hashlib
    import json
    from dataclasses import replace

    from terrine.data import Budget, MaterializedFrame
    from terrine.errors import TerrineError
    from terrine.runtime import (
        DestinationRef,
        ModelReference,
        RuntimeOwner,
        SavedPlan,
        SnapshotAuthority,
        dependency_graph,
        open_published_frame,
        plan_refresh,
        run_serialized_plan,
    )
    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend
    from terrine.runtime.changes import ChangeSet
    from terrine.runtime.leases import LeaseStore
    from terrine.runtime.planner import RefreshPlan
    from terrine.runtime.saved_plan import _bind_saved_plan
    from terrine.runtime.state import RuntimeStore

    state_path = tmp_path / "state.duckdb"
    con = ibis.duckdb.connect(str(state_path))
    try:
        model = _model(
            con,
            ["a", "a"],
            [_T0 - timedelta(days=10), _T0 - timedelta(hours=12)],
            [1e16, 1.0],
        )
        design = Design(
            "customer",
            FixedDate(_T0),
            features={"recent": agg("spend", window=("-1d", 0))},
        )
        authority = SnapshotAuthority(
            RuntimeOwner(
                state_path=state_path,
                snapshot_root=tmp_path / "snapshots",
            )
        )
        snapshot = authority.capture(
            model,
            design=design,
            con=con,
            namespace="owner",
            run_id="capture",
            budget=Budget(),
        )
        graph = dependency_graph(model, design, partitions=("all",))
        plan = plan_refresh(
            namespace="owner",
            dataset="study",
            definition_hash=design.spec_hash,
            snapshot_id=snapshot.snapshot_id,
            graph=graph,
        )
        # Reconstruct a valid revision-one identity, including its checksum.
        old_document = json.loads(plan.serialize())
        old_document["execution_revision"] = 1
        del old_document["fingerprint"]
        old_document["fingerprint"] = hashlib.sha256(
            json.dumps(old_document, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        old_plan = RefreshPlan.deserialize(json.dumps(old_document))
        destination = DestinationRef(str(state_path), "published", "study")
        old_saved = SavedPlan.create(
            plan_json=old_plan.serialize(),
            design=design,
            model=ModelReference.from_model(model, owner_key="model"),
            destination=destination,
            namespace="owner",
            expected_base_manifest=None,
        )
        assert SavedPlan.from_json(old_saved.to_json()) == old_saved
        frame = design.build(model).materialize()
        # Binary64 lifetime subtraction previously published (1e16 + 1) - 1e16 = 0.
        old_frame = replace(
            frame,
            table=frame.table.set_column(
                frame.table.schema.get_field_index("recent"),
                "recent",
                pa.array([0.0]),
            ),
            provenance=replace(frame.provenance, execution_revision=1),
        )
        backend = DuckDBBackend(con.con, namespace="owner")
        leases = LeaseStore(con.con, namespace="owner")
        lease = leases.acquire(
            "dataset:study",
            "historical",
            ttl=timedelta(minutes=5),
            now=datetime.now(UTC),
        )
        historical = backend.publish_generation(
            StagedGeneration(
                "study",
                "revision-one",
                {"all": old_frame.to_pyarrow()},
                snapshot_id=snapshot.snapshot_id,
                execution_revision=1,
            ),
            expected_manifest=None,
            lease=lease,
        )
        leases.release(lease)
        con.con.execute(
            "INSERT INTO owner.runtime_runs "
            "(run_id,dataset,fingerprint,state,published_generation,request_key,destination_json) "
            "VALUES ('historical','study',?,'succeeded','revision-one','same-request',?)",
            [
                _bind_saved_plan(old_saved, namespace="owner").definition.fingerprint,
                json.dumps(
                    destination.to_dict(),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ],
        )
        with pytest.raises(TerrineError) as error:
            run_serialized_plan(
                old_saved.to_json(),
                con=con.con,
                namespace="owner",
                request_key="same-request",
            )
        assert error.value.code == "terrine.runtime.saved_plan_mismatch"
        assert error.value.context["execution_revision"] == 1
        assert error.value.context["required_revision"] == 2
        revision = backend.generation_execution_revision("study", "revision-one")
        refreshed = plan_refresh(
            namespace="owner",
            dataset="study",
            definition_hash=design.spec_hash,
            snapshot_id=snapshot.snapshot_id,
            graph=graph,
            changes=ChangeSet((), "revision"),
            reuse_base_generation="revision-one",
            reuse_base_revision=revision,
        )
        assert refreshed.tasks
        saved = SavedPlan.create(
            plan_json=refreshed.serialize(),
            design=design,
            model=ModelReference.from_model(model, owner_key="model"),
            destination=destination,
            namespace="owner",
            expected_base_manifest="revision-one",
        )
        saved.pin_snapshot(RuntimeStore(con.con, namespace="owner"))

        class Resolver:
            def resolve(self, reference):
                return model

        receipt = run_serialized_plan(
            saved.to_json(),
            con=con.con,
            namespace="owner",
            request_key="refresh",
            resolver=Resolver(),
            snapshot_authority=authority,
        )
        current = open_published_frame(receipt, con=con.con, namespace="owner")
        assert current.table["recent"].to_pylist() == [pytest.approx(1.0)]
        assert current.provenance.execution_revision == 2
        assert receipt.published_generation is not None
        assert backend.generation_execution_revision("study", receipt.published_generation) == 2
        reopened = MaterializedFrame.from_arrow(backend.read_partition(historical, "all"))
        assert reopened.table["recent"].to_pylist() == [0.0]
        assert reopened.provenance.execution_revision == 1
        assert backend.generation_execution_revision("study", "revision-one") == 1
    finally:
        con.disconnect()
