"""Offline contracts at the BigQuery client and Arrow download boundaries."""

from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import ibis
import pyarrow as pa
import pytest

from terrine import Coverage, Design, FixedDate, Model, UnitSource, agg, preview
from terrine.errors import TerrineError
from terrine.execution import _bigquery
from terrine.execution._metadata import invalidate_metadata, metadata_operation
from terrine.semantic.models import Catalog
from tests.execution.test_inputs import _catalog


@contextmanager
def offline_bigquery(monkeypatch, schemas, submit):
    """Use real Ibis and anonymous clients; replace only warehouse operations."""
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery as bq
    from google.cloud.bigquery_storage_v1 import BigQueryReadClient
    from ibis.backends.bigquery.datatypes import BigQuerySchema

    owner = bq.Client(project="billing", credentials=AnonymousCredentials())
    storage = BigQueryReadClient(credentials=AnonymousCredentials())
    controls = []
    admitted = []
    check_routines = _bigquery.check_routines

    def check(sql):
        check_routines(sql)
        admitted.append(sql.encode("utf-8"))

    def control(client, sql, *, job_config=None, **kwargs):
        assert client is not owner
        controls.append(sql)
        if sql == "BEGIN TRANSACTION":
            assert job_config is not None and job_config.create_session
        elif sql not in ("SELECT 1", "ROLLBACK TRANSACTION", "CALL BQ.ABORT_SESSION()"):
            raise AssertionError(f"Unexpected control statement: {sql}")
        return SimpleNamespace(
            result=lambda: None,
            session_info=SimpleNamespace(session_id="private-session"),
            destination=bq.TableReference.from_string("billing.session_data.result"),
        )

    def get_table(client, table):
        ref = bq.TableReference.from_string(table) if isinstance(table, str) else table
        metadata = bq.Table(ref, schema=BigQuerySchema.from_ibis(schemas[ref.table_id]))
        return bq.Table.from_api_repr({**metadata.to_api_repr(), "type": "TABLE"})

    def query_and_wait(client, sql, *, job_config, project):
        assert client is not owner
        assert project == "billing"
        defaults = client.default_query_job_config
        assert str(defaults.default_dataset) == "project.dataset"
        assert [(p.key, p.value) for p in defaults.connection_properties] == [
            ("session_id", "private-session")
        ]
        return submit(sql)

    def forbid_network(*args, **kwargs):
        raise AssertionError("Unexpected BigQuery API call in an offline test")

    monkeypatch.setattr(_bigquery, "check_routines", check)
    monkeypatch.setattr(bq.Client, "query", control)
    monkeypatch.setattr(bq.Client, "get_table", get_table)
    monkeypatch.setattr(bq.Client, "query_and_wait", query_and_wait)
    monkeypatch.setattr(bq.Client, "_call_api", forbid_network)
    try:
        backend = ibis.bigquery.from_connection(
            owner, storage_client=storage, dataset_id="project.dataset"
        )
        yield backend, admitted, controls
    finally:
        owner.close()
        storage.transport.close()


@pytest.fixture(autouse=True)
def _requires_bigquery():
    pytest.importorskip("google.cloud.bigquery")
    pytest.importorskip("ibis.backends.bigquery")


@pytest.mark.parametrize(
    "phase", ["storage", "submit", "result", "no_session", "config", "install", "success"]
)
def test_startup_releases_only_acquired_private_clients(monkeypatch, phase):
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery, bigquery_storage_v1

    owner = bigquery.Client(project="billing", credentials=AnonymousCredentials())
    live = set()
    sessions = {"unrelated-session"}
    fault = RuntimeError(phase)
    make_config = bigquery.QueryJobConfig

    def fail_at(point):
        if phase == point:
            raise fault

    class Client:
        def __init__(self, **kwargs):
            live.add("query")

        def query(self, sql, *, job_config=None):
            if sql != "BEGIN TRANSACTION":
                if sql == "CALL BQ.ABORT_SESSION()":
                    properties = job_config.connection_properties if job_config else ()
                    target = next((p.value for p in properties if p.key == "session_id"), None)
                    sessions.discard(target or "unrelated-session")
                return SimpleNamespace(result=lambda: None)
            fail_at("submit")
            info = None if phase == "no_session" else SimpleNamespace(session_id="new-session")

            def begin():
                fail_at("result")
                if info is not None:
                    sessions.add(info.session_id)

            return SimpleNamespace(result=begin, session_info=info)

        @property
        def default_query_job_config(self):
            return None

        @default_query_job_config.setter
        def default_query_job_config(self, value):
            fail_at("install")

        def close(self):
            live.remove("query")
            raise OSError("query transport close failed")

    def storage(**kwargs):
        fail_at("storage")
        live.add("storage")
        return SimpleNamespace(transport=SimpleNamespace(close=lambda: live.remove("storage")))

    def config(**kwargs):
        if "default_dataset" in kwargs:
            fail_at("config")
        return make_config(**kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(bigquery, "Client", Client)
            patch.setattr(bigquery_storage_v1, "BigQueryReadClient", storage)
            patch.setattr(bigquery, "QueryJobConfig", config)
            con = SimpleNamespace(client=owner, dataset="events", data_project="source")
            if phase == "success":
                session = _bigquery.owned_session(con)
                assert live == {"query", "storage"}
                session.close()
            else:
                with pytest.raises(TerrineError) as caught:
                    _bigquery.owned_session(con)
                assert caught.value.code == "terrine.execution.snapshot_unavailable"
                if phase != "no_session":
                    assert caught.value.__cause__ is fault
            assert not live
            assert sessions == {"unrelated-session"}
    finally:
        owner.close()


_EVENTS = ibis.schema({"amount": "decimal(38,9)", "ts": "timestamp('UTC')"})
_AMOUNT = Decimal("12345678901234567890.123456789")
_INSTANT = datetime(2025, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)
_BATCH = pa.record_batch(
    [
        pa.array([_AMOUNT], type=pa.decimal128(38, 9)),
        pa.array([_INSTANT], type=pa.timestamp("us", tz="UTC")),
        pa.array(["partition"]),
    ],
    names=["driver_amount", "driver_ts", "_TABLE_SUFFIX"],
)


def test_admitted_sql_preserves_scalar_and_typed_arrow_results(monkeypatch):
    submitted = []
    batches = (_BATCH, _BATCH)

    def submit(sql):
        submitted.append(sql.encode("utf-8"))
        if "COUNT(" in sql:
            return SimpleNamespace(to_arrow=lambda **_: pa.table({"driver_count": [2]}))
        return SimpleNamespace(
            to_arrow=lambda **_: pa.Table.from_batches(batches),
            to_arrow_iterable=lambda **_: iter(batches),
        )

    catalog = _catalog()
    with offline_bigquery(monkeypatch, {"events": _EVENTS}, submit) as (owner, admitted, _):
        with _bigquery.PROVIDER.open_execution(catalog, owner) as context:
            table = context.resolve(catalog.sources["events"].relation)
            count = context.execute(table.count())
            assert count.type == pa.int64()
            assert count.as_py() == 2
            expected = [{"amount": _AMOUNT, "ts": _INSTANT}] * 2
            eager = context.execute(table)
            assert eager.schema.types == [pa.decimal128(38, 9), pa.timestamp("us", tz="UTC")]
            assert eager.to_pylist() == expected
            with context.to_arrow_batches(table, chunk_size=1) as reader:
                parts = list(reader)
                assert [batch.num_rows for batch in parts] == [1, 1]
                assert pa.Table.from_batches(parts, schema=reader.schema).equals(eager)
            batches = ()
            with context.to_arrow_batches(table) as reader:
                empty = reader.read_all()
                assert empty.num_rows == 0 and empty.schema == eager.schema
        assert submitted == admitted


def test_closing_bigquery_batches_releases_the_download_without_draining(monkeypatch):
    download_steps = []

    def download(**kwargs):
        try:
            download_steps.append("first")
            yield _BATCH
            raise AssertionError("Closing the reader drained the rest of the download")
        finally:
            download_steps.append("closed")

    catalog = _catalog()
    with offline_bigquery(
        monkeypatch, {"events": _EVENTS}, lambda _: SimpleNamespace(to_arrow_iterable=download)
    ) as (owner, _, _):
        with _bigquery.PROVIDER.open_execution(catalog, owner) as context:
            table = context.resolve(catalog.sources["events"].relation)
            with context.to_arrow_batches(table) as reader:
                assert next(reader).num_rows == 1
            assert download_steps == ["first", "closed"]


@pytest.mark.parametrize(
    ("phase", "message", "exact"),
    [
        ("submit", "BIGNUMERIC overflow: value exceeds the supported range", True),
        ("download", "NUMERIC overflow: value exceeds the supported range", True),
        ("submit", "Floating point overflow in SUM", False),
        ("download", "Floating point overflow in SUM", False),
    ],
)
def test_bigquery_preserves_arithmetic_failures_and_rolls_back(monkeypatch, phase, message, exact):
    failure = RuntimeError(message)

    def download(**kwargs):
        yield _BATCH
        raise failure

    def submit(sql):
        if phase == "submit":
            raise failure
        return SimpleNamespace(to_arrow_iterable=download)

    catalog = _catalog()
    with offline_bigquery(monkeypatch, {"events": _EVENTS}, submit) as (owner, _, controls):
        with pytest.raises((RuntimeError, TerrineError)) as caught:
            with _bigquery.PROVIDER.open_execution(catalog, owner) as context:
                table = context.resolve(catalog.sources["events"].relation)
                with context.to_arrow_batches(table) as reader:
                    assert next(reader).num_rows == 1
                    next(reader)
        assert controls[-2:] == ["ROLLBACK TRANSACTION", "CALL BQ.ABORT_SESSION()"]
    if exact:
        assert isinstance(caught.value, TerrineError)
        assert caught.value.code == "terrine.temporal.exact_overflow"
        assert caught.value.__cause__ is failure
    else:
        assert caught.value is failure


_GRAPH_SQL = "SELECT * FROM right_view"
_GRAPH_SCHEMA = ibis.schema({"user_id": "string", "ts": "timestamp('UTC')", "value": "int64"})
_SHARED = {"shared_view", "base_events"}
_LEFT = _SHARED | {"left_view", "segments", "users"}
_RIGHT = _SHARED | {"right_view"}
_ALL = _LEFT | _RIGHT | {"unused_events", "unused_users"}


class _DataSubmission(Exception):
    """Stop before downloading study rows; admission must already be complete."""


@pytest.fixture
def metadata_graph(monkeypatch):
    from google.cloud import bigquery as bq
    from ibis.backends.bigquery.datatypes import BigQuerySchema

    document = _catalog().model_dump()
    document["dialect"] = "bigquery"
    left = document["sources"]["events"]
    left["relation"] = {"kind": "table", "name": "dataset.left_view"}
    for name, relation in (
        ("right", {"kind": "sql", "sql": _GRAPH_SQL}),
        ("unused", {"kind": "table", "name": "project.dataset.unused_events"}),
    ):
        document["sources"][name] = {**left, "relation": relation, "events": ()}
    left["dimensions"] = ("segments",)
    document["dimensions"]["segments"] = {
        "relation": {"kind": "table", "name": "segments"},
        "unit": "user",
        "key": "user_id",
        "properties": [],
    }
    catalog = Catalog.model_validate(document)
    units = {
        "user": UnitSource("users", "user_id"),
        "unused": UnitSource("unused_users", "user_id"),
    }
    readiness = {name: Coverage("2025-01-01", "2025-01-05") for name in catalog.sources}
    definitions = {
        "left_view": "SELECT * FROM `project.dataset.shared_view`",
        "right_view": "SELECT * FROM `project.dataset.shared_view`",
        # Fully and partially qualified spelling of the same dependency.
        "shared_view": "SELECT * FROM `dataset.base_events`",
    }
    created = {}
    types = {}
    schema = BigQuerySchema.from_ibis(_GRAPH_SCHEMA)
    closure_reads = Counter()
    ibis_reads = Counter()
    dry_runs = Counter()

    def submit(sql):
        raise _DataSubmission

    with offline_bigquery(monkeypatch, {}, submit) as (owner, _, _):
        client_query = bq.Client.query

        def get_table(client, table):
            api_reads = closure_reads if isinstance(table, str) else ibis_reads
            ref = bq.TableReference.from_string(table) if isinstance(table, str) else table
            api_reads[(client is owner.client, str(ref))] += 1
            assert ref.project == "project" and ref.dataset_id == "dataset"
            name = ref.table_id
            assert name in _ALL
            metadata = bq.Table(ref, schema=schema).to_api_repr()
            metadata["type"] = types.get(name, "VIEW" if name in definitions else "TABLE")
            metadata["creationTime"] = created.get(name, "1735689600000")
            if name in definitions:
                metadata["view"] = {"query": definitions[name], "useLegacySql": False}
            return bq.Table.from_api_repr(metadata)

        def query(client, sql, *, job_config=None, **kwargs):
            if job_config is not None and job_config.dry_run:
                assert str(job_config.default_dataset) == "project.dataset"
                assert client.project == "billing"
                assert sql.strip() == _GRAPH_SQL
                dry_runs[client is owner.client] += 1
                return SimpleNamespace(schema=schema)
            return client_query(client, sql, job_config=job_config, **kwargs)

        monkeypatch.setattr(bq.Client, "get_table", get_table)
        monkeypatch.setattr(bq.Client, "query", query)
        yield SimpleNamespace(
            owner=owner,
            catalog=catalog,
            units=units,
            definitions=definitions,
            created=created,
            types=types,
            closure_reads=closure_reads,
            ibis_reads=ibis_reads,
            dry_runs=dry_runs,
            bind=lambda: Model.from_catalog(catalog, con=owner, units=units, readiness=readiness),
        )


def _metadata_counts(names, *, caller):
    return Counter({(caller, f"project.dataset.{name}"): 1 for name in names})


def test_shared_view_metadata_is_read_once_per_qualified_object(metadata_graph):
    graph = metadata_graph
    model = graph.bind()
    assert graph.closure_reads == _metadata_counts(_ALL, caller=True)
    assert graph.ibis_reads == _metadata_counts(
        {"left_view", "unused_events", "segments", "users", "unused_users"}, caller=True
    )
    assert graph.dry_runs == Counter({True: 1})
    graph.closure_reads.clear()
    physical = model.catalog.required_relations(graph.owner, units=graph.units)
    assert [relation.name for relation in physical] == sorted(_ALL - graph.definitions.keys())
    assert graph.closure_reads == _metadata_counts(_ALL, caller=True)


@pytest.mark.parametrize("bounded_preview", [False, True], ids=["materialize", "bounded-preview"])
def test_study_admission_reads_only_selected_dependencies(metadata_graph, bounded_preview):
    graph = metadata_graph
    model = graph.bind()
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 4, tzinfo=UTC)),
        features={"total_value": agg("total", window=("-1d", "0s"))},
    )
    frame = design.build(model)
    # The catalog once admitted all roots. A now-unsupported unused input must
    # not block the study, but unscoped catalog discovery must still refuse it.
    graph.types["unused_events"] = "EXTERNAL"
    for _ in range(2):
        graph.closure_reads.clear()
        graph.ibis_reads.clear()
        graph.dry_runs.clear()
        with pytest.raises(_DataSubmission):
            if bounded_preview:
                preview(design, model, bounded_count=1)
            else:
                frame.materialize()
        caller_reads = _metadata_counts(_LEFT, caller=True)
        if bounded_preview:
            # Planning and execution each recheck the caller's selected roots.
            caller_reads += _metadata_counts(_LEFT, caller=True)
        assert graph.closure_reads == caller_reads + _metadata_counts(_LEFT, caller=False)
        assert graph.ibis_reads == _metadata_counts(
            {"left_view", "segments", "users"}, caller=False
        )
        assert not graph.dry_runs
    with pytest.raises(TerrineError) as refused:
        model.catalog.required_relations(graph.owner, units=graph.units)
    assert refused.value.code == "terrine.execution.snapshot_unavailable"


def test_bounded_preview_refuses_roster_view_changed_after_planning(metadata_graph, monkeypatch):
    from google.cloud import bigquery as bq

    graph = metadata_graph
    graph.definitions["users"] = "SELECT * FROM segments"
    model = graph.bind()
    graph.closure_reads.clear()
    query = bq.Client.query

    def change_at_begin(client, sql, **kwargs):
        if sql == "BEGIN TRANSACTION":
            assert graph.closure_reads[(True, "project.dataset.users")] == 1
            graph.definitions["users"] += " WHERE user_id IS NOT NULL"
        return query(client, sql, **kwargs)

    monkeypatch.setattr(bq.Client, "query", change_at_begin)
    design = Design("user", FixedDate(datetime(2025, 1, 4, tzinfo=UTC)))
    # The fake transport raises _DataSubmission on any study read instead.
    with pytest.raises(TerrineError) as caught:
        preview(design, model, bounded_count=1)
    assert caught.value.code == "terrine.execution.invalid_input_binding"


def test_original_root_fingerprint_rejects_direct_and_transitive_changes(metadata_graph):
    graph = metadata_graph
    model = graph.bind()
    left = model.catalog.sources["events"].relation
    right = model.catalog.sources["right"].relation

    def assert_changed(relation):
        with pytest.raises(TerrineError) as caught:
            model._relation(relation)
        assert caught.value.code == "terrine.execution.invalid_input_binding"

    # Fingerprints captured from the immutable base, before changing traversal.
    baseline_left = "35012272feb2686142d6a996433e8a4e48081f0f57e3d200e698f46429e3d1d1"
    baseline_right = "a927a2cb4ea9b49523b7ae0d0d4a34e34d7f9625d980380ac6fd12fed67991cf"
    with metadata_operation():
        assert model._relation(left).schema() == _GRAPH_SCHEMA
        assert model._relation(right).schema() == _GRAPH_SCHEMA
        assert _bigquery.admission_fingerprint(left, graph.owner) == baseline_left
        assert _bigquery.admission_fingerprint(right, graph.owner) == baseline_right
        graph.definitions["right_view"] += " WHERE value > 0"
        invalidate_metadata()
        assert_changed(right)
        assert model._relation(left).schema() == _GRAPH_SCHEMA
        assert _bigquery.admission_fingerprint(left, graph.owner) == baseline_left
    graph.definitions["shared_view"] += " WHERE value < 10"
    assert_changed(left)
    graph.definitions["shared_view"] = "SELECT * FROM `dataset.base_events`"
    graph.created["base_events"] = "1735689601000"
    assert_changed(left)


@pytest.mark.parametrize("dependency", ["cycle", "routine", "unsupported"])
def test_bigquery_refuses_unsafe_transitive_dependencies(metadata_graph, dependency):
    graph = metadata_graph
    original = graph.definitions["shared_view"]
    if dependency == "cycle":
        graph.definitions["shared_view"] = "SELECT * FROM left_view"
    elif dependency == "routine":
        graph.definitions["shared_view"] = "SELECT `project.dataset.hidden`(value) FROM base_events"
    else:
        graph.types["base_events"] = "EXTERNAL"
    with metadata_operation():
        with pytest.raises(TerrineError) as caught:
            graph.bind()
        assert caught.value.code == "terrine.execution.snapshot_unavailable"
        assert not graph.ibis_reads
        assert not graph.dry_runs
        # No rejected traversal can authorize or poison another admission.
        graph.definitions["shared_view"] = original
        graph.types.clear()
        assert graph.bind().sources["events"].relation == graph.catalog.sources["events"].relation
