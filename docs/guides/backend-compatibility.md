# Backend compatibility

Terrine distinguishes verified execution from SQL compilation. Native local Ibis
DuckDB is the execution provider. An unrecognized connection—including a raw
driver connection or one that only declares `name = "duckdb"`—refuses before
source schema access with `terrine.backend.capability_unavailable`; use a local
Ibis DuckDB backend. Raw DuckDB connections remain valid for native runtime
protocols; they are not Ibis relation/execution providers.

Execution and publication share an immutable capability record without making
Model or the compiler depend on runtime. The provider preserves the caller-owned
connection, admits read-only SQL in its execution dialect, and verifies physical
relation identity. Each materialization opens an owned private session with a
transaction view and closes it after success, error or cancellation. The caller's
connection remains open. A transaction view is **not** a durable replay snapshot.

Runtime DuckDB publication retains its fenced, transactional manifest switch;
Ibis SQL compilation or DDL support alone does not establish publication safety.

PostgreSQL, Snowflake and BigQuery execution are unverified. No paid warehouse,
credentials or remote resources are accessed by the local workflow. Named dbt
relation binding is owner-gated; an existing dbt recipe is not evidence of a
reviewed relation-to-Catalog bridge.

## Operation evidence

Capabilities distinguish verified (`True`), unsupported (`False`), and unverified
(`None`). A missing capability refuses only its own operation with structured
`backend`, `operation`, `status`, and `route` context.

- Query admission requires a verified dialect; transaction-scoped execution and
  Arrow materialization have independent gates.
- Source timestamp and declared decimal fields require exact-type evidence;
  roster-only anchor compilation also requires exact timestamp semantics.
  Existing precision and overflow checks still apply within the verified domain.
- Retained input capture requires `retained_snapshot`; unknown retained snapshot
  support does not block an ordinary Frame materialization.
- Generation publication requires both `table_publication` and
  `atomic_manifest_publish`, never merely writable SQL tables.
  Dataset staging checks these and retained capture before opening destination
  state or reading sources. Losing publication evidence does not block existing
  verified reads.
- Dataset Arrow reads and local Parquet export require `arrow_batches` before
  reading the generation or creating destination files. Native unload is not
  required: local export uses bounded Arrow batches and PyArrow.
- Strict scan budgets, including local Parquet export, still require an actual
  verified scan bound. An output row or byte limit does not certify source scan
  work. Explicit non-strict policy permits execution with the existing warning.
- DuckDB has no native replay-snapshot claim (`snapshot=False`). Owner-retained
  immutable capture and its receipt are a separate verified protocol.
- Native query cancellation, source change-feed evidence, scan estimates/limits
  and native Parquet unload are not inferred from the driver API. Cooperative
  runtime cancellation and closing owned sessions do not establish native query
  cancellation support.

`terrine.execution.backend.backend_capabilities(con)` inspects evidence without
admitting the connection or reading source schema. PostgreSQL, Snowflake,
BigQuery and unregistered backend connections return unverified operation values;
attempting execution still refuses. No optional driver extra or runtime backend
class is supplied without a real execution slice. Local PostgreSQL was not
available (`pg_isready`: no response); no service was launched.

## Executed version matrix

The local evidence below was run with Python **3.13.5**, Ibis **12.0.0**, DuckDB
driver **1.5.6** / embedded engine **v1.5.6**, and PyArrow **23.0.1**. These are
measured versions, not a claim for every release in a dependency range. Python
3.14 and other dependency combinations are unverified here.

| Operation | Local DuckDB versions above | PostgreSQL | Snowflake | BigQuery | Route or limitation |
| --- | --- | --- | --- | --- | --- |
| Read-only dialect admission, relation identity | [Verified][provider] | Unverified | Unverified | Unverified | Use native local DuckDB; an Ibis dialect/name is not execution evidence |
| Accepted UTC timestamps, exact decimals, strict t0 and SCD2 boundaries | [Verified][acceptance] | Unverified | Unverified | Unverified | Existing timestamp precision/domain and decimal overflow refusals remain active |
| Private transaction view and owned-session cleanup | [Verified][provider] | Unverified | Unverified | Unverified | Does not promise a durable source snapshot or native query cancellation |
| Retained immutable inputs and pinned generation reads | [Verified][acceptance] | Unverified | Unverified | Unverified | Use the owner-retained snapshot receipt; native source revisions remain unknown |
| Table publication and atomic fenced manifest switch | [Verified][publication] | Unverified | Unverified | Unverified | Materialize a bounded Frame if publication is unavailable |
| Bounded Arrow batch reads | [Verified][acceptance] | Unverified | Unverified | Unverified | Requires independent Arrow-batch evidence |
| Local Parquet streaming and metadata readback | [Verified][handoffs] | Unverified | Unverified | Unverified | Bounded Arrow/PyArrow path; caller-owned local destination only |
| Output row/byte limits | [Verified][exports] | Unverified | Unverified | Unverified | Do not certify source scan work or peak query memory |
| Native durable/replay snapshot | Unsupported | Unverified | Unverified | Unverified | Use retained immutable capture, not a transaction-view claim |
| Native change-feed kinds | Unverified; no kinds declared | Unverified | Unverified | Unverified | Supply separately verified change evidence; do not infer revisions |
| Native scan estimate/limit | Unverified | Unverified | Unverified | Unverified | Requested strict scan-byte budgets refuse with `terrine.budget.scan_unknown`; explicit non-strict policy warns |
| Native query cancellation | Unverified | Unverified | Unverified | Unverified | Cooperative runtime cancellation remains distinct |
| Native Parquet unload | Unverified | Unverified | Unverified | Unverified | Use verified local Arrow streaming instead |

No PostgreSQL server/driver execution was exercised. Snowflake/BigQuery remain
owner-gated; no warehouse versions, auth integrations or paid-resource support are
claimed. Unknown backends refuse with an actionable local route before schema or
source access. The [operation-gate tests][gates] separately prove that missing
publication, retained capture or Arrow evidence does not disable unrelated work.

| Common-stack component | Measured version | Exercised contract |
| --- | --- | --- |
| PyArrow | 23.0.1 | [Typed values, reserved semantic metadata, batches and Parquet][handoffs] |
| pandas | 2.3.3 | [Arrow-backed nullable dtypes, exact decimal/timestamp values and nulls][handoffs] |
| Polars | 1.44.2 | [Exact logical values and nulls through Arrow; string offset width may differ][handoffs] |
| increment | 0.1.0a2 | [Documented optional dataset-definition projection][increment] |
| dbt Core / dbt-duckdb | 1.10.13 / 1.9.6 | Installed; existing orchestration regression only. New reviewed relation binding remains owner-gated and unimplemented |

Consumer dataframes are not portable Terrine metadata containers. Preserve the
Arrow metadata and Dataset/export manifests as described in
[common-stack interoperability](common-stack-interop.md). Native Catalog works
without increment; unsupported semantic mappings are listed separately in the
[semantic-layer matrix](semantic-layer-interop.md).

[provider]: ../../tests/execution/test_backend_provider.py
[acceptance]: ../../integration/warehouse_execution/test_backend_capabilities.py
[publication]: ../../integration/warehouse_execution/test_duckdb_runtime_recovery.py
[handoffs]: ../../tests/test_common_stack_interop.py
[exports]: ../../tests/dataset/test_export.py
[gates]: ../../tests/runtime/test_backend_capabilities.py
[increment]: ../../tests/compat/test_increment_semantics.py
