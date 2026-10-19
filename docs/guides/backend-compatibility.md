# Backend compatibility

Terrine distinguishes verified execution from SQL compilation. Native local DuckDB
is the execution provider. An unrecognized connection—including a connection that
only declares `name = "duckdb"`—refuses before source schema access with
`terrine.backend.capability_unavailable`; use a native local DuckDB connection.

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
- Strict scan budgets still require an actual verified scan bound. An output row
  or byte limit does not certify source scan work. Explicit non-strict policy
  permits execution with the existing warning.
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
