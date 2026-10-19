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
