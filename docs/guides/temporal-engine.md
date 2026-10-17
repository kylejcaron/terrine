# Native temporal catalogs

Terrine's native `Catalog` describes source relations and their meaning without
requiring the optional increment adapter. A catalog can be constructed from a
Python mapping with `Catalog.model_validate(...)` or loaded from safe YAML with
`load_catalog(path)`. Both paths use the same strict version-1 Pydantic models;
unknown fields, duplicate YAML keys, unsafe YAML tags, and unsupported schema
versions are rejected.

```yaml
schema_version: 1
day_boundary: UTC
dialect: duckdb
sources:
  sessions:
    relation: {kind: table, name: session_events}
    timestamp: occurred_at
    entities: {user: user_id}
    facts: [session]
    properties: []
    dimensions: []
facts:
  session: {source: sessions, column: null}
properties: {}
dimensions: {}
measures:
  session_count: {fact: session, aggregation: count, filters: []}
metrics:
  sessions: {kind: mean, entity: user, measure: session_count}
```

Here `kind: mean` describes the downstream population summary; `aggregation:
count` describes the per-unit session count. It does not average events or
change a requested feature window.

`day_boundary` accepts `UTC` or a fixed signed `HH:MM` offset, with an optional
`UTC` prefix (for example, `+05:30` or `UTC-05:00`). Coverage days are local to
that boundary and bind once to half-open UTC instants. Offsets are fixed, not
daylight-saving timezones.

Bind a catalog and explicit source completeness without scanning event rows:

```python
from datetime import UTC, datetime

import ibis

from terrine import Coverage, Model
from terrine.semantic.io import load_catalog

con = ibis.duckdb.connect()
con.create_table(
    "session_events",
    ibis.memtable({
        "occurred_at": [datetime(2025, 3, 1, 12, tzinfo=UTC)],
        "user_id": ["u-1"],
    }),
)
model = Model.from_catalog(
    load_catalog("catalog.yaml"),
    con=con,
    readiness={"sessions": Coverage("2025-03-01", "2025-03-31")},
)
```

Coverage dates are inclusive local days and are bound once to UTC instants
using `day_boundary` (for example, a UTC−05:00 boundary binds March 1 midnight
to 05:00 UTC). `Readiness` stores only these bound half-open intervals. A
source's observed maximum timestamp never implies completeness.

Logical entities map to physical source keys. A metric selecting `user` uses
only that logical key as its canonical unit key; other entity columns remain
available for explicit dimension joins and are not combined into an implicit
composite identity. Ratio inputs may select different physical columns for the
same logical entity.

Relation declarations are tagged: `{kind: table, name: ...}` names a table and
`{kind: sql, sql: ...}` declares SQL. SQL must parse as one read-only query for
the connection dialect. Metadata binding resolves relation schemas and refuses
missing or mismatched declared columns before execution. It does not create a
runtime data-verification receipt or infer table integrity.

`avg_calendar_day` is defined only on a positive whole number of aligned local
calendar days. For known anchors, both exact endpoints must be local midnight;
unknown anchors require a runtime check. Use `sum` or `avg_event` for arbitrary
instants, or choose aligned anchors and endpoints. Unsupported temporal kinds
and declared transforms retain their meaning and are refused rather than
silently approximated.

Catalog records and bound Models are immutable, including nested definition maps.
Create a new definition and bind a new Model when semantics change; an existing
model's hash cannot drift with a mutated catalog.

SQL admission traverses the whole statement, including CTEs. Writes, locks,
sequence operations and unreviewed or qualified function calls are refused.
Use a supported read-only built-in or an owner-reviewed named source relation.
This static gate does not replace least-privilege credentials or a trusted backend
catalog; it is not a sandbox for hostile database functions.
