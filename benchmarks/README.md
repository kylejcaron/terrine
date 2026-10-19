# Dataset-scale benchmark

Run the deterministic local DuckDB workload explicitly:

```sh
uv run python -m benchmarks.run_duckdb --config benchmarks/workloads/small.json
```

The versioned generator yields bounded Arrow event batches; the runner loads each
batch into an in-memory DuckDB relation and compares per-unit target-event counts
with an independently computed oracle. It emits one JSON record with workload
parameters, input and output cardinalities, elapsed time, traced client peak
memory, and the DuckDB physical plan. Scan bytes and cost are deliberately
reported as unknown when the backend does not expose them. The benchmark is not
an ordinary unit-test performance gate; use a selected workload configuration
and retain only measurements actually produced by the command.

No live warehouse provider, account, target scale, or spend limit is selected.
Do not run paid resources until the owner supplies that gate; live results must
remain distinct from DuckDB evidence.
