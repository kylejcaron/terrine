from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import ibis

from terrine import Budget
from terrine.dataset.reference import _definition_for
from terrine.dataset.splits import TimeSplit
from tests._timing import HANG_GUARD_SECONDS
from tests.fixtures.winsorization import create_model, design


def test_fitted_transform_reopens_without_training_rows_and_survives_publication(tmp_path):
    scoring_root = tmp_path / "scoring"
    scoring_root.mkdir()
    with TemporaryDirectory(dir=tmp_path) as training_root:
        con = ibis.duckdb.connect(str(Path(training_root) / "training.duckdb"))
        try:
            model = create_model(con)
            frame = (
                design()
                .build(model)
                .training_set(
                    label="label",
                    split=TimeSplit(train_until="2025-01-06", validation_until="2025-01-07"),
                )
            )
            fitted = frame.fit_winsorization("value", lower="0.25", upper="0.75")
            staged = frame.winsorize(fitted).stage("training", budget=Budget(strict_scan=False))
            stored = next(column for column in staged.manifest.columns if column.name == "value")
            assert (
                stored.metadata["transformation"]["fit"]["definition_hash"]
                == _definition_for(frame).digest
            )
            assert staged.manifest.definition.transforms == (fitted,)
            scoring = (
                design()
                .snapshot(model, as_of="2025-01-10", features=("value",), eligibility={})
                .winsorize(fitted)
            )
            (scoring_root / "training.json").write_bytes(_definition_for(frame).canonical_json())
            (scoring_root / "scoring.json").write_bytes(_definition_for(scoring).canonical_json())
            (scoring_root / "transform.json").write_bytes(fitted.to_json())
        finally:
            con.disconnect()
    assert not Path(training_root).exists()
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            r"""
import json, sys
from pathlib import Path
import ibis
import pyarrow as pa
from terrine import Budget
from terrine.data import MaterializedFrame
from terrine.runtime.changes import Change, ChangeSet
from terrine.spec.dataset import DatasetDefinition
from terrine.spec.transforms import Winsorization
from tests.fixtures.winsorization import bind_model
root = Path(sys.argv[1])
training_definition = DatasetDefinition.from_json((root / "training.json").read_bytes())
definition = DatasetDefinition.from_json((root / "scoring.json").read_bytes())
fitted = Winsorization.from_json((root / "transform.json").read_bytes())
assert fitted.fit.definition_hash == training_definition.digest
assert definition.transforms == (fitted,)
con = ibis.duckdb.connect(str(root / "scoring.duckdb"))
try:
    con.raw_sql("CREATE TABLE users AS SELECT * FROM (VALUES ('a'),('b')) AS units(user_id)")
    con.raw_sql("CREATE TABLE events AS SELECT user_id, ts::TIMESTAMPTZ AS ts, "
                "amount::BIGINT AS amount, 'known' AS category FROM (VALUES "
                "('a','2025-01-09 12:00:00+00',-100),"
                "('b','2025-01-09 12:00:00+00',5000)) AS points(user_id,ts,amount)")
    assert set(con.list_tables()) == {'events', 'users'}
    model = bind_model(con, scoring=True)
    frame = definition.build(model)
    output = frame.materialize()
    values = output.table.sort_by('unit')['value'].to_pylist()
    assert [str(value) for value in values] == ['5', '15']
    dataset = frame.stage('scoring_frozen', budget=Budget(strict_scan=False))
    batches = list(dataset.to_arrow_batches(batch_size=1))
    streamed = MaterializedFrame.from_arrow(pa.Table.from_batches(batches))
    assert streamed.table.sort_by('unit').equals(output.table.sort_by('unit'))
    assert streamed.columns == output.columns
    con.raw_sql("UPDATE events SET amount=CASE user_id WHEN 'a' THEN 5000 ELSE -100 END")
    refreshed = dataset.refresh(model, changes=ChangeSet((Change('update','events'),), 'operator'),
                                budget=Budget(strict_scan=False))
    batches = list(refreshed.to_arrow_batches(batch_size=1))
    changed = MaterializedFrame.from_arrow(pa.Table.from_batches(batches))
    values = changed.table.sort_by('unit')['value'].to_pylist()
    assert [str(value) for value in values] == ['15', '5']
    assert refreshed.manifest.definition.digest == dataset.manifest.definition.digest
    assert refreshed.manifest.definition.transforms == (fitted,)
    assert 'labels' not in con.list_tables()
    print(json.dumps({'lower': str(fitted.lower), 'upper': str(fitted.upper),
                      'rows': output.table.num_rows, 'changed_rows': changed.table.num_rows}))
finally:
    con.disconnect()
""",
            str(scoring_root),
        ],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=HANG_GUARD_SECONDS,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == {
        "lower": "5",
        "upper": "15",
        "rows": 2,
        "changed_rows": 2,
    }
