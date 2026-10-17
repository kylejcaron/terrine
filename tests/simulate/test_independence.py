import subprocess
import sys


def test_generator_and_truth_cannot_import_compiler_or_estimators():
    code = """
import importlib.abc
import sys

blocked = ('terrine.semantic', 'terrine.model', 'terrine.spec', 'terrine.temporal',
           'terrine.compile', 'terrine.estimate', 'increment', 'ibis', 'duckdb')
class BlockedImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == name or fullname.startswith(name + '.') for name in blocked):
            raise AssertionError('forbidden import: ' + fullname)
sys.meta_path.insert(0, BlockedImports())
from importlib.resources import files
from terrine.simulate import WorldConfig, simulate
from terrine.simulate.truth import ate
from tests.fixtures.baseline import materialized_baseline
from tests.reference.baseline import reference_rows
w = simulate(WorldConfig(n_units=8, outcome_noise_sd=0), seed=17)
assert reference_rows(w).equals(w.analysis_table)
f = materialized_baseline(w, weighted=True)
assert f.table.num_rows == 8 and f.roles.weight == 'target_weight'
assert ate(w, weighted=True) == ate(w)
assert w.semantic_yaml == files('terrine.simulate').joinpath('baseline.yaml').read_text()
assert not any(name == prefix or name.startswith(prefix + '.')
               for name in sys.modules for prefix in blocked)
print('independent-world-resource-and-fixture-ok')
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "independent-world-resource-and-fixture-ok"
