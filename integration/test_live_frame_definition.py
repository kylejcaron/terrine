import subprocess
import sys


def test_live_definition_exports_without_io_or_runtime_imports():
    code = """
import sys
from dataclasses import replace

import ibis

from terrine.spec.dataset import DatasetDefinition
from tests.fixtures.winsorization import create_model, design

con = ibis.duckdb.connect()
study = design()
frame = replace(study, unit=None).build(create_model(con))
con.disconnect()
expected = DatasetDefinition(base_design=study).canonical_json()
blocked = ('terrine.runtime', 'terrine.dataset.reference')

def forbid_effects(event, args):
    if event in ('open', 'socket.connect'):
        raise AssertionError(f'unexpected export I/O: {event} {args}')

assert not any(name == item or name.startswith(item + '.')
               for name in sys.modules for item in blocked)
sys.addaudithook(forbid_effects)
actual = frame.to_definition()
assert actual.canonical_json() == expected
assert not any(name == item or name.startswith(item + '.')
               for name in sys.modules for item in blocked)
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=120)
