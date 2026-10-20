from dataclasses import replace

import pyarrow as pa
import pytest

from terrine.agent.schemas import AgentDatasetReference, DatasetColumn
from terrine.data import Budget
from terrine.dataset.manifest import DatasetManifest, ManifestColumn, ManifestGeneration
from terrine.spec import Design, FixedDate
from terrine.spec.dataset import DatasetRecipe


@pytest.fixture
def pinned_preview(tools, monkeypatch):
    table = pa.table(
        {
            "unit": pa.array([1, 2], type=pa.int32()),
            "unapproved": ["private" * 1000, "private" * 1000],
        }
    )
    recipe = DatasetRecipe(
        schema_version=1, base_design=Design("user", FixedDate("2025-01-02")), view=None
    )

    class PinnedReader:
        manifest = DatasetManifest(
            recipe=recipe,
            generation=ManifestGeneration("study", "generation", ("p1",), ("checksum",), 2),
            columns=(
                ManifestColumn("unit", "int32", "unit"),
                ManifestColumn("unapproved", "string", "support"),
            ),
            design_hash=recipe.base_design.spec_hash,
            model_hash=tools.model.model_hash,
            source_coverage={},
            source_snapshots={},
        )

        def __init__(self):
            self.calls = []
            self.closed = []

        def to_arrow_batches(self, *, batch_size):
            self.calls.append(batch_size)
            try:
                yield from table.to_batches(max_chunksize=batch_size)
            finally:
                self.closed.append(True)

    reader = PinnedReader()
    reference = AgentDatasetReference(
        dataset="study",
        generation="generation",
        definition_hash=reader.manifest.design_hash,
        recipe_reference=recipe.digest,
        model_hash=reader.manifest.model_hash,
        manifest_hash=reader.manifest.digest,
        row_count=2,
        columns=(DatasetColumn(name="unit", data_type="int32", role="unit"),),
    )
    monkeypatch.setattr(tools, "_resolve_dataset", lambda reference, permission: (reader, None))
    return tools, reference, reader


def test_preview_is_denied_by_default_and_bounded_redacted_when_granted(pinned_preview):
    tools, reference, reader = pinned_preview
    assert (
        tools.preview_rows(reference, limit=1, columns=("unit",)).code
        == "terrine.agent.permission_denied"
    )
    tools.policy = replace(
        tools.policy,
        preview=True,
        preview_columns=frozenset({"unit"}),
        redacted_columns=frozenset({"unit"}),
        batch_cap=1,
        max_preview_rows=2,
    )
    for limit in (0, -1, 3, True):
        assert tools.preview_rows(reference, limit=limit, columns=("unit",)).status == "refused"
    assert (
        tools.preview_rows(reference, limit=1, columns=("private",)).code
        == "terrine.agent.scope_denied"
    )
    result = tools.preview_rows(reference, limit=2, columns=("unit",))
    assert result.status == "ok"
    assert result.output.columns == ("unit",)
    assert result.output.rows == ((None,), (None,))
    assert reader.calls == [1] and reader.closed == [True]


@pytest.mark.parametrize(
    ("kind", "code"),
    [
        ("scan", "terrine.budget.scan_unknown"),
        ("rows", "terrine.budget.rows_exceeded"),
        ("bytes", "terrine.budget.output_bytes_exceeded"),
    ],
)
def test_row_preview_enforces_owner_budget_without_partial_output(pinned_preview, kind, code):
    tools, reference, reader = pinned_preview
    budget = {
        "scan": Budget(max_scan_bytes=0, strict_scan=True),
        "rows": Budget(max_rows=0, strict_scan=False),
        "bytes": Budget(max_output_bytes=0, strict_scan=False),
    }[kind]
    tools.policy = replace(
        tools.policy, preview=True, preview_columns=frozenset({"unit"}), budget=budget
    )
    result = tools.preview_rows(reference, limit=2, columns=("unit",))
    assert result.status == "refused" and result.code == code
    assert result.output is None and result.suggested_route
    if kind in {"scan", "rows"}:
        assert reader.calls == [] and reader.closed == []
    else:
        assert reader.calls == [2] and reader.closed == [True]


def test_row_preview_byte_budget_counts_exact_projected_arrow_buffers(pinned_preview):
    tools, reference, reader = pinned_preview
    # Two non-null int32 values occupy eight bytes, independent of the private column.
    tools.policy = replace(
        tools.policy,
        preview=True,
        preview_columns=frozenset({"unit"}),
        budget=Budget(max_rows=2, max_output_bytes=8, strict_scan=False),
    )
    exact = tools.preview_rows(reference, limit=2, columns=("unit",))
    assert exact.status == "ok" and exact.output.rows == ((1,), (2,))
    tools.policy = replace(
        tools.policy, budget=Budget(max_rows=2, max_output_bytes=7, strict_scan=False)
    )
    refused = tools.preview_rows(reference, limit=2, columns=("unit",))
    assert refused.code == "terrine.budget.output_bytes_exceeded" and refused.output is None
    assert reader.closed == [True, True]
