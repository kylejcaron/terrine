from dataclasses import FrozenInstanceError
from typing import Any, cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from terrine.agent.policy import AgentPolicy, effective_budget
from terrine.agent.tools import DatasetTools
from terrine.data import Budget
from terrine.errors import TerrineError
from terrine.runtime.saved_plan import DestinationRef


def test_policy_permissions_are_operation_specific_and_owner_configured(tmp_path):
    destination = DestinationRef("local", "published", "training")
    policy = AgentPolicy(
        allowed_sources=frozenset({"events"}),
        allowed_catalogs=frozenset({"catalog"}),
        allowed_destinations=frozenset({destination}),
        design_root=tmp_path,
    )
    policy.authorize("inspect", sources=("events",), catalog="catalog")
    policy.authorize("validate")
    for permission in ("prepare", "write", "export", "preview", "read", "cancel"):
        with pytest.raises(TerrineError) as denied:
            policy.authorize(permission)
        assert denied.value.code == "terrine.agent.permission_denied"
    for kwargs in (
        {"sources": ("private",)},
        {"catalog": "private"},
        {"destination": DestinationRef("remote", "published", "training")},
    ):
        with pytest.raises(TerrineError):
            policy.authorize("inspect", **cast(Any, kwargs))
    with pytest.raises(TypeError):
        cast(Any, AgentPolicy)(permissions={"write": True})
    with pytest.raises(FrozenInstanceError):
        cast(Any, policy).write = True
    owner = Budget(max_rows=100, max_output_bytes=1000, max_scan_bytes=5000)
    assert effective_budget(owner, None) == owner
    assert effective_budget(
        owner, Budget(max_rows=200, max_scan_bytes=2000, strict_scan=False)
    ) == Budget(
        max_rows=100,
        max_output_bytes=1000,
        max_scan_bytes=2000,
    )
    assert effective_budget(
        Budget(strict_scan=False), Budget(max_rows=5, strict_scan=False)
    ) == Budget(
        max_rows=5,
        strict_scan=False,
    )


@given(
    field=st.sampled_from(
        (
            "max_run_seconds",
            "max_concurrency",
            "max_requests",
            "max_preview_rows",
            "batch_cap",
            "max_bounded_count",
        )
    ),
    value=st.one_of(st.integers(max_value=0), st.booleans()),
)
def test_owner_resource_bounds_require_positive_integers(field, value):
    with pytest.raises(TerrineError) as error:
        AgentPolicy(**{field: value})
    assert error.value.code == "terrine.agent.owner_configuration"
    assert error.value.context == {"reason": "not_positive_integer", "field": field}


@pytest.mark.parametrize(
    ("code", "build", "context"),
    [
        (
            "terrine.agent.owner_configuration",
            lambda: AgentPolicy(inspect=cast(Any, 1)),
            {"reason": "wrong_type", "field": "inspect"},
        ),
        (
            "terrine.agent.owner_configuration",
            lambda: AgentPolicy(allowed_sources=cast(Any, 5)),
            {"reason": "wrong_type", "field": "allowed_sources"},
        ),
        (
            "terrine.agent.owner_configuration",
            lambda: AgentPolicy(allowed_sources=frozenset({""})),
            {"reason": "empty_name", "field": "allowed_sources"},
        ),
        (
            "terrine.agent.owner_configuration",
            lambda: AgentPolicy(allowed_destinations=cast(Any, frozenset({"local"}))),
            {"reason": "wrong_type", "field": "allowed_destinations"},
        ),
        (
            "terrine.agent.owner_configuration",
            lambda: AgentPolicy(budget=cast(Any, None)),
            {"reason": "wrong_type", "field": "budget"},
        ),
        (
            "terrine.agent.owner_configuration",
            lambda: effective_budget(cast(Any, None), None),
            {"reason": "wrong_type", "field": "budget"},
        ),
        (
            "terrine.agent.invalid_request",
            lambda: effective_budget(Budget(), cast(Any, 5)),
            {"reason": "wrong_type", "field": "budget"},
        ),
        (
            "terrine.agent.owner_configuration",
            lambda: DatasetTools(cast(Any, None), AgentPolicy(), namespace="agent"),
            {"reason": "wrong_type", "field": "model"},
        ),
    ],
)
def test_owner_policy_inputs_refuse_with_codes(code, build, context):
    with pytest.raises(TerrineError) as error:
        build()
    assert error.value.code == code
    assert error.value.context == context
