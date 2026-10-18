from terrine.data import Roles


def test_panel_roles_name_trial_and_sampling_probability_columns():
    roles = Roles(
        unit="unit",
        anchor="anchor_id",
        t0="t0",
        treatment="treatment",
        features=("x",),
        outcomes=("y",),
        trial="trial_id",
        assignment_probability="inclusion_probability",
        target_weight="target_weight",
        cluster_unit="unit",
    )

    assert roles.trial == "trial_id"
    assert roles.assignment_probability == "inclusion_probability"
    assert roles.target_weight == "target_weight"


def test_panel_roles_name_trial_aggregation_weight_column():
    roles = Roles(
        unit="unit",
        anchor="anchor_id",
        t0="t0",
        treatment="treatment",
        features=(),
        outcomes=(),
        trial="trial_id",
        trial_weight="trial_weight",
        target_weight="q",
        cluster_unit="unit",
    )

    assert roles.trial == "trial_id"
    assert roles.trial_weight == "trial_weight"
    assert roles.target_weight == "q"
