import numpy as np

from terrine.estimate.folds import assign_unit_folds


def test_fold_assignments_never_split_units_across_trials():
    units = ["u1", "u2", "u1", "u3", "u2", "u4", "u5", "u3"]
    folds = assign_unit_folds(units, folds=2, seed=5)
    assert all(len(set(folds[np.asarray(units) == unit])) == 1 for unit in set(units))
    shuffled_units = ["u3", "u1", "u4", "u2", "u5"]
    shuffled = assign_unit_folds(shuffled_units, folds=2, seed=5)
    assert dict(zip(shuffled_units, shuffled, strict=True)) == {
        unit: folds[units.index(unit)] for unit in set(units)
    }
