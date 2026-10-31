def test_root_dir_lists_every_export():
    import terrine as tr

    assert set(tr.__all__) <= set(dir(tr))


def test_recipes_dir_shows_only_recipes():
    import terrine.recipes as r

    public = {name for name in dir(r) if not name.startswith("_")}
    assert public == set(r.__all__)
