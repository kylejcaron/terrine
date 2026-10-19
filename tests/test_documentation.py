from pathlib import Path


def test_readme_builds_and_reopens_first_dataset(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TERRINE_RUNTIME_NAMESPACE", raising=False)
    monkeypatch.delenv("TERRINE_RUNTIME_SNAPSHOT_ROOT", raising=False)
    source = Path(__file__).resolve().parents[1] / "README.md"
    section = source.read_text().split("## Build a study dataset\n", 1)[1]
    code = section.split("```python\n", 1)[1].split("\n```", 1)[0]
    namespace = {}
    exec(compile(code, str(source), "exec"), namespace)
    assert namespace["rows"] == [
        {"unit": "a", "sessions_before": 1, "sessions_after": 2},
        {"unit": "b", "sessions_before": 0, "sessions_after": 0},
    ]
    assert namespace["result"].roles.features == ("sessions_before",)
    assert namespace["result"].roles.outcomes == ("sessions_after",)
    assert namespace["reopened"].manifest == namespace["dataset"].manifest
