"""Execute the complete first-dataset tutorial from the README."""

from pathlib import Path


def main() -> None:
    readme = Path(__file__).resolve().parents[1] / "README.md"
    section = readme.read_text(encoding="utf-8").split("## Build a study dataset\n", 1)[1]
    code = section.split("```python\n", 1)[1].split("\n```", 1)[0]
    exec(compile(code, str(readme), "exec"), {"__name__": "__main__"})


if __name__ == "__main__":
    main()
