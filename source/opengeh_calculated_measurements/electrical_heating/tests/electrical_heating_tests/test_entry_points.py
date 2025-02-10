import tomli

from tests import PROJECT_ROOT


def test__entry_point_exists() -> None:
    with open(PROJECT_ROOT / "pyproject.toml", "rb") as file:
        pyproject = tomli.load(file)
        project = pyproject.get("project", {})
    scripts = project.get("scripts", {})
    assert "execute" in scripts, "`execute` not found in scripts"
