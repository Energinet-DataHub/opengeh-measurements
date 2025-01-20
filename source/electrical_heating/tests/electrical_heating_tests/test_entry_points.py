import pytest
import tomli

from tests import PROJECT_ROOT


@pytest.mark.skip
@pytest.mark.parametrize(
    "entry_point_name",
    [
        "execute",
    ],
)
def test__entry_point_exists() -> None:
    with open(PROJECT_ROOT / "pyproject.toml", "rb") as file:
        pyproject = tomli.load(file)
        project = pyproject.get("project", {})

    package_name = project.get("name")
    scripts = pyproject.get("scripts", {})
    assert package_name in scripts, f"Package {package_name} not found in scripts"
