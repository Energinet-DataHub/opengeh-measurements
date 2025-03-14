import pytest

from tests import PROJECT_ROOT, TESTS_ROOT


@pytest.fixture(scope="session")
def contracts_path() -> str:
    """
    Returns the source/contract folder path.
    Please note that this only works if current folder haven't been changed prior using
    `os.chdir()`. The correctness also relies on the prerequisite that this function is
    actually located in a file located directly in the tests folder.
    """
    return f"{PROJECT_ROOT}/src/geh_calculated_measurements/capacity_settlement/contracts"


@pytest.fixture(scope="session")
def tests_path() -> str:
    """Returns the tests folder path."""
    return (TESTS_ROOT / "capacity_settlement").as_posix()
