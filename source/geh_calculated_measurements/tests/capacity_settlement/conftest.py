import pytest

from tests import PROJECT_ROOT
from tests.testsession_configuration import TestSessionConfiguration


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
def test_session_configuration() -> TestSessionConfiguration:  # noqa: F821
    settings_file_path = PROJECT_ROOT / "tests" / "capacity_settlement" / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)
