import pytest
from testcommon.etl import assert_dataframes, get_then_names, TestCases

from tests.testsession_configuration import TestSessionConfiguration


# TODO BJM: Apply changes to all scenario tests
@pytest.mark.parametrize("name", get_then_names())
def test_get_then_names(
    name: str, test_cases: TestCases, configuration: TestSessionConfiguration
) -> None:
    test_case = test_cases[name]

    assert_dataframes(
        actual=test_case.actual,
        expected=test_case.expected,
        configuration=configuration,
    )
