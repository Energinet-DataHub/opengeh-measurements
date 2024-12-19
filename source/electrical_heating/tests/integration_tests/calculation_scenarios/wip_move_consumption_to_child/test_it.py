import pytest
from testcommon.etl import assert_dataframes, get_then_names

from integration_tests.calculation_scenarios.temp_testcommon import TestCases


@pytest.mark.parametrize("name", get_then_names())
def test_get_then_names(name: str, test_cases: TestCases) -> None:
    test_case = test_cases[name]
    assert_dataframes(test_case.actual, test_case.expected)
