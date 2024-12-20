import pytest
from testcommon.etl import assert_dataframes, get_then_names

from tests.integration_tests.calculation_scenarios.temp_testcommon import TestCases


# TODO: Enable calculations.csv (in another PR)
@pytest.mark.parametrize("name", get_then_names())
def test_get_then_names(name: str, test_cases: TestCases) -> None:
    test_case = test_cases[name]
    # TODO: Does this satisfy the QA peoples requirements? Does it behave similar to the assert function from wholesale?
    assert_dataframes(actual=test_case.actual, expected=test_case.expected)
