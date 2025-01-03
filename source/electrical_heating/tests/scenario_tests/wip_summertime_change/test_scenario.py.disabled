import pytest
from testcommon.etl import assert_dataframes, get_then_names, TestCases


@pytest.mark.parametrize("name", get_then_names())
def test_get_then_names(name: str, test_cases: TestCases) -> None:
    test_case = test_cases[name]
    assert_dataframes(actual=test_case.actual, expected=test_case.expected)
