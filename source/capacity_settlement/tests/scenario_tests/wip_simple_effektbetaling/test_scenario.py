import pytest
from testcommon.dataframes.assert_dataframes import assert_dataframes_and_schemas
from testcommon.etl import get_then_names, TestCases


@pytest.mark.parametrize("name", get_then_names())
def test_get_then_names(name: str, test_cases: TestCases) -> None:
    test_case = test_cases[name]
    assert_dataframes_and_schemas(actual=test_case.actual, expected=test_case.expected)
