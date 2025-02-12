import pytest
from geh_common.testing.dataframes import (
    assert_dataframes_and_schemas,
    AssertDataframesConfiguration,
)
from geh_common.testing.scenario_testing import get_then_names, TestCases


@pytest.mark.parametrize("name", get_then_names())
def test_case(
    name: str,
    test_cases: TestCases,
    assert_dataframes_configuration: AssertDataframesConfiguration,
) -> None:
    test_case = test_cases[name]

    assert_dataframes_and_schemas(
        actual=test_case.actual,
        expected=test_case.expected,
        configuration=assert_dataframes_configuration,
    )
