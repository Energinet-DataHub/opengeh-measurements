import pytest
from geh_common.testing.dataframes import (
    AssertDataframesConfiguration,
    assert_dataframes_and_schemas,
)
from geh_common.testing.scenario_testing import TestCases, get_then_names


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
