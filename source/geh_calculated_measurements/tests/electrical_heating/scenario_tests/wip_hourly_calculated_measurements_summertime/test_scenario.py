import pytest
from geh_common.testing.dataframes import AssertDataframesConfiguration, assert_dataframes_and_schemas
from geh_common.testing.scenario_testing import TestCases, get_then_names


@pytest.mark.parametrize("name", get_then_names())
def test_case(
    migrations_executed: None,
    patch_environment,
    test_cases_for_hourly_calculated_measurements: TestCases,
    assert_dataframes_configuration: AssertDataframesConfiguration,
    name: str,
) -> None:
    test_case = test_cases_for_hourly_calculated_measurements[name]

    assert_dataframes_and_schemas(
        actual=test_case.actual,
        expected=test_case.expected,
        configuration=assert_dataframes_configuration,
    )
