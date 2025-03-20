import pytest
from geh_common.testing.dataframes import AssertDataframesConfiguration, assert_dataframes_and_schemas
from geh_common.testing.scenario_testing import TestCases, get_then_names


@pytest.mark.parametrize("name", get_then_names())
def test_case(
    migrations_executed: None,
    patch_environment,
    test_cases: TestCases,
    assert_dataframes_configuration: AssertDataframesConfiguration,
    name: str,
) -> None:
    with pytest.MonkeyPatch.context() as ctx:
        ctx.setenv("DATABASE_MEASUREMENTS_CALCULATED_INTERNAL", "measurements_calculated_internal")
        ctx.setenv("DATABASE_MEASUREMENTS_CALCULATED", "measurements_calculated")
        test_case = test_cases[name]
        assert_dataframes_and_schemas(
            actual=test_case.actual,
            expected=test_case.expected,
            configuration=assert_dataframes_configuration,
        )
