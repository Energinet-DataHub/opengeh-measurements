import pytest
from geh_common.testing.dataframes import AssertDataframesConfiguration, assert_dataframes_and_schemas
from geh_common.testing.scenario_testing import TestCases, get_then_names
from pyspark.sql import SparkSession


@pytest.mark.parametrize("name", get_then_names())
def test_case(
    spark: SparkSession,
    migrations_executed: None,
    test_cases_for_data_products: TestCases,
    assert_dataframes_configuration: AssertDataframesConfiguration,
    request: pytest.FixtureRequest,
    name: str,
) -> None:
    test_case = test_cases_for_data_products[name]

    assert_dataframes_and_schemas(
        actual=test_case.actual,
        expected=test_case.expected,
        configuration=assert_dataframes_configuration,
    )
