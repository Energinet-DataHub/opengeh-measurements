from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from telemetry_logging import logging_configuration
from testcommon.dataframes import (
    assert_dataframes_and_schemas,
    AssertDataframesConfiguration,
    read_csv,
)
from testcommon.etl import get_then_names, TestCases

from source.capacity_settlement.src.contracts.electricity_market__capacity_settlement.metering_point_periods_v1 import (
    metering_point_periods_v1,
)
from source.capacity_settlement.src.capacity_settlement.domain.calculation import (
    execute_core_logic,
)

from source.capacity_settlement.src.contracts.measurements_gold.time_series_points_v1 import (
    time_series_points_v1,
)
from source.capacity_settlement.tests.testsession_configuration import (
    TestSessionConfiguration,
)


@pytest.fixture(scope="session", autouse=True)
def enable_logging() -> None:
    """Prevent logging from failing due to missing logging configuration."""
    logging_configuration.configure_logging(
        cloud_role_name="some cloud role name",
        tracer_name="some tracer name",
    )


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest) -> TestCases:
    """Fixture used for scenario tests. Learn more in package `testcommon.etl`."""

    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    time_series_points = read_csv(
        spark,
        f"{scenario_path}/when/measurements_gold/time_series_points_v1.csv",
        time_series_points_v1,
    )
    metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__capacity_settlement/metering_point_periods_v1.csv",
        metering_point_periods_v1,
    )

    # Execute the calculation logic
    actual_measurements = execute_core_logic(
        time_series_points,
        metering_point_periods,
        "Europe/Copenhagen",
    )

    # Return test cases
    return TestCases(
        [
            # TODO: Add calculations.csv test case (in another PR)
            TestCase(
                expected_csv_path=f"{scenario_path}/then/measurements.csv",
                actual=actual_measurements,
            ),
        ]
    )


@pytest.fixture(scope="session")
def assert_dataframes_configuration(
    test_session_configuration: TestSessionConfiguration,
) -> AssertDataframesConfiguration:
    return AssertDataframesConfiguration(
        show_actual_and_expected_count=test_session_configuration.scenario_tests.show_actual_and_expected_count,
        show_actual_and_expected=test_session_configuration.scenario_tests.show_actual_and_expected,
        show_columns_when_actual_and_expected_are_equal=test_session_configuration.scenario_tests.show_columns_when_actual_and_expected_are_equal,
    )
