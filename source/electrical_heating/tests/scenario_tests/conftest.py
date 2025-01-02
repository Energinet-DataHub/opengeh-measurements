from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from telemetry_logging import logging_configuration
from testcommon.etl import read_csv, TestCase, TestCases

from source.electrical_heating.src.electrical_heating.domain.calculation import (
    execute_core_logic,
)
from source.electrical_heating.src.electrical_heating.infrastructure.electricity_market.schemas.child_metering_point_periods_v1 import (
    child_metering_point_periods_v1,
)
from source.electrical_heating.src.electrical_heating.infrastructure.electricity_market.schemas.consumption_metering_point_periods_v1 import (
    consumption_metering_point_periods_v1,
)

from source.electrical_heating.src.electrical_heating.infrastructure.measurements_gold.schemas.time_series_points_v1 import (
    time_series_points_v1,
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
    consumption_metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__electrical_heating/consumption_metering_point_periods_v1.csv",
        consumption_metering_point_periods_v1,
    )
    child_metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__electrical_heating/child_metering_point_periods_v1.csv",
        child_metering_point_periods_v1,
    )

    # Execute the calculation logic
    actual_measurements = execute_core_logic(
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        "Europe/Copenhagen",
    )

    # Return test cases
    return TestCases(
        [
            # TODO: Add calculations.csv test case (in another PR)
            TestCase(
                expected_csv_path=f"{scenario_path}/then/measurements",
                actual=actual_measurements,
            ),
        ]
    )
