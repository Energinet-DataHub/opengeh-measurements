from datetime import datetime
from pathlib import Path

import pytest
import yaml
from pyspark.sql import SparkSession
from telemetry_logging import logging_configuration
from testcommon.dataframes import (
    AssertDataframesConfiguration,
    read_csv,
)
from testcommon.etl import TestCase, TestCases

from contracts.electricity_market__capacity_settlement.metering_point_periods_v1 import (
    metering_point_periods_v1,
)
from contracts.measurements_gold.time_series_points_v1 import (
    time_series_points_v1,
)
from opengeh_capacity_settlement.application.job_args.capacity_settlement_args import (
    CapacitySettlementArgs,
)
from opengeh_capacity_settlement.domain.calculation import (
    execute_core_logic,
)
from tests.testsession_configuration import (
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

    args = create_calculation_args(f"{scenario_path}/when/")

    # Execute the logic
    actual_measurements = execute_core_logic(
        time_series_points,
        metering_point_periods,
        args.calculation_month,
        args.calculation_year,
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


def create_calculation_args(path: str) -> CapacitySettlementArgs:
    with open(path + "job_parameters.yml", "r") as file:
        job_args = yaml.safe_load(file)[0]

    return CapacitySettlementArgs(
        orchestration_instance_id=job_args["orchestration_instance_id"],
        calculation_month=job_args["calculation_month"],
        calculation_year=job_args["calculation_year"],
    )
