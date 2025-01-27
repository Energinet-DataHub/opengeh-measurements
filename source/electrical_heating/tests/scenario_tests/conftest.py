from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml
from pyspark.sql import SparkSession
from telemetry_logging import logging_configuration
from testcommon.dataframes import AssertDataframesConfiguration, read_csv
from testcommon.etl import TestCase, TestCases

from electrical_heating.application.execute_with_deps import (
    execute_calculation,
)
from electrical_heating.application.job_args.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from electrical_heating.infrastructure.electricity_market.schemas.child_metering_points_v1 import (
    child_metering_points_v1,
)
from electrical_heating.infrastructure.electricity_market.schemas.consumption_metering_point_periods_v1 import (
    consumption_metering_point_periods_v1,
)
from electrical_heating.infrastructure.measurements_gold.schemas.time_series_points_v1 import (
    time_series_points_v1,
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
    consumption_metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__electrical_heating/consumption_metering_point_periods_v1.csv",
        consumption_metering_point_periods_v1,
    )
    child_metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__electrical_heating/child_metering_points_v1.csv",
        child_metering_points_v1,
    )

    args = _create_calculation_args(f"{scenario_path}/when/")

    # Execute the calculation
    calculation_output = execute_calculation(
        spark,
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args,
        datetime.now(timezone.utc),
    )

    # Return test cases
    return TestCases(
        [
            TestCase(
                expected_csv_path=f"{scenario_path}/then/electrical_heating_internal/calculations.csv",
                actual=calculation_output.calculations,
            ),
            TestCase(
                expected_csv_path=f"{scenario_path}/then/measurements.csv",
                actual=calculation_output.measurements,
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


def _create_calculation_args(path: str) -> ElectricalHeatingArgs:
    with open(path + "job_parameters.yml", "r") as file:
        job_args = yaml.safe_load(file)[0]

    return ElectricalHeatingArgs(
        orchestration_instance_id=job_args["orchestration_instance_id"],
        time_zone=job_args["time_zone"],
        catalog_name=job_args["catalog_name"],
        period_start=job_args["period_start"],
        period_end=job_args["period_end"],
    )
