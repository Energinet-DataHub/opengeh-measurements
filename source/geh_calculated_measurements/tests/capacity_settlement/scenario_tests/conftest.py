from pathlib import Path

import pytest
from geh_common.telemetry import logging_configuration
from geh_common.testing.dataframes.assert_dataframes import AssertDataframesConfiguration
from geh_common.testing.dataframes.read_csv import read_csv
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.contracts.electricity_market__capacity_settlement.metering_point_periods_v1 import (
    metering_point_periods_v1,
)
from geh_calculated_measurements.capacity_settlement.contracts.measurements_gold.time_series_points_v1 import (
    time_series_points_v1,
)
from geh_calculated_measurements.capacity_settlement.domain.calculation import execute
from tests.capacity_settlement.scenario_tests.capacity_settlement_test_args import (
    CapacitySettlementTestArgs,
)
from tests.capacity_settlement.testsession_configuration import (
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

    args = CapacitySettlementTestArgs(f"{scenario_path}/when/job_parameters.env")
    # Execute the logic
    calculation_output = execute(
        spark,
        time_series_points,
        metering_point_periods,
        args.orchestration_instance_id,
        args.calculation_month,
        args.calculation_year,
        args.time_zone,
    )

    # Return test cases
    return TestCases(
        [
            TestCase(
                expected_csv_path=f"{scenario_path}/then/calculations.csv",
                actual=calculation_output.calculations,
            ),
            TestCase(
                expected_csv_path=f"{scenario_path}/then/measurements.csv",
                actual=calculation_output.measurements,
            ),
            TestCase(
                expected_csv_path=f"{scenario_path}/then/ten_largest_quantities.csv",
                actual=calculation_output.ten_largest_quantities,
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
