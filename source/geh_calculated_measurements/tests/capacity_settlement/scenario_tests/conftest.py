import sys
from pathlib import Path
from typing import Generator
from unittest import mock
from unittest.mock import patch

import geh_common.telemetry.logging_configuration as config
import pytest
import yaml
from geh_common.testing.dataframes import (
    AssertDataframesConfiguration,
    read_csv,
)
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.application.capacity_settlement_args import CapacitySettlementArgs
from geh_calculated_measurements.capacity_settlement.contracts.electricity_market__capacity_settlement.metering_point_periods_v1 import (
    metering_point_periods_v1,
)
from geh_calculated_measurements.capacity_settlement.contracts.measurements_gold.time_series_points_v1 import (
    time_series_points_v1,
)
from geh_calculated_measurements.capacity_settlement.domain.calculation import execute
from tests.capacity_settlement.testsession_configuration import (
    TestSessionConfiguration,
)


@pytest.fixture(scope="session", autouse=True)
def configure_dummy_logging(env_args_fixture_logging, script_args_fixture_logging) -> Generator[None, None, None]:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""
    with (
        mock.patch("sys.argv", script_args_fixture_logging),
        mock.patch.dict("os.environ", env_args_fixture_logging, clear=False),
        mock.patch(
            "geh_common.telemetry.logging_configuration.configure_azure_monitor"
        ),  # Patching call to configure_azure_monitor in order to not actually connect to app. insights.
    ):
        logging_settings = config.LoggingSettings()
        yield config.configure_logging(logging_settings=logging_settings, extras=None)


@pytest.fixture(scope="session")
def job_environment_variables() -> dict:
    return {
        "CATALOG_NAME": "some_catalog",
    }


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest, job_environment_variables: dict) -> TestCases:
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

    with patch.dict("os.environ", job_environment_variables):
        with open(f"{scenario_path}/when/job_parameters.yml") as f:
            args = yaml.safe_load(f)
        with patch.object(sys, "argv", ["program"] + [f"--{k}={v}" for k, v in args.items()]):
            args = CapacitySettlementArgs()

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
