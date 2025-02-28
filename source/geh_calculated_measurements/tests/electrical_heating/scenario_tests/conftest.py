import sys
from pathlib import Path
from typing import Generator
from unittest import mock
from unittest.mock import patch

import geh_common.telemetry.logging_configuration as config
import pytest
import yaml
from geh_common.testing.dataframes import AssertDataframesConfiguration, read_csv
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ColumnNames
from geh_calculated_measurements.electrical_heating.application import ElectricalHeatingArgs
from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
    child_metering_points_v1,
    consumption_metering_point_periods_v1,
    execute,
    time_series_points_v1,
)
from tests.electrical_heating.testsession_configuration import (
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
        "TIME_ZONE": "Europe/Copenhagen",
        "ELECTRICITY_MARKET_DATA_PATH": "some_path",
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

    with patch.dict("os.environ", job_environment_variables):
        with open(f"{scenario_path}/when/job_parameters.yml") as f:
            args = yaml.safe_load(f)
        with patch.object(sys, "argv", ["program"] + [f"--{k}={v}" for k, v in args.items()]):
            args = ElectricalHeatingArgs()

    # Execute the logic
    actual = execute(
        TimeSeriesPoints(time_series_points),
        ConsumptionMeteringPointPeriods(consumption_metering_point_periods),
        ChildMeteringPoints(child_metering_point_periods),
        args.time_zone,
        args.orchestration_instance_id,
    )

    # Sort to make the tests deterministic
    actual = actual.df.orderBy(F.col(ColumnNames.metering_point_id), F.col(ColumnNames.date))

    # Return test cases
    return TestCases(
        [
            TestCase(
                expected_csv_path=f"{scenario_path}/then/measurements.csv",
                actual=actual,
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
