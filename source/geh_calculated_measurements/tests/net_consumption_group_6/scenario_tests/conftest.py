import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from geh_common.testing.dataframes import (
    read_csv,
)
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession

from geh_calculated_measurements.net_consumption_group_6.application.net_consumption_group_6_args import (
    NetConsumptionGroup6Args,
)
from geh_calculated_measurements.net_consumption_group_6.domain.calculation import execute
from geh_calculated_measurements.net_consumption_group_6.domain.model import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)

_JOB_ENVIRONMENT_VARIABLES = {
    "CATALOG_NAME": "some_catalog",
    "TIME_ZONE": "Europe/Copenhagen",
    "ELECTRICITY_MARKET_DATA_PATH": "some_path",
}


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest) -> TestCases:
    """Fixture used for scenario tests. Learn more in package `geh_common`."""

    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    time_series_points = read_csv(
        spark,
        f"{scenario_path}/when/measurements_gold/time_series_points_v1.csv",
        TimeSeriesPoints.schema,
    )
    consumption_metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__net_consumption_group_6/consumption_metering_point_periods_v1.csv",
        ConsumptionMeteringPointPeriods.schema,
    )

    child_metering_points = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__net_consumption_group_6/child_metering_points_v1.csv",
        ChildMeteringPoints.schema,
    )

    with patch.dict("os.environ", _JOB_ENVIRONMENT_VARIABLES):
        with open(f"{scenario_path}/when/job_parameters.yml") as f:
            args = yaml.safe_load(f)
        with patch.object(sys, "argv", ["program"] + [f"--{k}={v}" for k, v in args.items()]):
            args = NetConsumptionGroup6Args()

    # Execute the logic
    cenc, measurements = execute(
        TimeSeriesPoints(time_series_points),
        ConsumptionMeteringPointPeriods(consumption_metering_point_periods),
        ChildMeteringPoints(child_metering_points),
        args.time_zone,
        args.orchestration_instance_id,
    )

    # Return test cases
    return TestCases(
        [
            TestCase(
                expected_csv_path=f"{scenario_path}/then/cenc.csv",
                actual=cenc.df,
            ),
            TestCase(
                expected_csv_path=f"{scenario_path}/then/measurements.csv",
                actual=measurements.df,
            ),
        ]
    )
