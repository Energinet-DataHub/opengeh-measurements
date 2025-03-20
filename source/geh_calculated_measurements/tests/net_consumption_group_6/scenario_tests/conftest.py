from pathlib import Path

import pyspark.sql.types as T
import pytest
import yaml
from geh_common.testing.dataframes import (
    read_csv,
)
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession

from geh_calculated_measurements.net_consumption_group_6.domain.calculation import execute
from geh_calculated_measurements.net_consumption_group_6.domain.model import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)


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

    internal_daily = read_csv(
        spark,
        f"{scenario_path}/when/internal/daily_internal.csv",
        T.StructType(
            [
                T.StructField("metering_point_id", T.StringType(), nullable=False),
                T.StructField("last_run", T.TimestampType(), nullable=True),
            ]
        ),
    )

    with open(f"{scenario_path}/when/scenario_parameters.yml") as f:
        scenario_parameters = yaml.safe_load(f)

    # Execute the logic
    cenc, measurements = execute(
        TimeSeriesPoints(time_series_points),
        ConsumptionMeteringPointPeriods(consumption_metering_point_periods),
        ChildMeteringPoints(child_metering_points),
        internal_daily,
        "Europe/Copenhagen",
        scenario_parameters["orchestration_instance_id"],
        scenario_parameters["execution_start_datetime"],
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
