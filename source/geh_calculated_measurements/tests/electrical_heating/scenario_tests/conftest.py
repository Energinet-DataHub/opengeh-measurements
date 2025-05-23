from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from geh_common.testing.dataframes import read_csv
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.application.model import CalculatedMeasurementsInternal
from geh_calculated_measurements.common.domain import ContractColumnNames, CurrentMeasurements
from geh_calculated_measurements.common.domain.model import CalculatedMeasurementsDaily
from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    execute,
)
from tests.external_data_products import ExternalDataProducts


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest, dummy_logging) -> TestCases:
    """Fixture used for scenario tests. Learn more in package `testcommon.etl`."""

    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    current_measurements = read_csv(
        spark,
        f"{scenario_path}/when/measurements_gold/current_v1.csv",
        ExternalDataProducts.CURRENT_MEASUREMENTS.schema,
    )
    calculated_measurements_path = f"{scenario_path}/when/calculated_measurements_internal/calculated_measurements.csv"
    if Path(calculated_measurements_path).exists():
        calculated_measurements = read_csv(
            spark,
            f"{scenario_path}/when/calculated_measurements_internal/calculated_measurements.csv",
            CalculatedMeasurementsInternal.schema,
        )
    else:
        calculated_measurements = spark.createDataFrame([], CalculatedMeasurementsInternal.schema)

    consumption_metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__electrical_heating/consumption_metering_point_periods_v1.csv",
        ConsumptionMeteringPointPeriods.schema,
    )
    child_metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__electrical_heating/child_metering_points_v1.csv",
        ChildMeteringPoints.schema,
    )

    with open(f"{scenario_path}/when/scenario_parameters.yml") as f:
        scenario_parameters = yaml.safe_load(f)

    # Use current time as default execution start datetime if not provided
    if scenario_parameters is not None and "execution_start_datetime" in scenario_parameters:
        execution_start_datetime = scenario_parameters["execution_start_datetime"]
    else:
        execution_start_datetime = datetime.now(UTC)

    # Execute the logic
    actual: CalculatedMeasurementsDaily = execute(
        current_measurements=CurrentMeasurements(current_measurements),
        internal_calculated_measurements=CalculatedMeasurementsInternal(calculated_measurements),
        consumption_metering_point_periods=ConsumptionMeteringPointPeriods(consumption_metering_point_periods),
        child_metering_points=ChildMeteringPoints(child_metering_point_periods),
        time_zone="Europe/Copenhagen",
        execution_start_datetime=execution_start_datetime,
    )

    # Sort to make the tests deterministic
    actual_df = actual.df.orderBy(F.col(ContractColumnNames.metering_point_id), F.col(ContractColumnNames.date))

    # Return test cases
    return TestCases(
        [
            # Cache actual in order to prevent the assertion to potentially evaluate the same DataFrame multiple times
            TestCase(expected_csv_path=f"{scenario_path}/then/measurements.csv", actual=actual_df.cache()),
        ]
    )
