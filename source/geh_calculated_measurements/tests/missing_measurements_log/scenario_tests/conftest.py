from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest
import yaml
from geh_common.testing.dataframes import read_csv
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.missing_measurements_log.application import (
    MissingMeasurementsLogArgs,
    execute_application,
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
    metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__missing_measurements_log/metering_point_periods_v1.csv",
        ExternalDataProducts.MISSING_MEASUREMENTS_LOG_METERING_POINT_PERIODS.schema,
    )

    with open(f"{scenario_path}/when/scenario_parameters.yml") as f:
        scenario_parameters = yaml.safe_load(f)

    with (
        mock.patch(
            "geh_calculated_measurements.missing_measurements_log.infrastructure.MeteringPointPeriodsTable.read",
            return_value=metering_point_periods,
        ),
        mock.patch(
            "geh_calculated_measurements.common.infrastructure.CurrentMeasurementsRepository._read",
            return_value=current_measurements,
        ),
    ):
        args = MissingMeasurementsLogArgs.model_construct(
            orchestration_instance_id=scenario_parameters["orchestration_instance_id"],
            period_start_datetime=datetime.fromisoformat(str(scenario_parameters["period_start_datetime"])),
            period_end_datetime=datetime.fromisoformat(str(scenario_parameters["period_end_datetime"])),
            grid_area_codes=scenario_parameters["grid_area_codes"],
            catalog_name="test_catalog",
            time_zone="Europe/Copenhagen",
        )

        # Execute the logic
        actual = execute_application(
            spark,
            args,
        )

    # Sort to make the tests deterministic
    actual_df = actual.orderBy(F.col(ContractColumnNames.metering_point_id), F.col(ContractColumnNames.date))

    # Return test cases
    return TestCases(
        [
            # Cache actual in order to prevent the assertion to potentially evaluate the same DataFrame multiple times
            TestCase(expected_csv_path=f"{scenario_path}/then/missing_measurements_log.csv", actual=actual_df.cache()),
        ]
    )
