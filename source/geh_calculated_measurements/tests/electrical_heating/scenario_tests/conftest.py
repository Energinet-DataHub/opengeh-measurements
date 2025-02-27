from pathlib import Path

import pytest
from geh_common.telemetry import logging_configuration
from geh_common.testing.dataframes import AssertDataframesConfiguration, read_csv
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.database_migrations.migrations_runner import migrate
from geh_calculated_measurements.electrical_heating.domain import (
    ColumnNames,
    execute,
)
from geh_calculated_measurements.electrical_heating.infrastructure import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)
from geh_calculated_measurements.electrical_heating.infrastructure.electricity_market.child_metering_points.schema import (
    child_metering_points_v1,
)
from geh_calculated_measurements.electrical_heating.infrastructure.electricity_market.consumption_metering_point_periods.schema import (
    consumption_metering_point_periods_v1,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.schema import (
    time_series_points_v1,
)
from tests.electrical_heating.scenario_tests.electrical_heating_test_args import (
    ElectricalHeatingTestArgs,
)
from tests.electrical_heating.testsession_configuration import (
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

    args = ElectricalHeatingTestArgs(f"{scenario_path}/when/job_parameters.env")

    # Execute the logic
    actual = execute(
        TimeSeriesPoints(time_series_points),
        ConsumptionMeteringPointPeriods(consumption_metering_point_periods),
        ChildMeteringPoints(child_metering_point_periods),
        args.time_zone,
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


def _create_databases(spark: SparkSession) -> None:
    # """
    # Create Unity Catalog databases as they are not created by migration scripts.
    # They are created by infrastructure (in the real environments)
    # In tests they are created in the single available default database.
    # """

    spark.sql("CREATE DATABASE IF NOT EXISTS measurements_calculated")
    spark.sql("CREATE DATABASE IF NOT EXISTS measurements_calculated_internal")

    # for database in UnityCatalogDatabaseNames.get_names():
    #     print(f"Creating database {database}")
    #     spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")


@pytest.fixture(scope="session")
def migrations_executed(spark: SparkSession) -> None:
    # Execute all migrations
    _create_databases(spark)
    migrate()
