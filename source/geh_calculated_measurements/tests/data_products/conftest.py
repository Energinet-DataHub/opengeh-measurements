from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from geh_common.testing.dataframes import write_when_files_to_delta
from geh_common.testing.scenario_testing import TestCase, TestCases, get_then_names
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.application.model import CalculatedMeasurementsInternal
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from tests import SPARK_CATALOG_NAME


@pytest.fixture(scope="module")
def execution_datetime(request: pytest.FixtureRequest):
    """Gets execution start datetime from scenario_parameters.yml if exist or defaults to datetime now."""
    scenario_path = str(Path(request.module.__file__).parent)

    try:
        with open(f"{scenario_path}/when/scenario_parameters.yml") as f:
            scenario_parameters = yaml.safe_load(f)

        # Use parameter from config file if available
        if scenario_parameters is not None and "execution_start_datetime" in scenario_parameters:
            execution_start_datetime = scenario_parameters["execution_start_datetime"]
        else:
            execution_start_datetime = datetime.now(UTC)

        return execution_start_datetime
    except FileNotFoundError:
        # Default if no config file exists
        return datetime.now(UTC)


@pytest.fixture(scope="module")
def override_current_timestamp(spark: SparkSession, execution_datetime):
    """
    Override the current_timestamp function in Spark SQL to return a fixed timestamp.
    This allows for deterministic testing of time-dependent queries.

    Args:
        spark: SparkSession
        execution_datetime: Datetime to use for current_timestamp()
    """
    # Ensure the timestamp has UTC timezone
    test_timestamp = execution_datetime
    if test_timestamp.tzinfo is None:
        test_timestamp = test_timestamp.replace(tzinfo=UTC)
    test_timestamp_str = test_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    # Set timezone to UTC
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Register UDF that returns fixed timestamp
    from pyspark.sql.types import TimestampType

    def fixed_timestamp():
        return test_timestamp

    spark.udf.register("current_timestamp", fixed_timestamp, TimestampType())

    return test_timestamp_str


@pytest.fixture(scope="module")
def test_cases(
    spark: SparkSession,
    request: pytest.FixtureRequest,
    migrations_executed: None,  # Used implicitly
    execution_datetime,  # Use this instead of directly calculating it
    override_current_timestamp,  # This now depends on execution_datetime
) -> TestCases:
    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Populate the delta tables with the 'when' files
    path_schema_tuples = [
        (
            "measurements_calculated_internal.calculated_measurements.csv",
            CalculatedMeasurementsInternal.schema,
        )
    ]
    write_when_files_to_delta(
        spark,
        scenario_path,
        path_schema_tuples,
    )

    # Receive a list of 'then' file names
    then_files = get_then_names(scenario_path)

    # Construct a list of TestCase objects
    test_cases = []

    for path_name in then_files:
        actual = spark.sql(
            f"SELECT * FROM {SPARK_CATALOG_NAME}.{CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME}.{path_name}"
        )

        test_cases.append(
            TestCase(
                expected_csv_path=f"{scenario_path}/then/{path_name}.csv",
                actual=actual,
            )
        )

    # Return test cases
    return TestCases(test_cases)
