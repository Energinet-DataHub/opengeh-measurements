### This file contains the fixtures that are used in the tests. ###
from pathlib import Path
from typing import Generator

import pytest
from geh_common.telemetry.logging_configuration import configure_logging
from geh_common.testing.delta_lake import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.opengeh_electrical_heating.infrastructure import (
    CalculatedMeasurements,
)
from geh_calculated_measurements.opengeh_electrical_heating.infrastructure.measurements.calculated_measurements.database_definitions import (
    CalculatedMeasurementsDatabase,
)
from geh_calculated_measurements.opengeh_electrical_heating.infrastructure.measurements.calculated_measurements.schema import (
    calculated_measurements_schema,
)
from geh_calculated_measurements.opengeh_electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabase,
)
from geh_calculated_measurements.opengeh_electrical_heating.infrastructure.measurements.measurements_gold.schema import (
    time_series_points_v1,
)
from tests import PROJECT_ROOT
from tests.electrical_heating.testsession_configuration import TestSessionConfiguration
from tests.electrical_heating.utils.delta_table_utils import (
    read_from_csv,
)
from tests.electrical_heating.utils.measurements_utils import (
    create_calculated_measurements_dataframe,
)


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    """
    Clear the cache after each test module to avoid memory issues.
    """
    yield
    spark.catalog.clearCache()


@pytest.fixture(autouse=True)
def configure_dummy_logging() -> None:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""

    configure_logging(cloud_role_name="any-cloud-role-name", tracer_name="any-tracer-name")


@pytest.fixture(scope="session")
def tests_path() -> str:
    """Returns the tests folder path."""
    return (PROJECT_ROOT / "tests" / "electrical_heating").as_posix()


@pytest.fixture(scope="session")
def contracts_path() -> str:
    """Returns the source/contract folder path."""
    return (
        PROJECT_ROOT / "src" / "geh_calculated_measurements" / "opengeh_electrical_heating" / "contracts"
    ).as_posix()


@pytest.fixture(scope="session")
def test_session_configuration(tests_path: str) -> TestSessionConfiguration:
    settings_file_path = Path(tests_path) / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)


@pytest.fixture(scope="session")
def test_files_folder_path(tests_path: str) -> str:
    return f"{tests_path}/utils/test_files"


@pytest.fixture(scope="session")
def calculated_measurements(spark: SparkSession, test_files_folder_path: str) -> CalculatedMeasurements:
    create_database(spark, CalculatedMeasurementsDatabase.DATABASE_NAME)

    create_table(
        spark,
        database_name=CalculatedMeasurementsDatabase.DATABASE_NAME,
        table_name=CalculatedMeasurementsDatabase.MEASUREMENTS_NAME,
        schema=calculated_measurements_schema,
        table_location=f"{CalculatedMeasurementsDatabase.DATABASE_NAME}/{CalculatedMeasurementsDatabase.MEASUREMENTS_NAME}",
    )

    file_name = f"{test_files_folder_path}/{CalculatedMeasurementsDatabase.DATABASE_NAME}-{CalculatedMeasurementsDatabase.MEASUREMENTS_NAME}.csv"
    measurements = read_from_csv(spark, file_name)

    return create_calculated_measurements_dataframe(spark, measurements)


@pytest.fixture(scope="session")
def seed_gold_table(spark: SparkSession, test_files_folder_path: str) -> None:
    create_database(spark, MeasurementsGoldDatabase.DATABASE_NAME)

    create_table(
        spark,
        database_name=MeasurementsGoldDatabase.DATABASE_NAME,
        table_name=MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME,
        schema=time_series_points_v1,
        table_location=f"{MeasurementsGoldDatabase.DATABASE_NAME}/{MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME}",
    )

    file_name = f"{test_files_folder_path}/{MeasurementsGoldDatabase.DATABASE_NAME}-{MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME}.csv"
    time_series_points = read_from_csv(spark, file_name)
    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabase.DATABASE_NAME}.{MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME}",
        format="delta",
        mode="overwrite",
    )
