import pytest
from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path


@pytest.fixture(autouse=True)
def gold_table_seeded(
    spark: SparkSession,
    external_dataproducts_created: None,  # Used implicitly
) -> None:
    file_name = f"{get_test_files_folder_path()}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}.csv"
    time_series_points = read_csv_path(spark, file_name, CurrentMeasurements.schema)
    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
        format="delta",
        mode="overwrite",
    )
