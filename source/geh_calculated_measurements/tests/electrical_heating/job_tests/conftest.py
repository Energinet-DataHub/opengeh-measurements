import pytest
from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from tests import create_random_metering_point_id
from tests.electrical_heating.job_tests import get_test_files_folder_path

generate_metering_point_id_udf = F.udf(create_random_metering_point_id, StringType())


@pytest.fixture(scope="session")
def gold_table_seeded(
    spark: SparkSession,
    external_dataproducts_created: None,  # Used implicitly
) -> None:
    file_name = f"{get_test_files_folder_path()}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}.csv"
    time_series_points = read_csv_path(spark, file_name, CurrentMeasurements.schema)
    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
        format="delta",
        mode="append",
    )


@pytest.fixture(scope="session")
def gold_table_seeded_2(
    spark: SparkSession,
    external_dataproducts_created: None,  # Used implicitly
) -> None:
    file_name = f"{get_test_files_folder_path()}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}.csv"
    # Read csv
    time_series_points = read_csv_path(spark, file_name, CurrentMeasurements.schema)

    # Get unique metering_point_ids
    distinct_ids = time_series_points.select("metering_point_id").distinct()

    # Add a new randomized ID per original
    randomized_ids = distinct_ids.withColumn("new_metering_point_id", generate_metering_point_id_udf())

    # Join back to original DataFrame
    time_series_points = time_series_points.join(randomized_ids, on="metering_point_id", how="left")
    time_series_points = time_series_points.select(
        F.col("new_metering_point_id").alias("metering_point_id"),
        "observation_time",
        "quantity",
        "quality",
        "metering_point_type",
    )

    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
        format="delta",
        mode="append",
    )
