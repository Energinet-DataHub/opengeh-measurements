from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path


def _seed_gold_with_random_metering_point_ids(
    spark,
    parent_metering_point_id: str,
) -> None:
    # Create dataframe from the random metering point ids
    randomized_metering_point_id_df = spark.createDataFrame(
        [
            (parent_metering_point_id, "consumption"),
        ],
        schema=["new_metering_point_id", "metering_point_type"],
    )

    # Read test csv file
    file_name = f"{get_test_files_folder_path()}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}.csv"
    time_series_points = read_csv_path(spark, file_name, CurrentMeasurements.schema)

    # Join the random metering points to the test data
    time_series_points = time_series_points.join(randomized_metering_point_id_df, on="metering_point_type", how="left")
    time_series_points = time_series_points.select(
        F.col("new_metering_point_id").alias("metering_point_id"),
        "metering_point_type",
        "observation_time",
        "quantity",
        "quality",
    )

    # Persist the data to the table
    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
        format="delta",
        mode="append",
    )


def _remove_seeded_gold_data(
    spark,
    parent_metering_point_id,
    child_consumption_from_grid_metering_point,
    child_net_consumption_metering_point,
    child_supply_to_grid_metering_point,
) -> None:
    pass
