from datetime import datetime, timezone

from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import functions as F

from tests.conftest import ExternalDataProducts
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path


def _seed_gold(
    spark,
    parent_metering_point_id: str,
    child_consumption_from_grid_metering_point: str,
    child_net_consumption_metering_point: str,
    child_supply_to_grid_metering_point: str,
) -> None:
    current_measurements = ExternalDataProducts.CURRENT_MEASUREMENTS

    # Create dataframe from the random metering point ids
    randomized_metering_point_id_df = spark.createDataFrame(
        [
            (parent_metering_point_id, "consumption"),
            (child_consumption_from_grid_metering_point, "consumption_from_grid"),
            (child_net_consumption_metering_point, "net_consumption"),
            (child_supply_to_grid_metering_point, "supply_to_grid"),
        ],
        schema=["new_metering_point_id", "metering_point_type"],
    )

    # Read test csv file
    file_name = (
        f"{get_test_files_folder_path()}/{current_measurements.database_name}-{current_measurements.view_name}.csv"
    )
    time_series_points = read_csv_path(spark, file_name, current_measurements.schema)

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
        f"{current_measurements.database_name}.{current_measurements.view_name}",
        format="delta",
        mode="append",
    )


def _seed_electricity_market(
    spark,
    parent_metering_point_id: str,
    child_consumption_from_grid_metering_point: str,
    child_net_consumption_metering_point: str,
    child_supply_to_grid_metering_point: str,
) -> None:
    # CONSUMPTION
    consumption_metering_point_periods = ExternalDataProducts.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS
    df = spark.createDataFrame(
        [
            (
                parent_metering_point_id,
                False,
                1,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                False,
            )
        ],
        schema=consumption_metering_point_periods.schema,
    )

    df.write.format("delta").mode("append").saveAsTable(
        f"{consumption_metering_point_periods.database_name}.{consumption_metering_point_periods.view_name}"
    )

    # CHILD
    child_metering_points = ExternalDataProducts.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINTS
    df = spark.createDataFrame(
        [
            (
                child_net_consumption_metering_point,
                "net_consumption",
                parent_metering_point_id,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
            ),
            (
                child_supply_to_grid_metering_point,
                "supply_to_grid",
                parent_metering_point_id,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
            ),
            (
                child_consumption_from_grid_metering_point,
                "consumption_from_grid",
                parent_metering_point_id,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
            ),
        ],
        schema=child_metering_points.schema,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{child_metering_points.database_name}.{child_metering_points.view_name}"
    )


def seed(
    spark,
    parent_metering_point_id: str,
    child_consumption_from_grid_metering_point: str,
    child_net_consumption_metering_point: str,
    child_supply_to_grid_metering_point: str,
) -> None:
    _seed_gold(
        spark,
        parent_metering_point_id,
        child_consumption_from_grid_metering_point,
        child_net_consumption_metering_point,
        child_supply_to_grid_metering_point,
    )
    _seed_electricity_market(
        spark,
        parent_metering_point_id,
        child_consumption_from_grid_metering_point,
        child_net_consumption_metering_point,
        child_supply_to_grid_metering_point,
    )
