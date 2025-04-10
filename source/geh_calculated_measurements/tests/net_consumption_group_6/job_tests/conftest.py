import random
from datetime import datetime, timezone
from typing import Any, Generator

import pyspark.sql.functions as F
import pytest
from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.net_consumption_group_6.infrastucture.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)
from geh_calculated_measurements.net_consumption_group_6.infrastucture.schema import (
    net_consumption_group_6_child_metering_point_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path


def create_random_metering_point_id(position=8, digit=9):
    id = "".join(random.choice("0123456789") for _ in range(18))
    return id[:position] + str(digit) + id[position + 1 :]


@pytest.fixture(scope="class")
def parent_metering_point_id() -> str:
    return create_random_metering_point_id()


@pytest.fixture(scope="class")
def child_net_consumption_metering_point() -> str:
    return create_random_metering_point_id()


@pytest.fixture(scope="class")
def child_supply_to_grid_metering_point() -> str:
    return create_random_metering_point_id()


@pytest.fixture(scope="class")
def child_consumption_from_grid_metering_point() -> str:
    return create_random_metering_point_id()


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
        mode="append",
    )


@pytest.fixture
def gold_table_seeded_randomized_metering_point(
    spark: SparkSession,
    parent_metering_point_id: str,
    child_consumption_from_grid_metering_point: str,
    child_net_consumption_metering_point: str,
    child_supply_to_grid_metering_point: str,
) -> None:
    file_name = f"{get_test_files_folder_path()}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}.csv"
    time_series_points = read_csv_path(spark, file_name, CurrentMeasurements.schema)

    randomized_metering_point_id_df = spark.createDataFrame(
        [
            (parent_metering_point_id, "consumption"),
            (child_consumption_from_grid_metering_point, "consumption_from_grid"),
            (child_net_consumption_metering_point, "net_consumption"),
            (child_supply_to_grid_metering_point, "supply_to_grid"),
        ],
        schema=["new_metering_point_id", "metering_point_type"],
    )
    time_series_points = time_series_points.join(randomized_metering_point_id_df, on="metering_point_type", how="left")

    time_series_points = time_series_points.select(
        F.col("new_metering_point_id").alias("metering_point_id"),
        "metering_point_type",
        "observation_time",
        "quantity",
        "quality",
    )
    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
        format="delta",
        mode="append",
    )


@pytest.fixture
def electricity_market_tables_seeded(
    spark: SparkSession,
    parent_metering_point_id: str,
    child_consumption_from_grid_metering_point: str,
    child_net_consumption_metering_point: str,
    child_supply_to_grid_metering_point: str,
) -> Generator[None, Any, None]:
    # PARENT
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
        schema=net_consumption_group_6_consumption_metering_point_periods_v1,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS}"
    )

    # CHILDREN
    df = spark.createDataFrame(
        [
            (
                child_net_consumption_metering_point,
                "net_consumption",
                parent_metering_point_id,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            ),
            (
                child_supply_to_grid_metering_point,
                "supply_to_grid",
                parent_metering_point_id,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            ),
            (
                child_consumption_from_grid_metering_point,
                "consumption_from_grid",
                parent_metering_point_id,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            ),
        ],
        schema=net_consumption_group_6_child_metering_point_v1,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT}"
    )

    yield

    # Clean up

    spark.sql(f"""
                DELETE FROM {ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT}
                WHERE metering_point_id = {parent_metering_point_id}
              """)

    spark.sql(f"""
                DELETE FROM {ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT}
                WHERE metering_point_id IN ({child_net_consumption_metering_point}, {child_supply_to_grid_metering_point}, {child_consumption_from_grid_metering_point})
              """)
