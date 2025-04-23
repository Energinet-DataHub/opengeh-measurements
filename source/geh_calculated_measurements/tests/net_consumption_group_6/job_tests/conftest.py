from datetime import datetime, timezone

from geh_common.data_products.electricity_market_measurements_input import (
    net_consumption_group_6_child_metering_points_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)
from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.common.infrastructure.electricity_market import (
    DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
)
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path


def seed(spark: SparkSession) -> None:
    _seed_gold_table(spark)
    _seed_electricity_market_tables(spark)


def _seed_gold_table(spark: SparkSession) -> None:
    file_name = f"{get_test_files_folder_path()}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}.csv"
    time_series_points = read_csv_path(spark, file_name, CurrentMeasurements.schema)
    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
        format="delta",
        mode="append",
    )


def _seed_electricity_market_tables(spark: SparkSession) -> None:
    # PARENT
    df = spark.createDataFrame(
        [
            (
                170000050000000201,
                False,
                1,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                False,
            )
        ],
        schema=net_consumption_group_6_consumption_metering_point_periods_v1.schema,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME}.{net_consumption_group_6_consumption_metering_point_periods_v1.view_name}"
    )

    # CHILDREN
    df = spark.createDataFrame(
        [
            (
                150000001500170200,
                "net_consumption",
                170000050000000201,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            ),
            (
                "060000001500170200",
                "supply_to_grid",
                170000050000000201,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            ),
            (
                "070000001500170200",
                "consumption_from_grid",
                170000050000000201,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            ),
        ],
        schema=net_consumption_group_6_child_metering_points_v1.schema,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME}.{net_consumption_group_6_child_metering_points_v1.view_name}"
    )
