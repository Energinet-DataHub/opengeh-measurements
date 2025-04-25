from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession

from tests.electrical_heating.job_tests import get_test_files_folder_path
from tests.external_data_products import ExternalDataProducts


def seed_gold(spark: SparkSession) -> None:
    database_name = ExternalDataProducts.CURRENT_MEASUREMENTS.database_name
    table_name = ExternalDataProducts.CURRENT_MEASUREMENTS.view_name
    schema = ExternalDataProducts.CURRENT_MEASUREMENTS.schema
    file_name = f"{get_test_files_folder_path()}/{database_name}-{table_name}.csv"
    time_series_points = read_csv_path(spark, file_name, schema)
    time_series_points.write.saveAsTable(
        f"{database_name}.{table_name}",
        format="delta",
        mode="append",
    )


def seed_electricity_market(spark: SparkSession) -> None:
    # consumption_metering_point_periods_v1
    consumption_metering_point_periods = ExternalDataProducts.ELECTRICAL_HEATING_CONSUMPTION_METERING_POINT_PERIODS
    file_path = f"{get_test_files_folder_path()}/{consumption_metering_point_periods.view_name}.csv"
    df = read_csv_path(spark=spark, path=file_path, schema=consumption_metering_point_periods.schema)
    df.write.format("delta").mode("append").saveAsTable(
        f"{consumption_metering_point_periods.database_name}.{consumption_metering_point_periods.view_name}"
    )
    # child_metering_points_v1
    child_metering_points = ExternalDataProducts.ELECTRICAL_HEATING_CHILD_METERING_POINTS
    file_path = f"{get_test_files_folder_path()}/{child_metering_points.view_name}.csv"
    df = read_csv_path(spark=spark, path=file_path, schema=child_metering_points.schema)
    df.write.format("delta").mode("append").saveAsTable(
        f"{child_metering_points.database_name}.{child_metering_points.view_name}"
    )
