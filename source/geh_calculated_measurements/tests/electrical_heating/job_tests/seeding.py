from geh_common.data_products.electricity_market_measurements_input import (
    electrical_heating_child_metering_points_v1,
    electrical_heating_consumption_metering_point_periods_v1,
)
from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.net_consumption_group_6.infrastucture.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)
from tests.electrical_heating.job_tests import get_test_files_folder_path


def seed_gold(spark: SparkSession) -> None:
    file_name = f"{get_test_files_folder_path()}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}.csv"
    time_series_points = read_csv_path(spark, file_name, CurrentMeasurements.schema)
    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
        format="delta",
        mode="overwrite",
    )


def seed_electricity_market(spark: SparkSession) -> None:
    file_path = (
        f"{get_test_files_folder_path()}/{electrical_heating_consumption_metering_point_periods_v1.view_name}.csv"
    )
    df = read_csv_path(
        spark=spark, path=file_path, schema=electrical_heating_consumption_metering_point_periods_v1.schema
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{electrical_heating_consumption_metering_point_periods_v1.view_name}"
    )

    file_path = f"{get_test_files_folder_path()}/{electrical_heating_child_metering_points_v1.view_name}.csv"
    df = read_csv_path(spark=spark, path=file_path, schema=electrical_heating_child_metering_points_v1.schema)
    df.write.format("delta").mode("append").saveAsTable(
        f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{electrical_heating_child_metering_points_v1.view_name}"
    )
