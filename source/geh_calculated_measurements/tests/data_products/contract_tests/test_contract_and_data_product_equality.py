import pytest
from geh_common.data_products.measurements_calculated import calculated_measurements_v1, missing_measurements_log_v1
from geh_common.testing.dataframes import assert_contract
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from geh_calculated_measurements.net_consumption_group_6.infrastucture.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)
from geh_calculated_measurements.net_consumption_group_6.infrastucture.schema import (
    net_consumption_group_6_child_metering_point_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)
from tests import SPARK_CATALOG_NAME


@pytest.mark.parametrize(
    ("database_name", "view_name", "contract_schema"),
    [
        (
            CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME,
            CalculatedMeasurementsDatabaseDefinition.CALCULATED_MEASUREMENTS_VIEW_NAME,
            calculated_measurements_v1.calculated_measurements_v1,
        ),
        (
            CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME,
            CalculatedMeasurementsDatabaseDefinition.MISSING_MEASUREMENTS_LOG_VIEW_NAME,
            missing_measurements_log_v1.missing_measurements_log_v1,
        ),
        (
            ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME,
            ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS,
            net_consumption_group_6_consumption_metering_point_periods_v1,
        ),
        (
            ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME,
            ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT,
            net_consumption_group_6_child_metering_point_v1,
        ),
    ],
)
def test_data_product_matches_contract(
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    spark: SparkSession,
    database_name: str,
    view_name: str,
    contract_schema,
) -> None:
    # Act
    view_df = spark.table(f"{SPARK_CATALOG_NAME}.{database_name}.{view_name}").limit(1)

    # Assert
    assert_contract(actual_schema=view_df.schema, contract=contract_schema)
