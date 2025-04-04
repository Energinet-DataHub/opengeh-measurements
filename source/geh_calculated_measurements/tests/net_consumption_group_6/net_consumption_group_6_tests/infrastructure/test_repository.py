from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.net_consumption_group_6.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)
from geh_calculated_measurements.net_consumption_group_6.infrastucture.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)
from geh_calculated_measurements.net_consumption_group_6.infrastucture.repository import Repository
from tests import SPARK_CATALOG_NAME

PARENT_TABLE_NAME = f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS}"
CHILD_TABLE_NAME = f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT}"


@pytest.fixture(scope="module")
def create_electricity_market_database(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}")


@pytest.fixture(scope="module")
def valid_parent_dataframe(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (
                "170000000000000201",
                False,
                1,
                datetime(2022, 12, 31, 23),  #'2022-12-31T23:00:00Z',
                None,
                False,
            ),
        ],
        schema=ConsumptionMeteringPointPeriods.schema,
    )


@pytest.fixture(scope="module")
def valid_child_dataframe(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            ("150000001500170200", "net_consumption", "170000000000000201", datetime(2022, 12, 31, 23), None),
            ("060000001500170200", "supply_to_grid", "170000000000000201", datetime(2022, 12, 31, 23), None),
            ("070000001500170200", "consumption_from_grid", "170000000000000201", datetime(2022, 12, 31, 23), None),
        ],
        schema=ChildMeteringPoints.schema,
    )


@pytest.fixture(scope="module")
def repository(spark: SparkSession) -> Repository:
    return Repository(spark, catalog_name=SPARK_CATALOG_NAME)


# TODO BJM: This is a bad test because it changes the table and thus can break other tests.
#           At least when executed in parallel.
# def test__when_parent_table_is_missing_expected_column_raises_exception(
#     create_electricity_market_database: None,
#     valid_parent_dataframe: DataFrame,
#     repository: Repository,
# ) -> None:
#     # Arrange
#     invalid_dataframe = valid_parent_dataframe.drop(F.col("metering_point_id"))
#     invalid_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
#         PARENT_TABLE_NAME
#     )

#     # Act and Assert
#     with pytest.raises(
#         Exception,
#         match=r"\[UNRESOLVED_COLUMN\.WITH_SUGGESTION\].*",
#     ):
#         repository.read_net_consumption_group_6_consumption_metering_point_periods()

# TODO BJM: This is a bad test because it changes the table and thus can break other tests.
#           At least when executed in parallel.
# def test__when_child_table_is_missing_expected_column_raises_exception(
#     create_electricity_market_database: None,
#     valid_child_dataframe: DataFrame,
#     repository: Repository,
# ) -> None:
#     # Arrange
#     invalid_dataframe = valid_child_dataframe.drop(F.col("metering_point_id"))
#     invalid_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
#         CHILD_TABLE_NAME
#     )

#     # Act and Assert
#     with pytest.raises(
#         Exception,
#         match=r"\[UNRESOLVED_COLUMN\.WITH_SUGGESTION\].*",
#     ):
#         repository.read_net_consumption_group_6_child_metering_points()

# TODO BJM: This is a bad test because it changes the table and thus can break other tests.
#           At least when executed in parallel.
# def test__when_parent_dataframe_contains_unexpected_columns_returns_data_without_unexpected_column(
#     create_electricity_market_database: None,
#     valid_parent_dataframe: DataFrame,
#     repository: Repository,
# ) -> None:
#     # Arrange
#     valid_dataframe_with_extra_col = valid_parent_dataframe.withColumn("extra_col", F.lit("extra_value"))
#     valid_dataframe_with_extra_col.write.format("delta").mode("overwrite").option(
#         "overwriteSchema", "true"
#     ).saveAsTable(PARENT_TABLE_NAME)

#     # Act
#     actual = repository.read_net_consumption_group_6_consumption_metering_point_periods()

#     # Assert
#     assert actual.df.columns == valid_parent_dataframe.schema.fieldNames()

# TODO BJM: This is a bad test because it changes the table and thus can break other tests.
#           At least when executed in parallel.
# def test__when_child_dataframe_contains_unexpected_columns_returns_data_without_unexpected_column(
#     create_electricity_market_database: None,
#     valid_child_dataframe: DataFrame,
#     repository: Repository,
# ) -> None:
#     # Arrange
#     valid_dataframe_with_extra_col = valid_child_dataframe.withColumn("extra_col", F.lit("extra_value"))
#     valid_dataframe_with_extra_col.write.format("delta").mode("overwrite").option(
#         "overwriteSchema", "true"
#     ).saveAsTable(CHILD_TABLE_NAME)

#     # Act
#     actual = repository.read_net_consumption_group_6_child_metering_points()

#     # Assert
#     assert actual.df.columns == valid_child_dataframe.schema.fieldNames()


# TODO BJM: This is a bad test because it changes the table and thus can break other tests.
#           At least when executed in parallel.
# def test__when_parent_contains_wrong_data_type_raises_exception(
#     create_electricity_market_database: None,
#     valid_parent_dataframe: DataFrame,
#     repository: Repository,
# ) -> None:
#     # Arrange
#     invalid_dataframe = valid_parent_dataframe.withColumn(
#         "metering_point_id", F.col("metering_point_id").cast(T.IntegerType())
#     )
#     invalid_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
#         PARENT_TABLE_NAME
#     )

#     # Act & Assert
#     with pytest.raises(Exception, match=r".*Schema mismatch.*"):
#         repository.read_net_consumption_group_6_consumption_metering_point_periods()

# TODO BJM: This is a bad test because it changes the table and thus can break other tests.
#           At least when executed in parallel.
# def test__when_child_contains_wrong_data_type_raises_exception(
#     create_electricity_market_database: None,
#     valid_child_dataframe: DataFrame,
#     repository: Repository,
# ) -> None:
#     # Arrange
#     invalid_dataframe = valid_child_dataframe.withColumn(
#         "metering_point_id", F.col("metering_point_id").cast(T.IntegerType())
#     )
#     invalid_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
#         CHILD_TABLE_NAME
#     )

#     # Act & Assert
#     with pytest.raises(Exception, match=r".*Schema mismatch.*"):
#         repository.read_net_consumption_group_6_child_metering_points()
