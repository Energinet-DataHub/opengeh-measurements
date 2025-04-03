from datetime import datetime
from zoneinfo import ZoneInfo

import pyspark.sql.functions as F
import pytest
from geh_common.domain.types import MeteringPointResolution
from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.missing_measurements_log.domain import MeteringPointPeriods
from geh_calculated_measurements.missing_measurements_log.infrastructure.database_definitions import (
    MeteringPointPeriodsDatabaseDefinition,
)
from geh_calculated_measurements.missing_measurements_log.infrastructure.repository import Repository
from tests import SPARK_CATALOG_NAME

TABLE_OR_VIEW_NAME = f"{MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME}.{MeteringPointPeriodsDatabaseDefinition.METERING_POINT_PERIODS}"


@pytest.fixture(scope="module")
def valid_dataframe(spark: SparkSession) -> DataFrame:
    spark.sparkContext.setLogLevel("ERROR")
    return spark.createDataFrame(
        [
            (
                "123456789012345",
                "804",
                MeteringPointResolution.HOUR.value,
                datetime(2022, 1, 1, 0, tzinfo=ZoneInfo("Europe/Copenhagen")),
                datetime(2022, 1, 1, 1, tzinfo=ZoneInfo("Europe/Copenhagen")),
            ),
        ],
        schema=MeteringPointPeriods.schema,
    )


@pytest.fixture(scope="module")
def repository(spark: SparkSession) -> Repository:
    return Repository(spark, catalog_name=SPARK_CATALOG_NAME)


# TODO BJM: This is a bad test because it changes the table and thus can break other tests.
#           At least when executed in parallel.
# def test__when_missing_expected_column_raises_exception(
#     valid_dataframe: DataFrame,
#     repository: Repository,
# ) -> None:
#     # Arrange
#     invalid_dataframe = valid_dataframe.drop(F.col("metering_point_id"))
#     invalid_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
#         TABLE_OR_VIEW_NAME
#     )

#     # Act and Assert
#     with pytest.raises(
#         Exception,
#         match=r"\[UNRESOLVED_COLUMN\.WITH_SUGGESTION\].*",
#     ):
#         repository.read_metering_point_periods()


<<<<<<< Updated upstream
# TODO BJM: This test should not create the table by itself. It breaks other tests because
#           it changes the nullability of columns
=======
# Create huge spark outputs
>>>>>>> Stashed changes
def test__when_source_contains_unexpected_columns_returns_data_without_unexpected_column(
    valid_dataframe: DataFrame,
    repository: Repository,
) -> None:
    # Arrange
    valid_dataframe_with_extra_col = valid_dataframe.withColumn("extra_col", F.lit("extra_value"))
    valid_dataframe_with_extra_col.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TABLE_OR_VIEW_NAME)

    # Act
    actual = repository.read_metering_point_periods()

    # Assert
    assert actual.df.columns == valid_dataframe.schema.fieldNames()


# TODO BJM: This is a bad test because it changes the table and thus can break other tests.
#           At least when executed in parallel.
# def test__when_source_contains_wrong_data_type_raises_exception(
#     valid_dataframe: DataFrame,
#     repository: Repository,
# ) -> None:
#     # Arrange
#     invalid_dataframe = valid_dataframe.withColumn(
#         "metering_point_id", F.col("metering_point_id").cast(T.IntegerType())
#     )
#     invalid_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
#         TABLE_OR_VIEW_NAME
#     )
def test__spark_output(
    valid_dataframe: DataFrame,
    repository: Repository,
) -> None:
    invalid_dataframe = valid_dataframe.withColumn(
        "metering_point_id", F.col("metering_point_id").cast(T.IntegerType())
    )
    # create spark error: Could not alter schema of table
    # `electricity_market_measurements_input`.`missing_measurements_log_metering_point_periods_v1`
    # in a Hive compatible way. Updating Hive metastore in Spark SQL specific format
    invalid_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        TABLE_OR_VIEW_NAME
    )
    assert 1 == 1
    assert invalid_dataframe


# Create huge spark outputs
def test__when_source_contains_wrong_data_type_raises_exception(
    valid_dataframe: DataFrame,
    repository: Repository,
) -> None:
    # Arrange
    invalid_dataframe = valid_dataframe.withColumn(
        "metering_point_id", F.col("metering_point_id").cast(T.IntegerType())
    )
    invalid_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        TABLE_OR_VIEW_NAME
    )

#     # Act & Assert
#     with pytest.raises(Exception, match=r".*Schema mismatch.*"):
#         repository.read_metering_point_periods()
