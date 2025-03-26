from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from geh_common.domain.types import MeteringPointResolution
from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.missing_measurements_log.domain import MeteringPointPeriods
from geh_calculated_measurements.missing_measurements_log.infrastructure.database_definitions import (
    MeteringPointPeriodsDatabaseDefinition,
)
from geh_calculated_measurements.missing_measurements_log.infrastructure.repository import (
    Repository as MeteringPointPeriodsRepository,
)

TABLE_OR_VIEW_NAME = f"{MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME}.{MeteringPointPeriodsDatabaseDefinition.METERING_POINT_PERIODS}"


@pytest.fixture(scope="module")
def valid_dataframe(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (
                "123456789012345",
                "804",
                MeteringPointResolution.HOUR.value,
                datetime(2022, 1, 1, 0),
                datetime(2022, 1, 1, 1),
            ),
        ],
        schema=MeteringPointPeriods.schema,
    )


@pytest.fixture(scope="module")
def repository(spark: SparkSession) -> MeteringPointPeriodsRepository:
    return MeteringPointPeriodsRepository(spark, catalog_name="spark_catalog")


def test__when_missing_expected_column_raises_exception(
    valid_dataframe: DataFrame,
    repository: MeteringPointPeriodsRepository,
) -> None:
    # Arrange
    invalid_dataframe = valid_dataframe.drop(F.col("metering_point_id"))
    invalid_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        TABLE_OR_VIEW_NAME
    )

    # Act and Assert
    with pytest.raises(
        Exception,
        match=r"\[UNRESOLVED_COLUMN\.WITH_SUGGESTION\].*",
    ):
        repository.read_metering_point_periods()


def test__when_source_contains_unexpected_columns_returns_data_without_unexpected_column(
    valid_dataframe: DataFrame,
    repository: MeteringPointPeriodsRepository,
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


def test__when_source_contains_wrong_data_type_raises_exception(
    valid_dataframe: DataFrame,
    repository: MeteringPointPeriodsRepository,
) -> None:
    # Arrange
    invalid_dataframe = valid_dataframe.withColumn(
        "metering_point_id", F.col("metering_point_id").cast(T.IntegerType())
    )
    invalid_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        TABLE_OR_VIEW_NAME
    )

    # Act & Assert
    with pytest.raises(Exception, match=r".*Schema mismatch.*"):
        repository.read_metering_point_periods()
