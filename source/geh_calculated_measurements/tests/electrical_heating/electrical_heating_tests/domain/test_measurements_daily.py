import datetime
from decimal import Decimal

from geh_common.domain.types import MeteringPointType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from geh_calculated_measurements.electrical_heating.domain.calculated_measurements_daily import (
    CalculatedMeasurementsDaily,
    calculated_measurements_daily_schema,
)


def _create_dummy_dataframe(spark: SparkSession) -> DataFrame:
    data = [
        (
            "electrical_heating",
            "00000000-0000-0000-0000-000000000001",
            "11111111-0000-0000-0000-000000000001",
            datetime.datetime(2024, 3, 2, 23, 0),
            "1234567890123",
            MeteringPointType.ELECTRICAL_HEATING.value,
            datetime.datetime(2024, 3, 2, 23, 0),
            Decimal("0.123"),
        )
    ]
    return spark.createDataFrame(data, calculated_measurements_daily_schema)


class TestCtor:
    class TestWhenValidInput:
        def test_returns_expected_dataframe(self, spark: SparkSession) -> None:
            df = _create_dummy_dataframe(spark)

            actual = CalculatedMeasurementsDaily(df)

            assert actual.df.collect() == df.collect()

    class TestWhenInputContainsIrrelevantColumn:
        def test_returns_schema_without_irrelevant_column(self, spark: SparkSession) -> None:
            # Arrange
            df = _create_dummy_dataframe(spark)
            irrelevant_column = "irrelevant_column"
            df = df.withColumn(irrelevant_column, lit("test"))

            # Act
            actual = CalculatedMeasurementsDaily(df)

            # Assert
            assert irrelevant_column not in actual.df.schema.fieldNames()
