import datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from geh_calculated_measurements.electrical_heating.domain.calculated_measurements import (
    CalculatedMeasurements,
    calculated_measurements_schema,
)


def _create_dummy_dataframe(spark: SparkSession) -> DataFrame:
    data = [("1234567890123", datetime.datetime(2024, 3, 2, 23, 0), Decimal("0.123"))]
    return spark.createDataFrame(data, calculated_measurements_schema)


class TestCtor:
    class TestWhenValidInput:
        def test_returns_expected_dataframe(self, spark: SparkSession) -> None:
            df = _create_dummy_dataframe(spark)

            actual = CalculatedMeasurements(df)

            assert actual.df.collect() == df.collect()

    class TestWhenInputContainsIrrelevantColumn:
        def test_returns_schema_without_irrelevant_column(self, spark: SparkSession) -> None:
            # Arrange
            df = _create_dummy_dataframe(spark)
            irrelevant_column = "irrelevant_column"
            df = df.withColumn(irrelevant_column, lit("test"))

            # Act
            actual = CalculatedMeasurements(df)

            # Assert
            assert irrelevant_column not in actual.df.schema.fieldNames()
