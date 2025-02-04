from pyspark.sql import SparkSession

from opengeh_bronze.infrastructure.streams.bronze_repository import BronzeRepository


def test__read_measurements__calls_spark_readStream_format_table_with_bronze_measurements_table_name(
    spark: SparkSession, migrate
):
    # Act
    actual = BronzeRepository(spark).read_measurements()

    # Assert
    assert actual.collect()
