from pyspark.sql import DataFrame, SparkSession

from opengeh_electrical_heating.infrastructure.measurements_bronze.repository import Repository


def test__write_measurements__can_be_read(
    spark: SparkSession,
    measurements: DataFrame,
) -> None:
    # Arrange
    repository = Repository(spark)
    excepted_count = measurements.count()

    # Act
    repository.write_measurements(measurements, write_mode="overwrite")

    # Assert
    assert repository.read_measurements().count() == excepted_count
