from pyspark.sql import SparkSession

from opengeh_electrical_heating.infrastructure.measurements.measurements_bronze.wrapper import MeasurementsBronze
from opengeh_electrical_heating.infrastructure.measurements.repository import Repository


def test__write_measurements__can_be_read(
    spark: SparkSession,
    measurements_bronze: MeasurementsBronze,
) -> None:
    # Arrange
    repository = Repository(spark)
    expected_count = measurements_bronze.df.count()

    # Act
    repository.write_measurements_bronze(measurements_bronze, write_mode="overwrite")

    # Assert
    assert repository.read_measurements_bronze().df.count() == expected_count
