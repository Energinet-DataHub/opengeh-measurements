from pyspark.sql import SparkSession

from opengeh_electrical_heating.infrastructure.measurements_bronze.data_structure import MeasurementsBronze
from opengeh_electrical_heating.infrastructure.measurements_bronze.repository import Repository


def test__write_measurements__can_be_read(
    spark: SparkSession,
    measurements: MeasurementsBronze,
) -> None:
    # Arrange
    repository = Repository(spark)
    excepted_count = measurements.df.count()

    # Act
    repository.write_measurements(measurements, write_mode="overwrite")

    # Assert
    assert repository.read_measurements().df.count() == excepted_count
