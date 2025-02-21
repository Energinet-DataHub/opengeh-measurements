from typing import Any

from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.infrastructure import (
    MeasurementsRepository,
)


def test__write_measurements__can_be_read(
    spark: SparkSession,
    calculated_measurements: Any,
) -> None:
    # Arrange
    repository = MeasurementsRepository(spark)
    expected_count = calculated_measurements.df.count()

    # Act
    repository.write_calculated_measurements(calculated_measurements, write_mode="overwrite")

    # Assert
    assert repository.read_calculated_measurements().df.count() == expected_count
