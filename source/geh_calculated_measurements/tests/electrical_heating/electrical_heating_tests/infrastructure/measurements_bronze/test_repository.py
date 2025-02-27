import pytest
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CalculatedMeasurements
from geh_calculated_measurements.electrical_heating.infrastructure import MeasurementsGoldRepository


@pytest.mark.skip(reason="MeasurementsRepository has been changed to read from delta table not csv.")
def test__write_measurements__can_be_read(
    spark: SparkSession,
    calculated_measurements: CalculatedMeasurements,
) -> None:
    # Arrange
    repository = MeasurementsGoldRepository(spark)
    expected_count = calculated_measurements.df.count()

    # Act
    repository.write_calculated_measurements(calculated_measurements, write_mode="overwrite")

    # Assert
    assert repository.read_calculated_measurements().df.count() == expected_count
