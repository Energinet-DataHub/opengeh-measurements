from pyspark.sql import DataFrame
from electrical_heating.infrastructure.measurements_bronze.repository import Repository


def test__write_measurements__can_be_read(
    measurements_dataframe: DataFrame,
    default_catalog: str,
) -> None:
    # Arrange
    repository = Repository(default_catalog)
    excepted_count = measurements_dataframe.count()

    # Act
    repository.write_measurements(measurements_dataframe)

    # Assert
    assert repository.read_measurements().count() == excepted_count
