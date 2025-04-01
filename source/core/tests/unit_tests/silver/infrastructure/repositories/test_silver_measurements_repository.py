from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pytest_mock import MockerFixture

import core.silver.infrastructure.repositories.silver_measurements_repository as sut
from core.settings.silver_settings import SilverSettings
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.infrastructure.config import SilverTableNames
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository


def test__read_submitted_transactions__calls_expected(mocker: MockerFixture) -> None:
    # Arrange
    mock_spark = mocker.Mock()
    mocker.patch(f"{sut.__name__}.spark_session.initialize_spark", return_value=mock_spark)
    expected_table = f"{SilverSettings().silver_database_name}.{SilverTableNames.silver_measurements}"

    # Act
    _ = SilverMeasurementsRepository().read_submitted()

    # Assert
    mock_spark.readStream.format.assert_called_once_with("delta")
    mock_spark.readStream.format().option.assert_called_once_with("ignoreDeletes", "true")
    mock_spark.readStream.format().option().option.assert_called_once_with("skipChangeCommits", "true")
    mock_spark.readStream.format().option().option().table.assert_called_once_with(expected_table)
    mock_spark.readStream.format().option().option().table().filter.assert_called_once_with(
        f"{SilverMeasurementsColumnNames.orchestration_type} = '{GehCommonOrchestrationType.SUBMITTED.value}'"
    )
