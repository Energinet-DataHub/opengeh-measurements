from unittest.mock import Mock

from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType

from core.settings.silver_settings import SilverSettings
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.infrastructure.config import SilverTableNames
from core.silver.infrastructure.repositories.submitted_transactions_repository import SubmittedTransactionsRepository


def test__read_submitted_transactions__calls_expected() -> None:
    # Arrange
    mock_spark = Mock()
    expected_table = f"{SilverSettings().silver_database_name}.{SilverTableNames.silver_measurements}"

    # Act
    _ = SubmittedTransactionsRepository(mock_spark).read()

    # Assert
    mock_spark.readStream.format.assert_called_once_with("delta")
    mock_spark.readStream.format().option.assert_called_once_with("ignoreDeletes", "true")
    mock_spark.readStream.format().option().option.assert_called_once_with("skipChangeCommits", "true")
    mock_spark.readStream.format().option().option().table.assert_called_once_with(expected_table)
    mock_spark.readStream.format().option().option().table().filter.assert_called_once_with(
        f"{SilverMeasurementsColumnNames.orchestration_type} = '{GehCommonOrchestrationType.SUBMITTED.value}'"
    )
