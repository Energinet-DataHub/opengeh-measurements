from pytest_mock import MockerFixture

import core.receipts.infrastructure.repositories.receipts_repository as sut
from core.receipts.infrastructure.config.table_names import CoreInternalTableNames
from core.receipts.infrastructure.repositories.receipts_repository import ReceiptsRepository
from core.settings.core_internal_settings import CoreInternalSettings


def test__read__calls_expected(mocker: MockerFixture) -> None:
    # Arrange
    mock_spark = mocker.Mock()
    mocker.patch(f"{sut.__name__}.spark_session.initialize_spark", return_value=mock_spark)
    expected_table = (
        f"{CoreInternalSettings().core_internal_database_name}.{CoreInternalTableNames.process_manager_receipts}"
    )

    # Act
    _ = ReceiptsRepository().read()

    # Assert
    mock_spark.readStream.format.assert_called_once_with("delta")
    mock_spark.readStream.format().option.assert_called_once_with("ignoreDeletes", "true")
    mock_spark.readStream.format().option().option.assert_called_once_with("skipChangeCommits", "true")
    mock_spark.readStream.format().option().option().table.assert_called_once_with(expected_table)
