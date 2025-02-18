# import os
# from unittest import mock

# from core.bronze.infrastructure.settings.storage_account_settings import StorageAccountSettings
# from core.settings.catalog_settings import CatalogSettings
# from core.silver.infrastructure.config import SilverTableNames
# from core.silver.infrastructure.streams.silver_repository import SilverRepository


# @mock.patch("core.silver.infrastructure.streams.silver_repository.get_checkpoint_path")
# def test__write_measurements__called__with_correct_arguments(mock_checkpoint_path) -> None:
#     # Arrange
#     mocked_measurements = mock.Mock()
#     expected_checkpoint_path = "/test"
#     mock_checkpoint_path.return_value = expected_checkpoint_path

#     database_name = CatalogSettings().silver_database_name  # type: ignore
#     expected_table = f"{database_name}.{SilverTableNames.silver_measurements}"
#     expected_data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT  # type: ignore
#     expected_silver_container_name = CatalogSettings().silver_container_name  # type: ignore

#     # Act
#     SilverRepository().write_measurements(mocked_measurements)

#     # Assert
#     mocked_measurements.writeStream.outputMode.assert_called_once_with("append")
#     mocked_measurements.writeStream.outputMode().option.assert_called_once_with(
#         "checkpointLocation", expected_checkpoint_path
#     )
#     mocked_measurements.writeStream.outputMode().option().trigger.assert_called_once()
#     mocked_measurements.writeStream.outputMode().option().trigger().toTable.assert_called_once_with(expected_table)

#     mock_checkpoint_path.assert_called_once_with(
#         expected_data_lake_settings, expected_silver_container_name, "submitted_transactions"
#     )


# def test__write_measurements__when_contionous_streaming_is_disabled__should_not_call_trigger() -> None:
#     # Arrange
#     os.environ["CONTINUOUS_STREAMING_ENABLED"] = "true"
#     mocked_measurements = mock.Mock()

#     # Act
#     SilverRepository().write_measurements(mocked_measurements)

#     # Assert
#     mocked_measurements.writeStream.outputMode().option().trigger.assert_not_called()
