from core.utility.shared_helpers import get_checkpoint_path, get_storage_base_path


def test__get_storage_base_path__should_return_expected():
    # Arrange
    storage_account_name = "teststorageaccount"
    container_name = "testcontainer"
    expected = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

    # Act
    result = get_storage_base_path(storage_account_name, container_name)

    # Assert
    assert result == expected


def test__get_checkpoint_path__should_return_expected():
    # Arrange
    storage_account_name = "teststorageaccount"
    container_name = "testcontainer"
    table_name = "testtable"
    expected = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/{table_name}"

    # Act
    result = get_checkpoint_path(storage_account_name, container_name, table_name)

    # Assert
    assert result == expected
