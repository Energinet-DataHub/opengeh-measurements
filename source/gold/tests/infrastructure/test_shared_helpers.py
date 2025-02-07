from unittest.mock import patch

from opengeh_gold.infrastructure.shared_helpers import (
    EnvironmentVariable,
    get_checkpoint_path,
    get_env_variable_or_throw,
    get_full_table_name,
    get_storage_base_path,
)


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


def test__get_full_table_name__should_return_expected():
    # Arrange
    database = "testdb"
    table = "testtable"
    expected = f"{database}.{table}"

    # Act
    result = get_full_table_name(database, table)

    # Assert
    assert result == expected


@patch("os.getenv")
def test__get_env_variable_or_throw_found__should_return_expected(mock_getenv):
    # Arrange
    mock_getenv.return_value = "testvalue"
    variable = EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT

    # Act
    result = get_env_variable_or_throw(variable)

    # Assert
    assert result == "testvalue"
    mock_getenv.assert_called_once_with(variable.name)


@patch("os.getenv")
def test__get_env_variable_or_throw_not_found__should_throw_exception(mock_getenv):
    # Arrange
    mock_getenv.return_value = None
    variable = EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT

    # Act & Assert
    try:
        get_env_variable_or_throw(variable)
        assert False
    except ValueError as e:
        assert str(e) == f"Environment variable not found: {variable.name}"
    mock_getenv.assert_called_once_with(variable.name)
