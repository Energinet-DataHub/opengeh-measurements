from pytest_mock import MockFixture

from core.utility.environment_variable_helper import EnvironmentVariable, get_env_variable_or_throw


def test__get_env_variable_or_throw_found__should_return_expected(mocker: MockFixture):
    # Arrange
    mock_getenv = mocker.patch("os.getenv", return_value="testvalue")
    variable = EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT

    # Act
    result = get_env_variable_or_throw(variable)

    # Assert
    assert result == "testvalue"
    mock_getenv.assert_called_once_with(variable.name)


def test__get_env_variable_or_throw_not_found__should_throw_exception(mocker: MockFixture):
    # Arrange
    mock_getenv = mocker.patch("os.getenv", return_value=None)
    variable = EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT

    # Act & Assert
    try:
        get_env_variable_or_throw(variable)
        assert False
    except ValueError as e:
        assert str(e) == f"Environment variable not found: {variable.name}"
    mock_getenv.assert_called_once_with(variable.name)
