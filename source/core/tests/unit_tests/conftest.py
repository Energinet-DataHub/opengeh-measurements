import tests.helpers.environment_variables_helpers as environment_variables_helpers


def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    """
    environment_variables_helpers.set_test_environment_variables()
