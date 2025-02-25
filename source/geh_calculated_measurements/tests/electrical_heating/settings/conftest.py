import os


def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    Setting the environment variable is used for defining the name of the catalog.
    """
    os.environ["CATALOG_NAME"] = "spark_catalog"
