import os


def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    """
    os.environ["CATALOG_NAME"] = "spark_catalog"
