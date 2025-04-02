import os

from tests import SPARK_CATALOG_NAME


# TODO BJM: Should we remove this?
def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    Setting the environment variable is used for defining the name of the catalog.
    """
    os.environ["CATALOG_NAME"] = SPARK_CATALOG_NAME
