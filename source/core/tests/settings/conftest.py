import os


def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    """
    os.environ["CATALOG_NAME"] = "spark_catalog"
    os.environ["BRONZE_DATABASE_NAME"] = "measurements_bronze"
    os.environ["SILVER_DATABASE_NAME"] = "measurements_silver"
    os.environ["GOLD_DATABASE_NAME"] = "measurements_gold"
    os.environ["CORE_INTERNAL_DATABASE_NAME"] = "measurements_core_internal"
