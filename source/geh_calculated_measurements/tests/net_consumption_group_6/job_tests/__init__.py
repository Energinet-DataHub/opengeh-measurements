from tests import TESTS_ROOT

TEST_FILES_FOLDER_PATH = (TESTS_ROOT / "net_consumption_group_6" / "job_tests" / "test_files").as_posix()


def create_job_environment_variables() -> dict[str, str]:
    return {
        "CATALOG_NAME": "spark_catalog",
        "TIME_ZONE": "Europe/Copenhagen",
        "ELECTRICITY_MARKET_DATA_PATH": TEST_FILES_FOLDER_PATH,
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "some-connection-string",
    }
