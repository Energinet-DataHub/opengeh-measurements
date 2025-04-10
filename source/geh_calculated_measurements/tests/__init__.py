from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
TESTS_ROOT = PROJECT_ROOT / "tests"

SPARK_CATALOG_NAME = "spark_catalog"
MEASUREMENTS_GOLD_TABLE_NAME = "measurements"
TIME_ZONE = "Europe/Copenhagen"


def create_job_environment_variables(eletricity_market_path: str = "some_path") -> dict:
    return {
        "CATALOG_NAME": SPARK_CATALOG_NAME,
        "TIME_ZONE": TIME_ZONE,
        "ELECTRICITY_MARKET_DATA_PATH": eletricity_market_path,
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "some_connection_string",
    }
