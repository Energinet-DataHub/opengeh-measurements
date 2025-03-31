from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
TESTS_ROOT = PROJECT_ROOT / "tests"


def create_job_environment_variables(eletricity_market_path: str = "some_path") -> dict:
    return {
        "CATALOG_NAME": "spark_catalog",
        "TIME_ZONE": "Europe/Copenhagen",
        "ELECTRICITY_MARKET_DATA_PATH": eletricity_market_path,
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "some_connection_string",
    }
