from pathlib import Path

from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from geh_calculated_measurements.database_migrations import MeasurementsCalculatedInternalDatabaseDefinition

PROJECT_ROOT = Path(__file__).parent.parent
TESTS_ROOT = PROJECT_ROOT / "tests"


def create_job_environment_variables(eletricity_market_path: str = "some_path") -> dict:
    return {
        "CATALOG_NAME": "spark_catalog",
        "TIME_ZONE": "Europe/Copenhagen",
        "ELECTRICITY_MARKET_DATA_PATH": eletricity_market_path,
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "some_connection_string",
    }


_CALCULATED_MEASUREMENTS_DATABASE_NAMES = [
    MeasurementsCalculatedInternalDatabaseDefinition.measurements_calculated_internal_database,
    CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME,
]


def ensure_calculated_measurements_databases_exist(spark: SparkSession) -> None:
    """Databases are created in dh3infrastructure using terraform
    So we need to create them in test environment"""
    for db in _CALCULATED_MEASUREMENTS_DATABASE_NAMES:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")


def drop_calculated_measurements_databases(spark: SparkSession) -> None:
    for db in _CALCULATED_MEASUREMENTS_DATABASE_NAMES:
        spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
