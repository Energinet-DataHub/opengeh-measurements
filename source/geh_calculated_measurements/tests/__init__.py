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


def ensure_databases_created(spark: SparkSession) -> None:
    """Databases are created in dh3infrastructure using terraform
    So we need to create them in test environment"""
    print("##############################################################################")
    dbs = [
        MeasurementsCalculatedInternalDatabaseDefinition.measurements_calculated_internal_database,
        CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME,
    ]
    print(dbs)
    for db in dbs:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    catalog = "spark_catalog"
    schemas = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
    for schema_row in schemas:
        schema_name = schema_row["namespace"]
        print(f"Schema: {schema_name}")

        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema_name}").collect()
        for table_row in tables:
            print(f"  Table/View: {table_row['tableName']}")
