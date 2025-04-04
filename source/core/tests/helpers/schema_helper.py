from pyspark.sql import SparkSession

from core.migrations import MigrationDatabaseNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.calculated_settings import CalculatedSettings
from core.settings.core_internal_settings import CoreInternalSettings
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings
from tests.helpers.sql_scripts.create_calculated_measurements_v1 import create_calculated_measurements_v1


def create_internal_schemas(spark: SparkSession) -> None:
    bronze_settings = BronzeSettings()
    silver_settings = SilverSettings()
    gold_settings = GoldSettings()
    core_internal_settings = CoreInternalSettings()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {MigrationDatabaseNames.measurements_internal_database}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_settings.bronze_database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_settings.silver_database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_settings.gold_database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {core_internal_settings.core_internal_database_name}")


def create_external_schemas(spark: SparkSession) -> None:
    calculated_settings = CalculatedSettings()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {calculated_settings.calculated_database_name}")


def create_external_tables(spark: SparkSession) -> None:
    spark.sql(create_calculated_measurements_v1)
