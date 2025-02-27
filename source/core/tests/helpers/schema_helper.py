from pyspark.sql import SparkSession

from core.migrations import MigrationDatabaseNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings


def create_schemas(spark: SparkSession) -> None:
    bronze_settings = BronzeSettings()
    silver_settings = SilverSettings()
    gold_settings = GoldSettings()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {MigrationDatabaseNames.measurements_internal_database}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_settings.bronze_database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_settings.silver_database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_settings.gold_database_name}")
