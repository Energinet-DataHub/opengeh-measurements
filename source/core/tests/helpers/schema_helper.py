from pyspark.sql import SparkSession

from core.migrations import MigrationDatabaseNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings
from core.settings.core_internal_settings import CoreInternalSettings


def create_schemas(spark: SparkSession) -> None:
    bronze_settings = BronzeSettings()
    silver_settings = SilverSettings()
    gold_settings = GoldSettings()
    core_internal_settings = CoreInternalSettings()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {MigrationDatabaseNames.measurements_internal_database}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_settings.bronze_database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_settings.silver_database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_settings.gold_database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {core_internal_settings.core_internal_database_name}")
