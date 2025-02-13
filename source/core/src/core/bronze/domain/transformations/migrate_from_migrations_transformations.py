from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

from core.bronze.domain.constants.column_names.bronze_migrated_column_names import BronzeMigratedColumnNames
from core.bronze.domain.constants.column_names.migrations_silver_time_series_column_names import (
    MigrationsSilverTimeSeriesColumnNames,
)


def map_migrations_to_measurements(migrations_data: DataFrame):
    return (
        migrations_data.drop("partitioning_col")
        .withColumnRenamed(
            MigrationsSilverTimeSeriesColumnNames.created,
            BronzeMigratedColumnNames.created_in_migrations,
        )
        .withColumn(BronzeMigratedColumnNames.created_in_measurements, current_timestamp())
    )
