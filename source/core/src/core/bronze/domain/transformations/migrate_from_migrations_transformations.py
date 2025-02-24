from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

from core.bronze.domain.constants.column_names.bronze_migrated_transactions_column_names import (
    BronzeMigratedTransactionsColumnNames,
)
from core.bronze.domain.constants.column_names.migrations_silver_time_series_column_names import (
    MigrationsSilverTimeSeriesColumnNames,
)


def map_migrations_to_measurements(migrations_data: DataFrame) -> DataFrame:
    return migrations_data.select(
        col(MigrationsSilverTimeSeriesColumnNames.metering_point_id).alias(
            BronzeMigratedTransactionsColumnNames.metering_point_id
        ),
        col(MigrationsSilverTimeSeriesColumnNames.type_of_mp).alias(BronzeMigratedTransactionsColumnNames.type_of_mp),
        col(MigrationsSilverTimeSeriesColumnNames.historical_flag).alias(
            BronzeMigratedTransactionsColumnNames.historical_flag
        ),
        col(MigrationsSilverTimeSeriesColumnNames.resolution).alias(BronzeMigratedTransactionsColumnNames.resolution),
        col(MigrationsSilverTimeSeriesColumnNames.transaction_id).alias(
            BronzeMigratedTransactionsColumnNames.transaction_id
        ),
        col(MigrationsSilverTimeSeriesColumnNames.transaction_insert_date).alias(
            BronzeMigratedTransactionsColumnNames.transaction_insert_date
        ),
        col(MigrationsSilverTimeSeriesColumnNames.unit).alias(BronzeMigratedTransactionsColumnNames.unit),
        col(MigrationsSilverTimeSeriesColumnNames.status).alias(BronzeMigratedTransactionsColumnNames.status),
        col(MigrationsSilverTimeSeriesColumnNames.read_reason).alias(BronzeMigratedTransactionsColumnNames.read_reason),
        col(MigrationsSilverTimeSeriesColumnNames.valid_from_date).alias(
            BronzeMigratedTransactionsColumnNames.valid_from_date
        ),
        col(MigrationsSilverTimeSeriesColumnNames.valid_to_date).alias(
            BronzeMigratedTransactionsColumnNames.valid_to_date
        ),
        col(MigrationsSilverTimeSeriesColumnNames.values).alias(BronzeMigratedTransactionsColumnNames.values),
        col(MigrationsSilverTimeSeriesColumnNames.created).alias(
            BronzeMigratedTransactionsColumnNames.created_in_migrations
        ),
        current_timestamp().alias(BronzeMigratedTransactionsColumnNames.created),
    )
