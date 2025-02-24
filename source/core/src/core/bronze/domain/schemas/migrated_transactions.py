from pyspark.sql.types import ArrayType, DecimalType, IntegerType, StringType, StructField, StructType, TimestampType

from core.bronze.domain.constants.column_names.bronze_migrated_transactions_column_names import (
    BronzeMigratedTransactionsColumnNames,
    BronzeMigratedTransactionsValuesFieldNames,
)
from core.bronze.domain.constants.column_names.migrations_silver_time_series_column_names import (
    MigrationsSilverTimeSeriesColumnNames,
    MigrationsSilverTimeSeriesValuesFieldNames,
)

migrated_transactions_schema = StructType(
    [
        StructField(BronzeMigratedTransactionsColumnNames.metering_point_id, StringType(), False),
        StructField(BronzeMigratedTransactionsColumnNames.type_of_mp, StringType(), False),
        StructField(BronzeMigratedTransactionsColumnNames.historical_flag, StringType(), False),
        StructField(BronzeMigratedTransactionsColumnNames.resolution, StringType(), False),
        StructField(BronzeMigratedTransactionsColumnNames.transaction_id, StringType(), True),
        StructField(BronzeMigratedTransactionsColumnNames.transaction_insert_date, TimestampType(), False),
        StructField(BronzeMigratedTransactionsColumnNames.unit, StringType(), False),
        StructField(BronzeMigratedTransactionsColumnNames.status, IntegerType(), False),
        StructField(BronzeMigratedTransactionsColumnNames.read_reason, StringType(), False),
        StructField(BronzeMigratedTransactionsColumnNames.valid_from_date, TimestampType(), False),
        StructField(BronzeMigratedTransactionsColumnNames.valid_to_date, TimestampType(), False),
        StructField(
            BronzeMigratedTransactionsColumnNames.values,
            ArrayType(
                StructType(
                    [
                        StructField(BronzeMigratedTransactionsValuesFieldNames.position, IntegerType(), True),
                        StructField(BronzeMigratedTransactionsValuesFieldNames.quality, StringType(), True),
                        StructField(BronzeMigratedTransactionsValuesFieldNames.quantity, DecimalType(18, 6), True),
                    ]
                )
            ),
            False,
        ),
        StructField(BronzeMigratedTransactionsColumnNames.created_in_migrations, TimestampType(), False),
        StructField(BronzeMigratedTransactionsColumnNames.created, TimestampType(), False),
    ]
)

# The corresponding table in migrations_silver database.
# Belongs to another domain, but the schema is needed here.
migrations_silver_time_series_schema = StructType(
    [
        StructField(MigrationsSilverTimeSeriesColumnNames.metering_point_id, StringType(), False),
        StructField(MigrationsSilverTimeSeriesColumnNames.type_of_mp, StringType(), False),
        StructField(MigrationsSilverTimeSeriesColumnNames.historical_flag, StringType(), False),
        StructField(MigrationsSilverTimeSeriesColumnNames.resolution, StringType(), False),
        StructField(MigrationsSilverTimeSeriesColumnNames.transaction_id, StringType(), True),
        StructField(MigrationsSilverTimeSeriesColumnNames.transaction_insert_date, TimestampType(), False),
        StructField(MigrationsSilverTimeSeriesColumnNames.unit, StringType(), False),
        StructField(MigrationsSilverTimeSeriesColumnNames.status, IntegerType(), False),
        StructField(MigrationsSilverTimeSeriesColumnNames.read_reason, StringType(), False),
        StructField(MigrationsSilverTimeSeriesColumnNames.valid_from_date, TimestampType(), False),
        StructField(MigrationsSilverTimeSeriesColumnNames.valid_to_date, TimestampType(), False),
        StructField(
            MigrationsSilverTimeSeriesColumnNames.values,
            ArrayType(
                StructType(
                    [
                        StructField(MigrationsSilverTimeSeriesValuesFieldNames.position, IntegerType(), True),
                        StructField(MigrationsSilverTimeSeriesValuesFieldNames.quality, StringType(), True),
                        StructField(MigrationsSilverTimeSeriesValuesFieldNames.quantity, DecimalType(18, 6), True),
                    ]
                )
            ),
            False,
        ),
        StructField(MigrationsSilverTimeSeriesColumnNames.partitioning_col, TimestampType(), False),
        StructField(MigrationsSilverTimeSeriesColumnNames.created, TimestampType(), False),
    ]
)
