from pyspark.sql.types import ArrayType, DecimalType, IntegerType, StringType, StructField, StructType, TimestampType

from core.bronze.domain.constants.column_names.bronze_migrated_column_names import (
    BronzeMigratedColumnNames,
    BronzeMigratedValuesFieldNames,
)
from core.bronze.domain.constants.column_names.migrations_silver_time_series_column_names import (
    MigrationsSilverTimeSeriesColumnNames,
    MigrationsSilverTimeSeriesValuesFieldNames,
)

migrated_schema = StructType(
    [
        StructField(BronzeMigratedColumnNames.metering_point_id, StringType(), False),
        StructField(BronzeMigratedColumnNames.type_of_mp, StringType(), False),
        StructField(BronzeMigratedColumnNames.historical_flag, StringType(), False),
        StructField(BronzeMigratedColumnNames.resolution, StringType(), False),
        StructField(BronzeMigratedColumnNames.transaction_id, StringType(), True),
        StructField(BronzeMigratedColumnNames.transaction_insert_date, TimestampType(), False),
        StructField(BronzeMigratedColumnNames.unit, StringType(), False),
        StructField(BronzeMigratedColumnNames.status, IntegerType(), False),
        StructField(BronzeMigratedColumnNames.read_reason, StringType(), False),
        StructField(BronzeMigratedColumnNames.valid_from_date, TimestampType(), False),
        StructField(BronzeMigratedColumnNames.valid_to_date, TimestampType(), False),
        StructField(
            BronzeMigratedColumnNames.values,
            ArrayType(
                StructType(
                    [
                        StructField(BronzeMigratedValuesFieldNames.position, IntegerType(), True),
                        StructField(BronzeMigratedValuesFieldNames.quality, StringType(), True),
                        StructField(BronzeMigratedValuesFieldNames.quantity, DecimalType(18, 6), True),
                    ]
                )
            ),
            False,
        ),
        StructField(BronzeMigratedColumnNames.created_in_migrations, TimestampType(), False),
        StructField(BronzeMigratedColumnNames.created_in_measurements, TimestampType(), False),
    ]
)

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
