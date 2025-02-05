from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from opengeh_bronze.domain.constants.column_names.bronze_migrated_column_names import BronzeMigratedColumnNames, BronzeMigratedValuesFieldNames

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
        StructField(BronzeMigratedColumnNames.values, StringType(), False),
        StructField(BronzeMigratedColumnNames.valid_from_date, TimestampType(), False),
        StructField(BronzeMigratedColumnNames.valid_to_date, TimestampType(), False),
        StructField(BronzeMigratedColumnNames.values, ArrayType(StructType(
            StructField(BronzeMigratedValuesFieldNames.position, IntegerType(), True),
            StructField(BronzeMigratedValuesFieldNames.quality, StringType(), True),
            StructField(BronzeMigratedValuesFieldNames.quantity, DecimalType(18, 6), True)
            )), True),
        StructField(BronzeMigratedColumnNames.created, TimestampType(), False),
    ]
)
