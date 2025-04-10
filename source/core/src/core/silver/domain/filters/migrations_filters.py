from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from core.bronze.domain.constants.column_names.bronze_migrated_transactions_column_names import (
    BronzeMigratedTransactionsColumnNames,
)


def filter_away_rows_older_than_2017(migrated_transactions: DataFrame) -> DataFrame:
    return migrated_transactions.filter(
        col(BronzeMigratedTransactionsColumnNames.valid_to_date) > "2016-12-31 23:00:00"
    )
