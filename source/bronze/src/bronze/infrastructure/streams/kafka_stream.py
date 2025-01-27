from pyspark.sql import SparkSession

from bronze.domain.constants.database_names import DatabaseNames
from bronze.domain.constants.table_names import TableNames


def submit_transactions(spark: SparkSession, kafka_options: dict) -> None:
    spark.readStream.format("kafka").options(**kafka_options).load().writeTo(
        f"{DatabaseNames.bronze_database}.{TableNames.bronze_submitted_transactions_table}"
    )
