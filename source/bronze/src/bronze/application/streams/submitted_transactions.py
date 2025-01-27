from pyspark import SparkConf
from pyspark.sql.session import SparkSession

from bronze.application.settings.submitted_transactions_stream_settings import SubmittedTransactionsStreamSettings
from bronze.domain.constants.database_names import DatabaseNames
from bronze.domain.constants.table_names import TableNames


def submit_transactions() -> None:
    kafka_options = SubmittedTransactionsStreamSettings().create_kafka_options()

    spark = initialize_spark()

    spark.readStream.format("kafka").options(**kafka_options).load().writeTo(
        f"{DatabaseNames.bronze_database}.{TableNames.bronze_submitted_transactions_table}"
    )


def initialize_spark() -> SparkSession:
    spark_conf = SparkConf(loadDefaults=True).set("spark.sql.session.timeZone", "UTC")
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()
