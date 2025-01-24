from pyspark import SparkConf
from pyspark.sql.session import SparkSession

from bronze.application.settings.submitted_transactions_stream_settings import SubmittedTransactionsStreamSettings


def submit_transactions() -> None:
    kafka_options = SubmittedTransactionsStreamSettings().create_kafka_options()

    spark = initialize_spark()

    spark.readStream.format("kafka").options(**kafka_options).load().writeStream.format("delta").option(
        "checkpointLocation", "checkpointReceiving"
    ).toTable("submitted_transactions")


def initialize_spark() -> SparkSession:
    spark_conf = SparkConf(loadDefaults=True).set("spark.sql.session.timeZone", "UTC")
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()
