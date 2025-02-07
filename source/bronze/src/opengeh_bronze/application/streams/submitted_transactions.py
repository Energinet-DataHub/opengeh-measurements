import opengeh_bronze.application.config.spark_session as spark_session
import opengeh_bronze.infrastructure.streams.kafka_stream as kafka_stream


def submit_transactions() -> None:
    spark = spark_session.initialize_spark()

    kafka_stream.submit_transactions(spark)
