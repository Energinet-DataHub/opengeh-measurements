from pyspark.sql import DataFrame

import opengeh_bronze.application.config.spark_session as spark_session
import opengeh_bronze.infrastructure.streams.writer as writer
from opengeh_bronze.infrastructure.streams.bronze_repository import BronzeRepository


def stream() -> None:
    spark = spark_session.initialize_spark()
    bronze_stream = BronzeRepository(spark).read_measurements()
    writer.write_stream(bronze_stream, batch_operation_receipts)


def batch_operation_receipts(test: DataFrame, batch_id: int) -> None:
    # Udpak
    # Send kvittering
