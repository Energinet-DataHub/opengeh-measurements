from pyspark.sql import DataFrame

import bronze.application.config.spark_sesion as spark_session
import bronze.infrastructure.streams.writer as writer
from bronze.infrastructure.config.table_names import TableNames
from bronze.infrastructure.streams.bronze_repository import BronzeRepository


def stream() -> None:
    spark = spark_session.initialize_spark()
    bronze_stream = BronzeRepository(spark).read_measurements()
    writer.write_stream(bronze_stream, batch_operation_receipts)


def batch_operation_receipts(test: DataFrame, batch_id: int) -> None:
    # Udpak
    # Send kvittering
