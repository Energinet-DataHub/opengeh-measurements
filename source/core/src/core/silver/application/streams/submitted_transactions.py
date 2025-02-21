from pyspark.sql import DataFrame

import core.bronze.domain.transformations.submitted_transactions_transformation as submitted_transactions_transformation
import core.silver.application.config.spark_session as spark_session
import core.silver.domain.transformations.measurements_transformation as measurements_transformation
from core.bronze.infrastructure.streams.bronze_repository import BronzeRepository
from core.silver.infrastructure.streams.silver_repository import SilverRepository


def stream_submitted_transactions() -> None:
    spark = spark_session.initialize_spark()
    submitted_transactions = BronzeRepository(spark).read_submitted_transactions()
    SilverRepository().write_stream(submitted_transactions, _batch_operation)


def _batch_operation(submitted_transactions: DataFrame, batchId: int) -> None:
    spark = spark_session.initialize_spark()
    unpacked_submitted_transactions = submitted_transactions_transformation.create_by_packed_submitted_transactions(
        submitted_transactions
    )
    measurements = measurements_transformation.create_by_unpacked_submitted_transactions(
        spark, unpacked_submitted_transactions
    )
    SilverRepository().append(measurements)
