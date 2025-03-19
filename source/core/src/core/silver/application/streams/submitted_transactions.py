from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import DataFrame

import core.bronze.domain.transformations.submitted_transactions_quarantined_transformations as submitted_transactions_quarantined_transformations
import core.silver.domain.transformations.measurements_transformation as measurements_transformation
import core.silver.domain.validations.submitted_transactions_to_silver_validation as submitted_transactions_to_silver_validation
import core.silver.infrastructure.config.spark_session as spark_session
import core.silver.infrastructure.protobuf.persist_submitted_transaction as persist_submitted_transaction
from core.bronze.infrastructure.repositories.invalid_submitted_transactions_repository import (
    InvalidSubmittedTransactionsRepository,
)
from core.bronze.infrastructure.repositories.submitted_transactions_quarantined_repository import (
    SubmittedTransactionsQuarantinedRepository,
)
from core.bronze.infrastructure.streams.bronze_repository import BronzeRepository
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository


def stream_submitted_transactions() -> None:
    spark = spark_session.initialize_spark()
    submitted_transactions = BronzeRepository(spark).read_submitted_transactions()
    SilverMeasurementsRepository().write_stream(
        submitted_transactions,
        GehCommonOrchestrationType.SUBMITTED,
        _batch_operation,
    )


def _batch_operation(submitted_transactions: DataFrame, batchId: int) -> None:
    (valid_submitted_transactions, invalid_submitted_transactions) = persist_submitted_transaction.unpack(
        submitted_transactions
    )

    _handle_valid_submitted_transactions(valid_submitted_transactions)
    _handle_invalid_submitted_transactions(invalid_submitted_transactions)


def _handle_valid_submitted_transactions(submitted_transactions: DataFrame) -> None:
    spark = spark_session.initialize_spark()
    measurements = measurements_transformation.transform(spark, submitted_transactions)

    (valid_measurements, invalid_measurements) = submitted_transactions_to_silver_validation.validate(measurements)

    SilverMeasurementsRepository().append_if_not_exists(valid_measurements)

    _handle_submitted_transactions_quarantined(invalid_measurements)


def _handle_submitted_transactions_quarantined(invalid_measurements):
    submitted_transactions_quarantined = submitted_transactions_quarantined_transformations.map_silver_measurements_to_submitted_transactions_quarantined(
        invalid_measurements
    )

    SubmittedTransactionsQuarantinedRepository().append(submitted_transactions_quarantined)


def _handle_invalid_submitted_transactions(invalid_submitted_transactions: DataFrame) -> None:
    InvalidSubmittedTransactionsRepository().append(invalid_submitted_transactions)
