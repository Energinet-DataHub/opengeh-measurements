from pyspark.sql import DataFrame

import core.bronze.domain.transformations.submitted_transactions_quarantined_transformations as submitted_transactions_quarantined_transformations
import core.silver.domain.transformations.persist_submitted_transaction_transformation as persist_submitted_transaction_transformation
import core.silver.domain.validations.submitted_transactions_to_silver_validation as submitted_transactions_to_silver_validation
import core.silver.infrastructure.config.spark_session as spark_session
from core.bronze.infrastructure.repositories.invalid_submitted_transactions_repository import (
    InvalidSubmittedTransactionsRepository,
)
from core.bronze.infrastructure.repositories.submitted_transactions_quarantined_repository import (
    SubmittedTransactionsQuarantinedRepository,
)
from core.silver.application.versions.protobuf_versions import ProtobufVersions
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository


def handle_unknown_submitted_transaction(submitted_transactions: DataFrame) -> None:
    collected_versions = [protobuf_message.version for protobuf_message in ProtobufVersions().protobuf_messages]
    unknown_protobuf_messages = submitted_transactions.filter(f"version not in {collected_versions}")
    persist_invalid_submitted_transactions(unknown_protobuf_messages)


def handle_valid_submitted_transactions(submitted_transactions: DataFrame) -> None:
    spark = spark_session.initialize_spark()
    measurements = persist_submitted_transaction_transformation.transform(spark, submitted_transactions)

    (valid_measurements, invalid_measurements) = submitted_transactions_to_silver_validation.validate(measurements)

    SilverMeasurementsRepository().append_if_not_exists(valid_measurements)

    _persist_submitted_transactions_quarantined(invalid_measurements)


def persist_invalid_submitted_transactions(invalid_submitted_transactions: DataFrame) -> None:
    InvalidSubmittedTransactionsRepository().append(invalid_submitted_transactions)


def _persist_submitted_transactions_quarantined(invalid_measurements):
    submitted_transactions_quarantined = submitted_transactions_quarantined_transformations.map_silver_measurements_to_submitted_transactions_quarantined(
        invalid_measurements
    )

    SubmittedTransactionsQuarantinedRepository().append(submitted_transactions_quarantined)
