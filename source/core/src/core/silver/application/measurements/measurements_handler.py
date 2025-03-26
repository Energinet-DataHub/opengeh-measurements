from pyspark.sql import DataFrame

import core.bronze.domain.transformations.submitted_transactions_quarantined_transformations as submitted_transactions_quarantined_transformations
import core.silver.domain.validations.submitted_transactions_to_silver_validation as submitted_transactions_to_silver_validation
from core.bronze.infrastructure.repositories.submitted_transactions_quarantined_repository import (
    SubmittedTransactionsQuarantinedRepository,
)
from core.silver.domain.protobuf.protobuf import ProtoDeserializerBase
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository


def handle(submitted_transactions: DataFrame, protobuf_message: ProtoDeserializerBase) -> None:
    measurements = protobuf_message.transform(submitted_transactions)

    (valid_measurements, invalid_measurements) = submitted_transactions_to_silver_validation.validate(measurements)

    SilverMeasurementsRepository().append_if_not_exists(valid_measurements)

    _persist_submitted_transactions_quarantined(invalid_measurements)


def _persist_submitted_transactions_quarantined(invalid_measurements):
    submitted_transactions_quarantined = submitted_transactions_quarantined_transformations.map_silver_measurements_to_submitted_transactions_quarantined(
        invalid_measurements
    )

    SubmittedTransactionsQuarantinedRepository().append(submitted_transactions_quarantined)
