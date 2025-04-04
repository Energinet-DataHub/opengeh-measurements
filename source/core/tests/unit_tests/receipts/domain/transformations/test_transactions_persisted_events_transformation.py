from pyspark.sql import SparkSession

import core.receipts.domain.transformations.transactions_persisted_events_transformation as sut
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.protobuf_helper as protobuf_helper
from core.contracts.process_manager.PersistSubmittedTransaction.persist_submitted_transaction_proto_version import (
    PersistSubmittedTransactionProtoVersion,
)
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder


def test__transform__should_return_expected_protobuf_event(spark: SparkSession) -> None:
    # Arrange
    orchestration_instance_id = identifier_helper.generate_random_string()
    silver_measurements = (
        SilverMeasurementsBuilder(spark).add_row(orchestration_instance_id=orchestration_instance_id).build()
    )

    # Act
    actual = sut.transform(silver_measurements)

    # Assert
    unpacked = protobuf_helper.unpack_brs021_forward_metered_data_notify_v1(actual)
    assert unpacked.count() == 1
    unpacked_value = unpacked.collect()[0]
    assert unpacked_value.version == PersistSubmittedTransactionProtoVersion.version_1
    assert unpacked_value.orchestration_instance_id == orchestration_instance_id
