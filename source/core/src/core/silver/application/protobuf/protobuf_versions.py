from core.silver.application.persist_submitted_transaction.persist_submitted_transaction_protobufV1 import (
    PersistSubmittedTransactionV1,
)
from core.silver.application.persist_submitted_transaction.persist_submitted_transaction_protobufV2 import (
    PersistSubmittedTransactionV2,
)

protobuf_messages = [PersistSubmittedTransactionV1, PersistSubmittedTransactionV2]


def get_versions() -> list[str]:
    collected_versions = []

    for protobuf_message in protobuf_messages:
        version = protobuf_message().version
        collected_versions.append(version)

    return collected_versions
