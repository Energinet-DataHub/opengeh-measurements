from core.silver.application.persist_submitted_transaction.persist_submitted_transaction_protobuf import (
    PersistSubmittedTransaction,
)

protobuf_messages = [PersistSubmittedTransaction]


def get_versions() -> list[str]:
    collected_versions = []

    for protobuf_message in protobuf_messages:
        version = protobuf_message().version
        collected_versions.append(version)

    return collected_versions
