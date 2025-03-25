from core.silver.application.persist_submitted_transaction.persist_submitted_transaction_protobuf import (
    PersistSubmittedTransaction,
)


class ProtobufVersions:
    protobuf_messages = [PersistSubmittedTransaction]

    def get_versions(self) -> list[str]:
        collected_versions = []

        for protobuf_message in self.protobuf_messages:
            version = protobuf_message().version
            collected_versions.append(version)

        return collected_versions
