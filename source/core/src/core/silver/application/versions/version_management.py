import core.bronze.application.submitted_transactions.submitted_transactions_handler as submitted_transactions_handler
import core.silver.infrastructure.protobuf.persist_submitted_transaction as persist_submitted_transaction


class VersionManagement:
    class Version:
        def __init__(self, version: int, unpack, transformation):
            self.version = version
            self.unpack = unpack
            self.transformation = transformation

    versions = [
        Version(
            1, persist_submitted_transaction.unpack, submitted_transactions_handler.handle_valid_submitted_transactions
        )
    ]


class ProtobufManagement:
    class ProtobufMessage:
        def __init__(self, version: int, unpack, transformation):
            self.version = version
            self.unpack = unpack
            self.transformation = transformation

    protobuf_messages = [
        ProtobufMessage(
            1, persist_submitted_transaction.unpack, submitted_transactions_handler.handle_valid_submitted_transactions
        )
    ]
