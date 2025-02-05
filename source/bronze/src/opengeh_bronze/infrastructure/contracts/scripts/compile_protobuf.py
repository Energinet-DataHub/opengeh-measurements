# !!!
# IMPORTANT: Before executing - Remove the existing .binpb files in the assets folder
# !!!

import subprocess


def compile_protobuf(proto_file, descriptor_file, proto_path):
    result = subprocess.run(
        [
            "protoc",
            "--include_imports",  # Include imports in the descriptor
            f"--proto_path={proto_path}",  # Set the directory containing .proto files
            f"--descriptor_set_out={descriptor_file}",
            proto_file,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"protoc compilation failed: {result.stderr}")


def compile_submitted_transaction_persisted() -> None:
    descriptor_file = "src/opengeh_bronze/infrastructure/contracts/assets/submitted_transaction_persisted.binpb"
    proto_path = "src/opengeh_bronze/infrastructure/contracts"
    proto_file = "SubmittedTransactionPersisted.proto"

    compile_protobuf(proto_file, descriptor_file, proto_path)


def compile_persist_submitted_transaction() -> None:
    descriptor_file = "src/opengeh_bronze/infrastructure/contracts/assets/persist_submitted_transaction.binpb"
    proto_path = "src/opengeh_bronze/infrastructure/contracts"
    proto_file = "PersistSubmittedTransaction.proto"

    compile_protobuf(proto_file, descriptor_file, proto_path)


compile_submitted_transaction_persisted()
compile_persist_submitted_transaction()
