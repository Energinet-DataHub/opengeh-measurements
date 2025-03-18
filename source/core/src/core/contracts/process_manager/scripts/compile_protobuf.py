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


def compile_brs_021_forward_metered_data_notify_v1() -> None:
    descriptor_file = "src/core/contracts/process_manager/assets/brs021_forward_metered_data_notify_v1.binpb"
    proto_path = "src/core/contracts/process_manager"
    proto_file = "Brs021ForwardMeteredDataNotifyV1.proto"

    compile_protobuf(proto_file, descriptor_file, proto_path)


def compile_persist_submitted_transaction() -> None:
    descriptor_file = "src/core/contracts/process_manager/assets/persist_submitted_transaction.binpb"
    proto_path = "src/core/contracts/process_manager"
    proto_file = "PersistSubmittedTransaction.proto"

    compile_protobuf(proto_file, descriptor_file, proto_path)


compile_brs_021_forward_metered_data_notify_v1()
compile_persist_submitted_transaction()
