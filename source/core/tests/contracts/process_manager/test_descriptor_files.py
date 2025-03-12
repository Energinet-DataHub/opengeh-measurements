import subprocess


def compile_proto_to_descriptor(proto_file, descriptor_file, proto_path="."):
    """Compiles a .proto file into a descriptor file with correct proto_path."""
    temp_descriptor = "temp_descriptor.binpb"

    result = subprocess.run(
        [
            "protoc",
            "--include_imports",  # Include imports in the descriptor
            f"--proto_path={proto_path}",  # Set the directory containing .proto files
            f"--descriptor_set_out={temp_descriptor}",
            proto_file,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"protoc compilation failed: {result.stderr}")

    # Compare generated descriptor with the reference descriptor
    with open(temp_descriptor, "rb") as temp, open(descriptor_file, "rb") as ref:
        if temp.read() == ref.read():
            return True
        else:
            return False


def test__persist_submitted_transaction_protobuf() -> None:
    descriptor_file = "src/core/contracts/process_manager/assets/persist_submitted_transaction.binpb"
    proto_path = "src/core/contracts/process_manager"
    proto_file = "PersistSubmittedTransaction.proto"

    assert compile_proto_to_descriptor(proto_file, descriptor_file, proto_path), (
        "Protobuf compilation does not match the descriptor file!"
    )


def test__brs021_forward_metered_data_notify_v1_protobuf() -> None:
    descriptor_file = "src/core/contracts/process_manager/assets/brs021_forward_metered_data_notify_v1.binpb"
    proto_path = "src/core/contracts/process_manager"
    proto_file = "Brs021ForwardMeteredDataNotifyV1.proto"

    assert compile_proto_to_descriptor(proto_file, descriptor_file, proto_path), (
        "Protobuf compilation does not match the descriptor file!"
    )
