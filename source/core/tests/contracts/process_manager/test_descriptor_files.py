import subprocess

import pytest


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


@pytest.mark.parametrize(
    "protobuf_file_name",
    [
        "Brs021ForwardMeteredDataNotifyV1",
        "PersistSubmittedTransaction",
        "VersionMessage",
    ],
)
def test__persist_submitted_transaction_protobuf(protobuf_file_name: str) -> None:
    process_manager_path = "src/core/contracts/process_manager"
    descriptor_file = f"{process_manager_path}/{protobuf_file_name}/{protobuf_file_name}.binpb"
    proto_file = f"{protobuf_file_name}.proto"
    proto_path = f"{process_manager_path}/{protobuf_file_name}"

    assert compile_proto_to_descriptor(proto_file, descriptor_file, proto_path), (
        "Protobuf compilation does not match the descriptor file!"
    )
