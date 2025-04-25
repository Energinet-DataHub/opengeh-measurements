# !!!
# IMPORTANT: Before executing - Remove the existing .binpb files in the assets folder
# !!!

import os
import subprocess

process_manager_path = "src/core/contracts/process_manager"


def compile_protobuf(proto_file, descriptor_file, proto_path):
    os.makedirs(f"{proto_path}/generated", exist_ok=True)

    result = subprocess.run(
        [
            "protoc",
            "--include_imports",  # Include imports in the descriptor
            f"--proto_path={proto_path}",  # Set the directory containing .proto files
            f"--descriptor_set_out={descriptor_file}",
            f"--python_out={proto_path}/generated",  # Output directory for generated Python files
            f"--pyi_out={proto_path}/generated",  # Output directory for generated Python files
            proto_file,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"protoc compilation failed: {result.stderr}")


def compile_protobuf_file(protobuf_file_name) -> None:
    descriptor_file = f"{process_manager_path}/{protobuf_file_name}/generated/{protobuf_file_name}.binpb"
    proto_file = f"{protobuf_file_name}.proto"
    proto_path = f"{process_manager_path}/{protobuf_file_name}"

    compile_protobuf(proto_file, descriptor_file, proto_path)


compile_protobuf_file("Brs021ForwardMeteredDataNotifyV1")
compile_protobuf_file("PersistSubmittedTransaction")
compile_protobuf_file("VersionMessage")
