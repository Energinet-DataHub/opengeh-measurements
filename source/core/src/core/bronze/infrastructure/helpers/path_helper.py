from importlib.resources import files


def get_protobuf_descriptor_path(file_name: str) -> str:
    return str(files("core.bronze.infrastructure.contracts.assets").joinpath(file_name))


def get_protobuf_path(file_name: str) -> str:
    return str(files("core.bronze.infrastructure.contracts").joinpath(file_name))
