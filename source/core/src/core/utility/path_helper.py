from importlib.resources import files


def get_protobuf_descriptor_path(file_name: str) -> str:
    return str(files("core.contracts.process_manager.assets").joinpath(file_name))


def get_protobuf_path(file_name: str) -> str:
    return str(files("core.contracts").joinpath(file_name))
