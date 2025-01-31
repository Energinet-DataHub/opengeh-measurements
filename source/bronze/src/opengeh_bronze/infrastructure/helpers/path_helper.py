from importlib.abc import Traversable
from importlib.resources import files


def get_protobuf_descriptor_path(file_name: str) -> Traversable:
    return files("opengeh_bronze.infrastructure.contracts.assets").joinpath(file_name)
