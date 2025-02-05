from importlib.resources import files


def get_protobuf_descriptor_path(file_name: str) -> str:
    return str(files("opengeh_bronze.infrastructure.contracts.assets").joinpath(file_name))


def get_protobuf_path(file_name: str) -> str:
    return str(files("opengeh_bronze.infrastructure.contracts").joinpath(file_name))


def get_storage_base_path(
    storage_account_name: str,
    container_name: str,
) -> str:
    return f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"


def get_checkpoint_path(
    datalake_storage_account_name: str,
    container_name: str,
    table_name: str,
) -> str:
    return get_storage_base_path(datalake_storage_account_name, container_name) + f"checkpoints/{table_name}"
