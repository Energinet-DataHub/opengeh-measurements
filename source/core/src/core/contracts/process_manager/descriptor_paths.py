from importlib.resources import files


class _DescriptorFileNames:
    Brs021ForwardMeteredDataNotifyV1 = "Brs021ForwardMeteredDataNotifyV1.binpb"
    PersistSubmittedTransactionV1 = "PersistSubmittedTransactionV1.binpb"
    PersistSubmittedTransactionV2 = "PersistSubmittedTransactionV2.binpb"
    VersionMessage = "VersionMessage.binpb"


def protobuf_path(protobuf_file_name: str) -> str:
    return f"core.contracts.process_manager.{protobuf_file_name}.generated"


class DescriptorFilePaths:
    Brs021ForwardMeteredDataNotifyV1 = str(
        files(protobuf_path("Brs021ForwardMeteredDataNotifyV1")).joinpath(
            _DescriptorFileNames.Brs021ForwardMeteredDataNotifyV1
        )
    )
    PersistSubmittedTransactionV1 = str(
        files(protobuf_path("PersistSubmittedTransactionV1")).joinpath(
            _DescriptorFileNames.PersistSubmittedTransactionV1
        )
    )
    PersistSubmittedTransactionV2 = str(
        files(protobuf_path("PersistSubmittedTransactionV2")).joinpath(
            _DescriptorFileNames.PersistSubmittedTransactionV2
        )
    )
    VersionMessage = str(files(protobuf_path("VersionMessage")).joinpath(_DescriptorFileNames.VersionMessage))
