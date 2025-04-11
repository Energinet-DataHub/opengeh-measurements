from importlib.resources import files


class _DescriptorFileNames:
    Brs021ForwardMeteredDataNotifyV1 = "Brs021ForwardMeteredDataNotifyV1.binpb"
    PersistSubmittedTransaction = "PersistSubmittedTransaction.binpb"
    VersionMessage = "VersionMessage.binpb"


def protobuf_path(protobuf_file_name: str) -> str:
    return f"core.contracts.process_manager.{protobuf_file_name}.generated"


class DescriptorFilePaths:
    Brs021ForwardMeteredDataNotifyV1 = str(
        files(protobuf_path("Brs021ForwardMeteredDataNotifyV1")).joinpath(
            _DescriptorFileNames.Brs021ForwardMeteredDataNotifyV1
        )
    )
    PersistSubmittedTransaction = str(
        files(protobuf_path("PersistSubmittedTransaction")).joinpath(_DescriptorFileNames.PersistSubmittedTransaction)
    )
    VersionMessage = str(files(protobuf_path("VersionMessage")).joinpath(_DescriptorFileNames.VersionMessage))
