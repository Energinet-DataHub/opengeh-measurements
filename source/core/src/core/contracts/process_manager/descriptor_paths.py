from importlib.resources import files


class _DescriptorFileNames:
    Brs021ForwardMeteredDataNotifyV1 = "Brs021ForwardMeteredDataNotifyV1.binpb"
    PersistSubmittedTransaction = "PersistSubmittedTransaction.binpb"


class DescriptorFilePaths:
    Brs021ForwardMeteredDataNotifyV1 = str(
        files("core.contracts.process_manager.Brs021ForwardMeteredDataNotify").joinpath(
            _DescriptorFileNames.Brs021ForwardMeteredDataNotifyV1
        )
    )
    PersistSubmittedTransaction = str(
        files("core.contracts.process_manager.PersistSubmittedTransaction").joinpath(
            _DescriptorFileNames.PersistSubmittedTransaction
        )
    )
