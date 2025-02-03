from pyspark.sql import DataFrame, SparkSession

import opengeh_bronze.infrastructure.shared_helpers as shared_helpers
from opengeh_bronze.infrastructure.config.storage_container_names import StorageContainerNames
from opengeh_bronze.infrastructure.settings.storage_account_settings import (
    StorageAccountSettings,
)
from opengeh_bronze.infrastructure.settings.submitted_transactions_stream_settings import (
    SubmittedTransactionsStreamSettings,
)


class EventHubStream:
    eventhub_options: dict

    def __init__(self, spark: SparkSession) -> None:
        self.eventhub_options = SubmittedTransactionsStreamSettings().create_eventhub_options(spark)

    def write_stream(
        self,
        dataframe: DataFrame,
    ):
        checkpoint_location = shared_helpers.get_checkpoint_path(
            StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT, StorageContainerNames.bronze, "processed_transactions"
        )

        dataframe.writeStream.format("eventhubs").options(**self.eventhub_options).option(
            "checkpointLocation", checkpoint_location
        ).trigger(availableNow=True).start()
