from pyspark.sql import DataFrame, SparkSession

import opengeh_bronze.infrastructure.shared_helpers as shared_helpers
from opengeh_bronze.application.settings import KafkaAuthenticationSettings, SubmittedTransactionsStreamSettings
from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames
from opengeh_bronze.infrastructure.config.storage_container_names import StorageContainerNames
from opengeh_bronze.infrastructure.settings.storage_account_settings import (
    StorageAccountSettings,
)
from opengeh_bronze.infrastructure.settings.submitted_transactions_stream_settings import (
    SubmittedTransactionsStreamSettings,
)


class KafkaStream:
    kafka_options: dict

    def __init__(self) -> None:
        self.kafka_options = SubmittedTransactionsStreamSettings().create_kafka_options()  # type: ignore
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT  # type: ignore

    def submit_transactions(self, spark: SparkSession) -> None:
        checkpoint_location = shared_helpers.get_checkpoint_path(
            self.data_lake_settings, StorageContainerNames.bronze, "submitted_transactions"
        )
        write_stream = spark.readStream.format("kafka").options(**self.kafka_options).load().writeStream.outputMode("append").option("checkpointLocation", checkpoint_location)

        stream_settings = SubmittedTransactionsStreamSettings()  # type: ignre

        if stream_settings.continuous_streaming_enabled is True:
            write_stream = write_stream.trigger(continuous="1 second")
        else:
            write_stream = write_stream.trigger(availableNow=True)

        write_stream.toTable(
            f"{DatabaseNames.bronze_database}.{TableNames.bronze_submitted_transactions_table}"
        )

    def write_stream(
        self,
        dataframe: DataFrame,
    ):
        checkpoint_location = shared_helpers.get_checkpoint_path(
            self.data_lake_settings, StorageContainerNames.bronze, "processed_transactions"
        )
        event_hub_instance = SubmittedTransactionsStreamSettings().event_hub_instance  # type: ignore

        dataframe.writeStream.format("kafka").options(**self.kafka_options).option("topic", event_hub_instance).option(
            "checkpointLocation", checkpoint_location
        ).trigger(availableNow=True).start()
